/// Example: Zero-Copy CDC Implementation
///
/// This demonstrates how to integrate the zero-copy types into your binlog processing.
/// Key changes from the original main.rs:
/// 1. Use CdcEventRef during parsing (zero allocations)
/// 2. Convert to owned CdcEvent only when sending through channel
/// 3. Significant memory savings during high-throughput scenarios
use binlog_tap::config::{CdcConfig, CdcEvent, EventType};
use binlog_tap::errors::Result as BinlogTapResult;
use binlog_tap::zero_copy::{CdcColumnRef, CdcEventRef, CdcValueRef};
use clap::Parser;
use futures::StreamExt;
use log::{error, info, warn};
use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::prelude::*;
use mysql_async::{BinlogStreamRequest, Pool};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Number of CDC events to batch before flushing
    #[arg(long, default_value_t = 25000)]
    pub batch_size: usize,

    /// Maximum events to keep in memory queue across all tables
    #[arg(long, default_value_t = 50000)]
    pub max_buffer_size: usize,

    /// Maximum time in seconds to wait before flushing incomplete batches
    #[arg(long, default_value_t = 30)]
    pub flush_interval_secs: u64,

    /// ClickHouse HTTP URL endpoint
    #[arg(long, default_value_t = String::from("http://localhost:8123"))]
    pub clickhouse_url: String,

    /// ClickHouse Username (also read from CLICKHOUSE_USER env var)
    #[arg(long, env = "CLICKHOUSE_USER", default_value_t = String::from("default"))]
    pub clickhouse_user: String,

    /// ClickHouse Password (also read from CLICKHOUSE_PASS env var)
    #[arg(long, env = "CLICKHOUSE_PASS", default_value_t = String::from(""))]
    pub clickhouse_pass: String,
}

pub struct TableMetadata {
    pub map_event: TableMapEvent<'static>,
    pub columns: Vec<Arc<str>>,
    pub table_name: Arc<str>,
    pub db_name: Arc<str>,
}

#[tokio::main]
async fn main() -> BinlogTapResult<()> {
    env_logger::init();
    let args = Args::parse();

    let config_path =
        std::env::var("BINLOG_TAP_CONFIG").unwrap_or_else(|_| "table_config.toml".to_string());

    let table_config = CdcConfig::load_from_file(&config_path)?;

    // Build "db.table" -> primary_key map from config for use in auto-CREATE DDL
    let mut pk_map: HashMap<String, String> = HashMap::new();
    for db in &table_config.databases {
        if let Some(tables) = &db.tables {
            for tbl in tables {
                let key = format!("{}.{}", db.name, tbl.name);
                pk_map.insert(key, tbl.primary_key.clone());
            }
        }
    }
    let primary_keys = Arc::new(pk_map);

    let mut tasks: Vec<tokio::task::JoinHandle<BinlogTapResult<()>>> = Vec::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<CdcEvent>(10000);

    let writer_handle = tokio::spawn({
        let args = args.clone();
        let primary_keys = Arc::clone(&primary_keys);
        async move { writer(rx, args, primary_keys).await }
    });

    for db in table_config.databases {
        let worker_handle = tokio::spawn({
            let tx = tx.clone();
            let primary_keys = Arc::clone(&primary_keys);
            async move {
                let pool = Pool::new(db.url.as_str());
                let mut name_to_columns: HashMap<String, Vec<Arc<str>>> = HashMap::new();
                let mut table_metadata: HashMap<u64, TableMetadata> = HashMap::new();

                snapshot_columns(&pool, &db.name, &mut name_to_columns).await?;

                info!(
                    "[binlog-tap] {} tables dimuat dari {}",
                    name_to_columns.len(),
                    db.name
                );

                info!("[binlog-tap] Binlog stream dimulai untuk {}...", db.name);
                process_binlog_stream_zero_copy(
                    &pool,
                    db.server_id,
                    &name_to_columns,
                    &mut table_metadata,
                    &primary_keys,
                    tx,
                )
                .await?;

                Ok(())
            }
        });
        tasks.push(worker_handle);
    }

    drop(tx);

    for task in tasks {
        match task.await {
            Ok(Err(e)) => error!("Worker thread failed: {}", e),
            Err(e) => error!("Worker task panicked/failed to join: {}", e),
            _ => {}
        }
    }

    if let Err(e) = writer_handle.await {
        return Err(e.into());
    }

    Ok(())
}

async fn snapshot_columns(
    pool: &Pool,
    db_name: &str,
    name_to_columns: &mut HashMap<String, Vec<Arc<str>>>,
) -> BinlogTapResult<()> {
    let mut conn = pool.get_conn().await?;

    let query = r#"
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME, ORDINAL_POSITION;
    "#;

    let rows: Vec<(String, String, String)> = conn.exec(query, (db_name,)).await?;

    for (db_name, table_name, column_name) in rows {
        let key = format!("{}.{}", db_name, table_name);
        name_to_columns
            .entry(key)
            .or_default()
            .push(Arc::from(column_name.as_str()));
    }

    Ok(())
}

/// ZERO-COPY BINLOG PROCESSING
/// Key difference: Uses CdcEventRef<'a> during parsing, only allocates when sending to channel
async fn process_binlog_stream_zero_copy(
    pool: &Pool,
    server_id: u32,
    name_to_columns: &HashMap<String, Vec<Arc<str>>>,
    table_metadata: &mut HashMap<u64, TableMetadata>,
    primary_keys: &HashMap<String, String>,
    tx: tokio::sync::mpsc::Sender<CdcEvent>,
) -> BinlogTapResult<()> {
    let conn = pool.get_conn().await?;
    let request = BinlogStreamRequest::new(server_id);
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    let mut event_count = 0;
    // Name-based column cache: resolved ONCE per "db.table", reused across table_id changes
    let mut resolved_columns: HashMap<String, Vec<Arc<str>>> = HashMap::new();

    while let Some(event) = binlog_stream.next().await {
        match event {
            Ok(event) => {
                event_count += 1;

                if let Some(event_data) = event.read_data()? {
                    if let EventData::TableMapEvent(table_map) = event_data {
                        // Skip internal MySQL databases to prevent ClickHouse sink errors
                        let db_name_raw = table_map.database_name();
                        let db_name_str = db_name_raw.trim_end_matches('\0');
                        let table_name_raw = table_map.table_name();
                        let table_name_str = table_name_raw.trim_end_matches('\0');

                        if matches!(
                            db_name_str,
                            "mysql" | "information_schema" | "performance_schema" | "sys"
                        ) {
                            continue;
                        }

                        let table_id = table_map.table_id();
                        let full_table_name = format!("{}.{}", db_name_str, table_name_str);

                        // Fast path: if we already resolved columns for this table NAME, reuse them
                        let columns = if let Some(cached) = resolved_columns.get(&full_table_name) {
                            cached.clone()
                        } else {
                            // === COLUMN NAME RESOLUTION (runs ONCE per table name) ===

                            // Tier 1: Startup cache
                            let mut cols = name_to_columns
                                .get(&full_table_name)
                                .cloned()
                                .unwrap_or_default();

                            // Tier 2: Extract from TableMapEvent optional metadata (MySQL 8.0+ with binlog_row_metadata=FULL)
                            if cols.is_empty() {
                                use mysql_async::binlog::events::OptionalMetaExtractor;
                                match OptionalMetaExtractor::new(table_map.iter_optional_meta()) {
                                    Ok(extractor) => {
                                        let extracted: Vec<Arc<str>> = extractor
                                            .iter_column_name()
                                            .filter_map(|r| r.ok())
                                            .map(|cn| Arc::from(cn.name().as_ref()))
                                            .collect();
                                        if !extracted.is_empty() {
                                            info!(
                                                "[binlog-tap] Extracted {} column names from binlog metadata for {}",
                                                extracted.len(),
                                                full_table_name
                                            );
                                            cols = extracted;
                                        } else {
                                            warn!(
                                                "[binlog-tap] OptionalMetaExtractor succeeded but extracted 0 columns for {}",
                                                full_table_name
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[binlog-tap] OptionalMetaExtractor failed for {}: {:?}",
                                            full_table_name, e
                                        );
                                    }
                                }
                            }

                            // Tier 3: Query INFORMATION_SCHEMA (network fallback)
                            if cols.is_empty() {
                                match pool.get_conn().await {
                                    Ok(mut fallback_conn) => {
                                        let query = r#"
                                            SELECT COLUMN_NAME
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                                            ORDER BY ORDINAL_POSITION
                                        "#;
                                        match fallback_conn
                                            .exec::<String, _, _>(
                                                query,
                                                (db_name_str, table_name_str),
                                            )
                                            .await
                                        {
                                            Ok(rows) => {
                                                if !rows.is_empty() {
                                                    info!(
                                                        "[binlog-tap] Fetched {} columns from INFORMATION_SCHEMA for {}",
                                                        rows.len(),
                                                        full_table_name
                                                    );
                                                    cols = rows
                                                        .into_iter()
                                                        .map(|c| Arc::from(c.as_str()))
                                                        .collect();
                                                } else {
                                                    warn!(
                                                        "[binlog-tap] INFORMATION_SCHEMA returned 0 rows for {}",
                                                        full_table_name
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "[binlog-tap] Query to INFORMATION_SCHEMA failed for {}: {:?}",
                                                    full_table_name, e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[binlog-tap] Failed to get fallback connection for {}: {:?}",
                                            full_table_name, e
                                        );
                                    }
                                }
                            }

                            // Tier 4: Generate numbered column names as last resort
                            if cols.is_empty() {
                                let col_count = table_map.columns_count() as usize;
                                warn!(
                                    "[binlog-tap] No column names available for {}. Using {} numbered columns.",
                                    full_table_name, col_count
                                );
                                cols = (0..col_count)
                                    .map(|i| Arc::from(format!("col_{}", i).as_str()))
                                    .collect();
                            }

                            // Cache it permanently by name
                            resolved_columns.insert(full_table_name.clone(), cols.clone());
                            cols
                        };

                        let db_name: Arc<str> = Arc::from(db_name_str);
                        let table_name: Arc<str> = Arc::from(table_name_str);

                        table_metadata.insert(
                            table_id,
                            TableMetadata {
                                map_event: table_map.into_owned(),
                                columns,
                                table_name,
                                db_name,
                            },
                        );
                    } else if let EventData::RowsEvent(rows_event) = event_data {
                        let table_id = rows_event.table_id();
                        if let Some(metadata) = table_metadata.get(&table_id) {
                            let event_type = EventType::from_mysql_event_type(&rows_event);

                            // ZERO-COPY MAGIC HAPPENS HERE!
                            for row_result in rows_event.rows(&metadata.map_event) {
                                match row_result {
                                    Ok((before, after_opt)) => {
                                        // Pick the data row to emit:
                                        //   INSERT / UPDATE → emit the new "after" row
                                        //   DELETE          → emit the "before" row as a soft-delete tombstone
                                        let row_data = match (event_type, after_opt, before) {
                                            // INSERT or UPDATE: use the after row
                                            (_, Some(row_after), _) => Some(row_after.unwrap()),
                                            // DELETE: use the before row
                                            (EventType::Delete, None, Some(row_before)) => {
                                                Some(row_before.unwrap())
                                            }
                                            _ => None,
                                        };

                                        if let Some(binlog_values) = row_data {
                                            // Build zero-copy CdcEventRef by borrowing from binlog_values
                                            let zero_copy_event = CdcEventRef {
                                                db_name: metadata.db_name.as_ref(),
                                                table_name: metadata.table_name.as_ref(),
                                                event_type,
                                                columns: binlog_values
                                                    .iter()
                                                    .enumerate()
                                                    .map(|(i, binlog_val)| {
                                                        let column_name = metadata
                                                            .columns
                                                            .get(i)
                                                            .map(|arc| arc.as_ref())
                                                            .unwrap_or("unknown");

                                                        CdcColumnRef {
                                                            name: column_name,
                                                            value:
                                                                CdcValueRef::from_binlog_value_ref(
                                                                    binlog_val,
                                                                ),
                                                            is_primary_key: if primary_keys.get(
                                                                format!(
                                                                    "{}.{}",
                                                                    metadata.db_name.as_ref(),
                                                                    metadata.table_name.as_ref()
                                                                ),
                                                            ) == column_name
                                                            {
                                                                true
                                                            } else {
                                                                false
                                                            },
                                                        }
                                                    })
                                                    .collect(),
                                            };

                                            // Convert to owned ONLY when sending through channel
                                            let owned_event = zero_copy_event.to_owned_event(
                                                Arc::clone(&metadata.db_name),
                                                Arc::clone(&metadata.table_name),
                                            );

                                            tx.send(owned_event).await?;
                                        }
                                    }
                                    Err(e) => {
                                        return Err(e.into());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    println!(
        "✅ [binlog-tap] Stream ended. Total events: {}",
        event_count
    );
    Ok(())
}

pub async fn writer(
    mut rx: tokio::sync::mpsc::Receiver<CdcEvent>,
    args: Args,
    primary_keys: Arc<HashMap<String, String>>,
) -> BinlogTapResult<()> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut flush_tasks = tokio::task::JoinSet::new();

    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(20)
        .http1_only()
        .build()
        .expect("Failed to build HTTP Client");

    let shared_args = Arc::new(args.clone());
    let known_tables = Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new()));

    let mut buffers: std::collections::HashMap<String, Vec<CdcEvent>> =
        std::collections::HashMap::new();

    let mut total_events: usize = 0;

    let mut ticker =
        tokio::time::interval(tokio::time::Duration::from_secs(args.flush_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut metrics_ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
    metrics_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut total_processed: u64 = 0;
    let mut last_processed: u64 = 0;

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        total_processed += 1;
                        let key = format!("{}.{}", event.db_name, event.table_name);

                        if total_events >= args.max_buffer_size {
                            let largest_key = buffers
                                .iter()
                                .max_by_key(|(_, v)| v.len())
                                .map(|(k, _)| k.clone());
                            if let Some(flush_key) = largest_key
                                && let Some(ready_rows) = buffers.remove(&flush_key) {
                                    total_events -= ready_rows.len();
                                    warn!("[binlog-tap] ⚠️ Memory limit ({} rows) reached! Force-flushing {} events from '{}' early...", args.max_buffer_size, ready_rows.len(), flush_key);
                                    spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), http_client.clone(), Arc::clone(&shared_args), Arc::clone(&known_tables), Arc::clone(&primary_keys), flush_key, ready_rows);
                                }
                        }

                        let buf = buffers.entry(key.clone()).or_default();
                        buf.push(event);
                        total_events += 1;

                        if buf.len() >= args.batch_size {
                            let rows = std::mem::take(buf);
                            total_events -= rows.len();
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), http_client.clone(), Arc::clone(&shared_args), Arc::clone(&known_tables), Arc::clone(&primary_keys), key, rows);
                        }
                    }
                    None => {
                        println!();
                        info!("Channel closed, flushing remaining buffers...");
                        for (key, rows) in buffers.drain().filter(|(_, rows)| !rows.is_empty()) {
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), http_client.clone(), Arc::clone(&shared_args), Arc::clone(&known_tables), Arc::clone(&primary_keys), key, rows);
                        }
                        break;
                    }
                }
            }

            _ = ticker.tick() => {
                for (key, buf) in buffers.iter_mut().filter(|(_, v)| !v.is_empty()) {
                    let rows = std::mem::take(buf);
                    total_events -= rows.len();
                    spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), http_client.clone(), Arc::clone(&shared_args), Arc::clone(&known_tables), Arc::clone(&primary_keys), key.clone(), rows);
                }
            }

            _ = metrics_ticker.tick() => {
                let rps = total_processed - last_processed;
                last_processed = total_processed;

                if rps > 0 || total_events > 0 {
                    info!("[binlog-tap] Processed: {} rows | Speed: {} rows/sec | Pending: {}",
                        total_processed, rps, total_events);
                }
            }

            Some(result) = flush_tasks.join_next() => {
                if let Err(e) = result {
                    return Err(e.into());
                }
            }
        }
    }

    while let Some(result) = flush_tasks.join_next().await {
        if let Err(e) = result {
            return Err(e.into());
        }
    }

    Ok(())
}

fn spawn_flush_task(
    tasks: &mut tokio::task::JoinSet<()>,
    semaphore: Arc<tokio::sync::Semaphore>,
    client: reqwest::Client,
    args: Arc<Args>,
    known_tables: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    primary_keys: Arc<HashMap<String, String>>,
    key: String,
    rows: Vec<CdcEvent>,
) {
    tasks.spawn(async move {
        let _permit = semaphore
            .acquire()
            .await
            .expect("Semaphore closed unexpectedly");

        if let Err(e) = flush_table(client, args, known_tables, primary_keys, key, rows).await {
            error!("❌ [flush] ClickHouse Insert Failed: {}", e);
        }
    });
}

async fn flush_table(
    client: reqwest::Client,
    args: Arc<Args>,
    known_tables: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    primary_keys: Arc<HashMap<String, String>>,
    table: String,
    rows: Vec<CdcEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let row_count = rows.len();

    // Parse "db.table" into parts for URL-safe queries
    let table_parts: Vec<&str> = table.splitn(2, '.').collect();
    if table_parts.len() != 2 {
        return Err(format!(
            "Invalid table identifier '{}': expected 'db.table' format",
            table
        )
        .into());
    }

    // URL-encode the table name for safe embedding in the HTTP query string
    let table_encoded = format!(
        "{}.{}",
        urlencoding::encode(table_parts[0]),
        urlencoding::encode(table_parts[1]),
    );

    // 0. Super-fast in-memory cache to verify if Table Exists
    {
        let cache = known_tables.read().await;
        if !cache.contains(&table) {
            drop(cache); // Release read lock to allow network query

            // Use system.tables query — more reliable than non-standard EXISTS TABLE syntax
            let encoded_db = urlencoding::encode(table_parts[0]);
            let encoded_tbl = urlencoding::encode(table_parts[1]);
            let raw_query = format!(
                "SELECT count() FROM system.tables WHERE database='{}' AND name='{}'",
                encoded_db, encoded_tbl
            );
            let encoded_query = urlencoding::encode(&raw_query);
            let check_url = format!("{}/?query={}", args.clickhouse_url, encoded_query);
            let mut check_req = client.get(&check_url);

            if !args.clickhouse_user.is_empty() {
                check_req =
                    check_req.basic_auth(&args.clickhouse_user, Some(&args.clickhouse_pass));
            }

            let res = check_req.send().await?;
            let is_exists_text = res.text().await?.trim().to_string();

            if is_exists_text == "0" {
                warn!(
                    "[flush] ⚠️ Table '{}' does not exist in ClickHouse! Attempting to create it dynamically...",
                    table
                );

                // Build dynamic schema from the first row
                let mut create_query = format!("CREATE TABLE IF NOT EXISTS {} (", table);
                let first_row = &rows[0];

                for (i, col) in first_row.columns.iter().enumerate() {
                    if i > 0 {
                        create_query.push_str(", ");
                    }

                    let ch_type = match &col.value {
                        binlog_tap::config::CdcValue::Null
                        | binlog_tap::config::CdcValue::String(_)
                        | binlog_tap::config::CdcValue::Bytes(_)
                        | binlog_tap::config::CdcValue::Json(_) => "Nullable(String)",
                        binlog_tap::config::CdcValue::Bool(_) => "Nullable(Bool)",
                        binlog_tap::config::CdcValue::Int(_) => "Nullable(Int64)",
                        binlog_tap::config::CdcValue::Uint(_) => "Nullable(UInt64)",
                        binlog_tap::config::CdcValue::Float(_) => "Nullable(Float64)",
                        binlog_tap::config::CdcValue::Decimal(_) => "Nullable(Decimal(38, 9))",
                        binlog_tap::config::CdcValue::Uuid(_) => "Nullable(UUID)",
                        binlog_tap::config::CdcValue::Date(_) => "Nullable(Date)",
                        binlog_tap::config::CdcValue::Time(_) => "Nullable(String)",
                        binlog_tap::config::CdcValue::DateTime(_) => "Nullable(DateTime64(3))",
                    };

                    create_query.push_str(&format!("`{}` {}", col.name, ch_type));
                }

                // Add CDC meta-columns for tracking event type and ingestion time
                create_query.push_str(", `_event_type` LowCardinality(String)");
                create_query.push_str(", `_cdc_timestamp` DateTime64(3) DEFAULT now64()");
                create_query.push_str(", `_is_deleted` UInt8 DEFAULT 0");

                // ReplacingMergeTree: deduplicates rows by primary key on merge.
                // _is_deleted tombstone pattern: rows with _is_deleted=1 are treated as DELETEs
                // by querying with FINAL or by the ReplacingMergeTree cleanup logic.

                println!("primary keys for table {}: {:?}", table, primary_keys);

                let order_by = primary_keys
                    .get(&table)
                    .map(|pk| format!("ORDER BY (`{}`)", pk))
                    .unwrap_or_else(|| "ORDER BY tuple()".to_string());
                create_query.push_str(&format!(
                    ") ENGINE = ReplacingMergeTree(_cdc_timestamp) {}",
                    order_by
                ));

                // Send CREATE TABLE as the POST body to ensure Content-Length is provided
                let mut create_req = client.post(&args.clickhouse_url).body(create_query.clone());
                if !args.clickhouse_user.is_empty() {
                    create_req =
                        create_req.basic_auth(&args.clickhouse_user, Some(&args.clickhouse_pass));
                }

                let create_res = create_req.send().await?;
                if !create_res.status().is_success() {
                    let err = create_res.text().await.unwrap_or_default();
                    error!(
                        "❌ [flush] Failed to auto-create table '{}' with error : {} and query: {}",
                        table,
                        err,
                        create_query.clone()
                    );
                    return Err(err.into());
                }

                info!(
                    "[flush] ✨ Auto-created table '{}' successfully in ClickHouse!",
                    table
                );
            }

            // Valid! Cache it so we never make this EXISTS HTTP request again for this table
            let mut write_cache = known_tables.write().await;
            write_cache.insert(table.clone());
        }
    }

    // 1. Build a Stream of Bytes mapped from the Vec<CdcEvent> directly
    // This PREVENTS out of memory string allocation! The HTTP client will stream chunks.
    use binlog_tap::config::CdcValue;

    let stream_rows = futures::stream::iter(rows).map(move |row| {
        let mut row_str = String::with_capacity(256);
        row_str.push('{');
        for (i, col) in row.columns.iter().enumerate() {
            if i > 0 {
                row_str.push(',');
            }

            // Serialize Key safely
            row_str.push_str(&serde_json::to_string(col.name.as_ref()).unwrap());
            row_str.push(':');

            // Serialize Value safely based on Enum
            match &col.value {
                CdcValue::Null => row_str.push_str("null"),
                CdcValue::Bool(v) => row_str.push_str(if *v { "true" } else { "false" }),
                CdcValue::Int(v) => {
                    use std::fmt::Write;
                    write!(row_str, "{}", v).unwrap();
                }
                CdcValue::Uint(v) => {
                    use std::fmt::Write;
                    write!(row_str, "{}", v).unwrap();
                }
                CdcValue::Float(v) => {
                    use std::fmt::Write;
                    write!(row_str, "{}", v).unwrap();
                }
                CdcValue::Decimal(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
                CdcValue::String(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
                CdcValue::Bytes(v) => {
                    let lossy = String::from_utf8_lossy(v);
                    row_str.push_str(&serde_json::to_string(lossy.as_ref()).unwrap());
                }
                CdcValue::Json(v) => row_str.push_str(v),
                CdcValue::Uuid(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
                CdcValue::Date(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
                CdcValue::Time(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
                CdcValue::DateTime(v) => row_str.push_str(&serde_json::to_string(v).unwrap()),
            }
        }

        // Append CDC meta-columns
        // _event_type: INSERT, UPDATE, or DELETE (DELETE rows are soft-delete tombstones)
        use std::fmt::Write;
        write!(row_str, ",\"_event_type\":\"{}\"", row.event_type).unwrap();
        // _cdc_timestamp: ISO-8601 UTC timestamp when this event was captured by binlog-tap
        let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        write!(row_str, ",\"_cdc_timestamp\":\"{}\"", ts).unwrap();
        // _is_deleted: 1 for DELETE events (soft-delete tombstone), 0 otherwise
        let is_deleted = if row.event_type == EventType::Delete {
            1u8
        } else {
            0u8
        };
        write!(row_str, ",\"_is_deleted\":{}", is_deleted).unwrap();

        row_str.push('}');
        row_str.push('\n'); // Required delimiter for JSONEachRow!

        Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(row_str))
    });

    // 2. Build the ClickHouse Target URL using the URL-encoded table identifier
    let target_url = format!(
        "{}/?query=INSERT%20INTO%20{}%20FORMAT%20JSONEachRow",
        args.clickhouse_url, table_encoded
    );

    // 3. Send blazing fast HTTP POST
    let mut request = client.post(&target_url);

    // Add Basic Auth if provided
    if !args.clickhouse_user.is_empty() {
        request = request.basic_auth(&args.clickhouse_user, Some::<&str>(&args.clickhouse_pass));
    }

    let response = request
        .body(reqwest::Body::wrap_stream(stream_rows))
        .send()
        .await?;

    // 4. Validate insertion accuracy
    if !response.status().is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err(format!("ClickHouse HTTP Error: {}", err_text).into());
    }

    info!(
        "[flush] ✅ Successfully flushed {} rows to {}",
        row_count, table
    );
    Ok(())
}
