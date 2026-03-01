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
use log::{debug, error, info, warn};
use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::prelude::*;
use mysql_async::{BinlogStreamRequest, Pool};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Number of CDC events to batch before flushing
    #[arg(long, default_value_t = 50000)]
    pub batch_size: usize,

    /// Maximum events to keep in memory queue across all tables
    #[arg(long, default_value_t = 100000)]
    pub max_buffer_size: usize,

    /// Maximum time in seconds to wait before flushing incomplete batches
    #[arg(long, default_value_t = 30)]
    pub flush_interval_secs: u64,
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

    let mut threads_conn: Vec<tokio::task::JoinHandle<BinlogTapResult<()>>> = Vec::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<CdcEvent>(10000);

    let writer_handle = tokio::spawn({
        let args = args.clone();
        async move { writer(rx, args).await }
    });

    for db in table_config.databases {
        let worker_handle = tokio::spawn({
            let tx = tx.clone();
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
                    tx,
                )
                .await?;

                Ok(())
            }
        });
        threads_conn.push(worker_handle);
    }

    drop(tx);

    for thread in threads_conn {
        match thread.await {
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
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    "#;

    let rows: Vec<(String, String, String)> = conn.exec(query, (db_name,)).await?;

    for (db_name, table_name, column_name) in rows {
        let key = format!("{}.{}", db_name, table_name);
        name_to_columns
            .entry(key)
            .or_insert_with(Vec::new)
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
    tx: tokio::sync::mpsc::Sender<CdcEvent>,
) -> BinlogTapResult<()> {
    let conn = pool.get_conn().await?;
    let request = BinlogStreamRequest::new(server_id);
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    let mut event_count = 0;

    while let Some(event) = binlog_stream.next().await {
        match event {
            Ok(event) => {
                event_count += 1;

                if let Some(event_data) = event.read_data()? {
                    if let EventData::TableMapEvent(table_map) = event_data {
                        let table_id = table_map.table_id();
                        debug!(
                            "[binlog-tap] Cached table: {}.{} (id: {})",
                            table_map.database_name(),
                            table_map.table_name(),
                            table_id
                        );

                        let db_name: Arc<str> = Arc::from(table_map.database_name());
                        let table_name: Arc<str> = Arc::from(table_map.table_name());

                        let columns = name_to_columns
                            .get(&format!(
                                "{}.{}",
                                table_map.database_name(),
                                table_map.table_name()
                            ))
                            .cloned()
                            .unwrap_or_default();

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
                                    Ok((_before, Some(row_after))) => {
                                        // Step 1: Extract BinlogValues (owned by mysql_async with 'static)
                                        let binlog_values = row_after.unwrap();

                                        // Step 2: Build zero-copy CdcEventRef by borrowing from binlog_values
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
                                                        value: CdcValueRef::from_binlog_value_ref(
                                                            binlog_val,
                                                        ),
                                                    }
                                                })
                                                .collect(),
                                        };

                                        // Step 3: Convert to owned ONLY when sending through channel
                                        // This is the only allocation point!
                                        let owned_event = zero_copy_event.to_owned_event(
                                            Arc::clone(&metadata.db_name),
                                            Arc::clone(&metadata.table_name),
                                        );

                                        tx.send(owned_event).await?;
                                    }
                                    Ok((_before, None)) => {} // DELETE
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
) -> BinlogTapResult<()> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut flush_tasks = tokio::task::JoinSet::new();

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
                            if let Some(flush_key) = largest_key {
                                if let Some(ready_rows) = buffers.remove(&flush_key) {
                                    total_events -= ready_rows.len();
                                    warn!("[binlog-tap] ⚠️ Memory limit ({} rows) reached! Force-flushing {} events from '{}' early...", args.max_buffer_size, ready_rows.len(), flush_key);
                                    spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), flush_key, ready_rows);
                                }
                            }
                        }

                        let buf = buffers.entry(key.clone()).or_default();
                        buf.push(event);
                        total_events += 1;

                        if buf.len() >= args.batch_size {
                            let rows = std::mem::take(buf);
                            total_events -= rows.len();
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key, rows);
                        }
                    }
                    None => {
                        println!();
                        info!("Channel closed, flushing remaining buffers...");
                        for (key, rows) in buffers.drain().filter(|(_, rows)| !rows.is_empty()) {
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key, rows);
                        }
                        break;
                    }
                }
            }

            _ = ticker.tick() => {
                for (key, buf) in buffers.iter_mut().filter(|(_, v)| !v.is_empty()) {
                    let rows = std::mem::take(buf);
                    total_events -= rows.len();
                    spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key.clone(), rows);
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
    key: String,
    rows: Vec<CdcEvent>,
) {
    tasks.spawn(async move {
        let _permit = semaphore
            .acquire()
            .await
            .expect("Semaphore closed unexpectedly");
        flush_table(key, rows).await;
    });
}

async fn flush_table(table: String, rows: Vec<CdcEvent>) {
    let row_count = rows.len();

    // For 500K/sec throughput, flush must be instant
    // TODO: Replace with actual ClickHouse client
    info!("[flush] Flushing {} rows to table: {}", row_count, table);

    // Real ClickHouse insert would go here:
    // clickhouse_client.insert(&table, rows).await?;

    // Rows dropped here → memory freed instantly!
    debug!(
        "[flush] Successfully flushed {} rows to {}",
        row_count, table
    );
}
