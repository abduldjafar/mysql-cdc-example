use binlog_tap::config::{CdcColumn, CdcConfig, CdcEvent, CdcValue, EventType};
use binlog_tap::errors::Result as BinlogTapResult;
use futures::StreamExt;
use log::{debug, error, info, warn};
use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::prelude::*;
use mysql_async::{BinlogStreamRequest, Pool};
use std::collections::HashMap;
use std::sync::Arc;

pub struct TableMetadata {
    pub map_event: TableMapEvent<'static>,
    pub columns: Vec<Arc<str>>,
    pub table_name: Arc<str>,
    pub db_name: Arc<str>,
}

#[tokio::main]
async fn main() -> BinlogTapResult<()> {
    env_logger::init();
    // 1. Resolve configuration path dynamically
    let config_path =
        std::env::var("BINLOG_TAP_CONFIG").unwrap_or_else(|_| "table_config.toml".to_string());

    let table_config = CdcConfig::load_from_file(&config_path)?;

    let mut threads_conn: Vec<tokio::task::JoinHandle<BinlogTapResult<()>>> = Vec::new();
    // Bounded channel to apply backpressure back to workers during spikes
    let (tx, rx) = tokio::sync::mpsc::channel::<CdcEvent>(10000);

    // 2. Spawn writer task on the tokio runtime so it runs concurrently
    let writer_handle = tokio::spawn(async move { writer(rx).await });

    for db in table_config.databases {
        let worker_handle = tokio::spawn({
            let tx = tx.clone(); // setiap worker punya sender-nya sendiri
            async move {
                let pool = Pool::new(db.url.as_str());
                let mut name_to_columns: HashMap<String, Vec<Arc<str>>> = HashMap::new();
                let mut table_metadata: HashMap<u64, TableMetadata> = HashMap::new();

                // Step 1: Snapshot schema
                snapshot_columns(&pool, &db.name, &mut name_to_columns).await?;
                info!(
                    "📊 [binlog-tap] {} tables dimuat dari {}",
                    name_to_columns.len(),
                    db.name
                );

                // Step 2: Binlog stream (long-running)
                info!("🔴 [binlog-tap] Binlog stream dimulai untuk {}...", db.name);
                process_binlog_stream(
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

    // Drop the main thread's transmitter so the writer knows when workers are done
    drop(tx);

    // 3. Properly await all workers and propagate errors
    for thread in threads_conn {
        match thread.await {
            Ok(Err(e)) => error!("❌ Worker thread failed: {}", e),
            Err(e) => error!("❌ Worker task panicked/failed to join: {}", e),
            _ => {}
        }
    }

    // Wait for the writer task to finish draining channels
    if let Err(e) = writer_handle.await {
        error!("❌ Writer task failed to join: {}", e);
    }

    Ok(())
}

/// Snapshot table schemas from INFORMATION_SCHEMA
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
        // Key by "db.table" to avoid collisions across databases
        let key = format!("{}.{}", db_name, table_name);
        name_to_columns
            .entry(key)
            .or_insert_with(Vec::new)
            .push(Arc::from(column_name.as_str()));
    }

    Ok(())
}

/// Process binlog stream events
async fn process_binlog_stream(
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

    // Process binlog events
    while let Some(event) = binlog_stream.next().await {
        match event {
            Ok(event) => {
                event_count += 1;

                if let Some(event_data) = event.read_data()? {
                    // Handle TableMapEvent to cache table metadata
                    if let EventData::TableMapEvent(table_map) = event_data {
                        let table_id = table_map.table_id();
                        debug!(
                            "📋 [binlog-tap] Cached table: {}.{} (id: {})",
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
                    }
                    // Handle RowsEvent for actual data changes
                    else if let EventData::RowsEvent(rows_event) = event_data {
                        let table_id = rows_event.table_id();
                        if let Some(metadata) = table_metadata.get(&table_id) {
                            let event_type = EventType::from_mysql_event_type(&rows_event);

                            // Process all rows functionally using iterators
                            let mut stream =
                                futures::stream::iter(rows_event.rows(&metadata.map_event).map(
                                    |row_result| -> BinlogTapResult<Option<CdcEvent>> {
                                        match row_result {
                                            Ok((_before, Some(row_after))) => {
                                                // BinlogRow::unwrap() → Vec<BinlogValue<'static>>
                                                // This is an infallible method on the BinlogRow struct,
                                                // NOT Option::unwrap(). It cannot panic.
                                                let binlog_values: Vec<_> = row_after
                                                    .unwrap()
                                                    .into_iter()
                                                    .enumerate()
                                                    .collect();
                                                Ok(Some(CdcEvent {
                                                    event_type,
                                                    table_name: Arc::clone(&metadata.table_name),
                                                    db_name: Arc::clone(&metadata.db_name),
                                                    columns: binlog_values
                                                        .into_iter()
                                                        .map(|(i, binlog_val)| CdcColumn {
                                                            name: metadata
                                                                .columns
                                                                .get(i)
                                                                .map(|s| Arc::clone(s))
                                                                .unwrap_or_else(|| {
                                                                    // fallback: col_N when schema unknown
                                                                    Arc::from(format!("col_{}", i))
                                                                }),
                                                            value: CdcValue::from_binlog_value(
                                                                binlog_val,
                                                            ),
                                                        })
                                                        .collect(),
                                                }))
                                            }
                                            Ok((_before, None)) => Ok(None), // DELETE: hanya ada before
                                            Err(e) => {
                                                error!("❌ [binlog-tap] Error parsing row: {}", e);
                                                Err(e.into())
                                            }
                                        }
                                    },
                                ));

                            while let Some(result) = stream.next().await {
                                match result {
                                    Ok(Some(cdc_event)) => {
                                        tx.send(cdc_event).await?;
                                    }
                                    Ok(None) => {} // Abaikan event DELETE kosong
                                    Err(e) => return Err(e), // Batalkan seluruh proses stream tabel ini saat data corrupt
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ [binlog-tap] Error reading binlog event: {}", e);
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

const BATCH_SIZE: usize = 5_000;
const FLUSH_INTERVAL_SECS: u64 = 10;
const MAX_BUFFER_SIZE: usize = 20_000; // Safe limit for 512MB RAM

pub async fn writer(mut rx: tokio::sync::mpsc::Receiver<CdcEvent>) -> BinlogTapResult<()> {
    // Limit concurrent flush tasks to prevent unbounded spawning during spikes
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut flush_tasks = tokio::task::JoinSet::new();

    // Buffer per tabel: key = "db.table", value = Vec<CdcEvent>
    let mut buffers: std::collections::HashMap<String, Vec<CdcEvent>> =
        std::collections::HashMap::new();

    let mut total_events: usize = 0;

    // Timer untuk flush periodik (setiap 30 detik)
    let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(FLUSH_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Timer untuk metrics realtime (setiap 1 detik)
    let mut metrics_ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
    metrics_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut total_processed: u64 = 0;
    let mut last_processed: u64 = 0;

    loop {
        tokio::select! {
            // Event baru masuk dari worker
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        total_processed += 1;
                        let key = format!("{}.{}", event.db_name, event.table_name);

                        // Circuit Breaker: prevent OOM by dropping the LARGEST buffer,
                        // not the incoming table's (which might have very few events)
                        if total_events >= MAX_BUFFER_SIZE {
                            let largest_key = buffers
                                .iter()
                                .max_by_key(|(_, v)| v.len())
                                .map(|(k, _)| k.clone());
                            if let Some(drop_key) = largest_key {
                                if let Some(dropped) = buffers.remove(&drop_key) {
                                    total_events -= dropped.len();
                                    warn!("⚠️ [binlog-tap] Memory limit reached ({} events). Dropped {} events from '{}' (largest buffer).", MAX_BUFFER_SIZE, dropped.len(), drop_key);
                                }
                            }
                        }

                        let buf = buffers.entry(key.clone()).or_default();
                        buf.push(event);
                        total_events += 1;

                        if buf.len() >= BATCH_SIZE {
                            // Flush kalau sudah mencapai BATCH_SIZE
                            let rows = std::mem::take(buf);
                            total_events -= rows.len();
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key, rows);
                        }
                    }
                    None => {
                        // Channel closed — flush sisa buffer semua tabel lalu exit
                        println!(); // Pindah baris dari log realtime
                        info!("📭 Channel closed, flushing remaining buffers...");
                        // total_events will be zero after loop breaks; no need to update
                        for (key, rows) in buffers.drain().filter(|(_, rows)| !rows.is_empty()) {
                            spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key, rows);
                        }
                        break;
                    }
                }
            }

            // Timer 30 detik — flush semua tabel yang punya pending events
            _ = ticker.tick() => {
                for (key, buf) in buffers.iter_mut().filter(|(_, v)| !v.is_empty()) {
                    let rows = std::mem::take(buf);
                    total_events -= rows.len();
                    spawn_flush_task(&mut flush_tasks, Arc::clone(&semaphore), key.clone(), rows);
                }
            }

            // Metrics Reporting 1 Detik
            _ = metrics_ticker.tick() => {
                let rps = total_processed - last_processed;
                last_processed = total_processed;

                // Print normally instead of \r, because \r often gets swallowed by Docker logs
                if rps > 0 || total_events > 0 {
                    info!("📊 [binlog-tap] Processed: {} rows | Speed: {} rows/sec | Pending Buffer: {} rows",
                        total_processed, rps, total_events);
                }
            }

            // Clean up completed tasks to prevent memory leak of JoinSet
            Some(result) = flush_tasks.join_next() => {
                if let Err(e) = result {
                    error!("❌ [writer] Flush task panicked: {}", e);
                }
            }
        }
    }

    // Wait for all remaining flush tasks to complete before exiting
    while let Some(result) = flush_tasks.join_next().await {
        if let Err(e) = result {
            error!("❌ [writer] Flush task panicked during shutdown: {}", e);
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
        // Acquire permit, waiting if concurrency limit is reached
        let _permit = semaphore
            .acquire()
            .await
            .expect("Semaphore closed unexpectedly — this is a programming error");
        flush_table(key, rows).await;
    });
}

/// Flush satu tabel ke destination (saat ini: sink database ClickHouse)
async fn flush_table(_table: String, _rows: Vec<CdcEvent>) {
    // Pada sink / clickhouse implementasi:
    // Insert _rows ke _table
}
