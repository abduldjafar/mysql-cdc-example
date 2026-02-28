use binlog_tap::config::{CdcColumn, CdcConfig, CdcEvent, CdcValue, EventType};
use binlog_tap::errors::Result as BinlogTapResult;
use futures::StreamExt;
use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::prelude::*;
use mysql_async::{BinlogStreamRequest, Pool};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> BinlogTapResult<()> {
    let table_config = CdcConfig::load_from_file(
        "/Users/abdulharisdjafar/Documents/private/custom-tools/mysql-cdc-example/binlog-tap/table_config.toml",
    )?;
    let mut threads_conn: Vec<tokio::task::JoinHandle<BinlogTapResult<()>>> = Vec::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<CdcEvent>(1000);

    // Simple reader task: print setiap event yang masuk dari semua worker
    writer(rx).await?;

    for db in table_config.databases {
        let test_conn = tokio::spawn({
            let tx = tx.clone(); // setiap worker punya sender-nya sendiri
            async move {
                let pool = Pool::new(db.url.as_str());
                let mut name_to_columns: HashMap<String, Vec<String>> = HashMap::new();
                let mut table_map_events: HashMap<u64, TableMapEvent<'static>> = HashMap::new();

                // Step 1: Snapshot schema
                snapshot_columns(&pool, &db.name, &mut name_to_columns).await?;
                println!(
                    "📊 [binlog-tap] {} tables dimuat dari {}",
                    name_to_columns.len(),
                    db.name
                );

                // Step 2: Binlog stream (long-running)
                println!("🔴 [binlog-tap] Binlog stream dimulai untuk {}...", db.name);
                process_binlog_stream(
                    &pool,
                    db.server_id,
                    &name_to_columns,
                    &mut table_map_events,
                    tx,
                )
                .await?;

                Ok(())
            }
        });
        threads_conn.push(test_conn);
    }

    for thread in threads_conn {
        let _ = thread.await?;
    }
    Ok(())
}

/// Snapshot table schemas from INFORMATION_SCHEMA
async fn snapshot_columns(
    pool: &Pool,
    db_name: &str,
    name_to_columns: &mut HashMap<String, Vec<String>>,
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
            .push(column_name);
    }

    Ok(())
}

/// Process binlog stream events
async fn process_binlog_stream(
    pool: &Pool,
    server_id: u32,
    name_to_columns: &HashMap<String, Vec<String>>,
    table_map_events: &mut HashMap<u64, TableMapEvent<'static>>,
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

                if event_count % 1000 == 0 {
                    println!("📈 [binlog-tap] Processed {} events", event_count);
                }

                if let Some(event_data) = event.read_data()? {
                    // Handle TableMapEvent to cache table metadata
                    if let EventData::TableMapEvent(table_map) = event_data {
                        let table_id = table_map.table_id();
                        println!(
                            "📋 [binlog-tap] Cached table: {} (db: {}, id: {})",
                            table_map.table_name(),
                            table_map.database_name(),
                            table_id
                        );
                        table_map_events.insert(table_id, table_map.into_owned());
                    }
                    // Handle RowsEvent for actual data changes
                    else if let EventData::RowsEvent(rows_event) = event_data {
                        let table_id = rows_event.table_id();
                        if let Some(table_map) = table_map_events.get(&table_id) {
                            let event_type = EventType::from_mysql_event_type(&rows_event);

                            let columns: Vec<String> = name_to_columns
                                .get(&format!(
                                    "{}.{}",
                                    table_map.database_name(),
                                    table_map.table_name()
                                ))
                                .into_iter()
                                .flatten()
                                .cloned()
                                .collect();

                            let table_name: Arc<str> = table_map.table_name().into();
                            let db_name: Arc<str> = table_map.database_name().into();

                            for row_result in rows_event.rows(table_map) {
                                match row_result {
                                    Ok((_before, Some(row_after))) => {
                                        let cdc_event = CdcEvent {
                                            event_type,
                                            table_name: Arc::clone(&table_name),
                                            db_name: Arc::clone(&db_name),
                                            columns: row_after
                                                .unwrap()
                                                .into_iter()
                                                .enumerate()
                                                .map(|(i, binlog_val)| CdcColumn {
                                                    name: columns
                                                        .get(i)
                                                        .map(|s| Arc::from(s.as_str()))
                                                        .unwrap_or_else(|| {
                                                            Arc::from(format!("col_{}", i).as_str())
                                                        }),
                                                    value: CdcValue::from_binlog_value(binlog_val),
                                                })
                                                .collect(),
                                        };
                                        // Kirim ke writer — kalau channel closed, worker exit
                                        tx.send(cdc_event).await?;
                                    }
                                    Ok((_before, None)) => {} // DELETE: hanya ada before
                                    Err(e) => {
                                        eprintln!("❌ [binlog-tap] Error parsing row: {}", e);
                                    }
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

const BATCH_SIZE: usize = 1000;
const FLUSH_INTERVAL_SECS: u64 = 30;

pub async fn writer(mut rx: tokio::sync::mpsc::Receiver<CdcEvent>) -> BinlogTapResult<()> {
    tokio::spawn(async move {
        // Buffer per tabel: key = "db.table", value = Vec<CdcEvent>
        let mut buffers: std::collections::HashMap<String, Vec<CdcEvent>> =
            std::collections::HashMap::new();

        // Timer untuk flush periodik (setiap 30 detik)
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(FLUSH_INTERVAL_SECS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Event baru masuk dari worker
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            let key = format!("{}.{}", event.db_name, event.table_name);
                            let buf = buffers.entry(key.clone()).or_default();
                            buf.push(event);

                            // Flush kalau sudah mencapai BATCH_SIZE
                            if buf.len() >= BATCH_SIZE {
                                let rows = std::mem::take(buf);
                                tokio::spawn(flush_table(key, rows));
                            }
                        }
                        None => {
                            // Channel closed — flush sisa buffer semua tabel lalu exit
                            println!("📭 Channel closed, flushing remaining buffers...");
                            for (key, rows) in buffers.drain() {
                                if !rows.is_empty() {
                                    tokio::spawn(flush_table(key, rows));
                                }
                            }
                            break;
                        }
                    }
                }

                // Timer 30 detik — flush semua tabel yang punya pending events
                _ = ticker.tick() => {
                    let keys: Vec<String> = buffers
                        .iter()
                        .filter(|(_, v)| !v.is_empty())
                        .map(|(k, _)| k.clone())
                        .collect();

                    for key in keys {
                        let rows = std::mem::take(buffers.get_mut(&key).unwrap());
                        tokio::spawn(flush_table(key, rows)); // concurrent per tabel
                    }
                }
            }
        }
    });

    Ok(())
}

/// Flush satu tabel ke destination (saat ini: print, nanti: ClickHouse)
async fn flush_table(table: String, rows: Vec<CdcEvent>) {
    println!(
        "� [writer] Flushing {} rows ke {}",
        rows.len(),
        table
    );
}
