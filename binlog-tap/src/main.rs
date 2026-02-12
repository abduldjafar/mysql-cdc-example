use mysql_async::prelude::*;
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::{BinlogStreamRequest, Pool};
use futures::StreamExt;
use std::collections::HashMap;
use serde_json::json;

#[derive(Debug, Clone)]
struct TableSchema {
    table_name: String,
    columns: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "mysql://root:rootpassword@localhost:3306/fintech_db";
    let pool = Pool::new(database_url);
    
    println!("🔍 [binlog-tap] Melakukan snapshot skema...");
    let mut schema_registry: HashMap<u64, TableSchema> = HashMap::new();
    let mut name_to_columns: HashMap<String, Vec<String>> = HashMap::new();
    
    // Cache untuk menyimpan TableMapEvent mentah yang dibutuhkan oleh RowsEvent
    let mut table_map_events: HashMap<u64, TableMapEvent> = HashMap::new();

    // 1. Snapshot Columns
    {
        let mut conn = pool.get_conn().await?;
        let query = "
            SELECT TABLE_NAME, COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'fintech_db' 
            ORDER BY TABLE_NAME, ORDINAL_POSITION";
        
        let rows: Vec<(String, String)> = conn.query(query).await?;
        for (table_name, column_name) in rows {
            name_to_columns.entry(table_name).or_insert(vec![]).push(column_name);
        }
    }

    // 2. Start Binlog Stream
    let mut conn = pool.get_conn().await?;
    let request = BinlogStreamRequest::new(9999);
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    println!("🚀 [binlog-tap] Sinkronisasi dimulai...");

    while let Some(result) = binlog_stream.next().await {
        let event = result?;
        
        match event.read_data()? {
            Some(EventData::TableMapEvent(table_map)) => {
                let table_id = table_map.table_id();
                let table_name = table_map.table_name().to_string();

                // Simpan skema kolom
                if let Some(cols) = name_to_columns.get(&table_name) {
                    schema_registry.insert(table_id, TableSchema {
                        table_name: table_name.clone(),
                        columns: cols.clone(),
                    });
                }
                
                // Simpan event TableMap secara utuh untuk parsing Rows nanti
                // Kita perlu clone karena TableMapEvent akan dikonsumsi
                table_map_events.insert(table_id, table_map.into_owned());
            },

            Some(EventData::RowsEvent(rows_data)) => {
                if let RowsEventData::WriteRowsEvent(write_data) = rows_data {
                    let table_id = write_data.table_id();
                    
                    // Ambil TableMapEvent yang sudah kita simpan sebelumnya
                    if let (Some(schema), Some(tm_event)) = (schema_registry.get(&table_id), table_map_events.get(&table_id)) {
                        
                        // FIX: Masukkan tm_event sebagai argumen ke rows()
                        for row in write_data.rows(tm_event) {
                            let mut row_json = json!({});
                            
                            for (i, value) in row.iter().enumerate() {
                                if let Some(col_name) = schema.columns.get(i) {
                                    // Mapping value ke JSON (Sederhana)
                                    let val_str = format!("{:?}", value);
                                    row_json[col_name] = json!(val_str.replace("\"", ""));
                                }
                            }

                            let output = json!({
                                "event": "INSERT",
                                "table": schema.table_name,
                                "data": row_json,
                                "ts": event.header().timestamp()
                            });

                            println!("{}", serde_json::to_string(&output)?);
                        }
                    }
                }
            },
            _ => {}
        }
    }

    Ok(())
}
