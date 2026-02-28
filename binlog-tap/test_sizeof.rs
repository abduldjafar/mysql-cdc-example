use std::mem::size_of;

enum CdcValue {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Uuid(String),
    Date(String),
    Time(String),
    DateTime(String),
}

struct CdcColumn {
    name: std::sync::Arc<str>,
    value: CdcValue,
}

struct CdcEvent {
    db_name: std::sync::Arc<str>,
    table_name: std::sync::Arc<str>,
    event_type: u8,
    columns: Vec<CdcColumn>,
}

fn main() {
    println!("CdcValue: {} bytes", size_of::<CdcValue>());
    println!("CdcColumn: {} bytes", size_of::<CdcColumn>());
    println!("Vec<CdcColumn>: {} bytes", size_of::<Vec<CdcColumn>>());
    println!("CdcEvent: {} bytes", size_of::<CdcEvent>());
}
