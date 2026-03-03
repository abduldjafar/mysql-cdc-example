use crate::errors::Result;
use mysql_async::binlog::events::RowsEventData;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Debug)]
pub struct CdcConfig {
    pub databases: Vec<DatabaseConfig>,
}

#[derive(Deserialize, Debug)]
pub struct DatabaseConfig {
    pub name: String,
    pub url: String,
    pub server_id: u32,
    pub exclude_tables: Option<Vec<String>>, // blacklist
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>, // override per tabel
}

#[derive(Deserialize, Debug)]
pub struct TableConfig {
    pub name: String,
    pub primary_key: String,
    pub columns: Option<Vec<String>>, // None = semua kolom
}

#[derive(Debug, Clone)]
pub struct CdcEvent {
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    pub event_type: EventType,
    pub columns: Vec<CdcColumn>,
}

#[derive(Debug, Clone)]
pub struct CdcColumn {
    pub name: Arc<str>,
    pub value: CdcValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum CdcValue {
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

impl EventType {
    pub fn from_mysql_event_type(event_type: &RowsEventData) -> Self {
        match event_type {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                EventType::Insert
            }
            RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => {
                EventType::Update
            }
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                EventType::Delete
            }
            _ => panic!("Unsupported event type"),
        }
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EventType::Insert => "INSERT",
            EventType::Update => "UPDATE",
            EventType::Delete => "DELETE",
        };
        write!(f, "{}", s)
    }
}
impl CdcValue {
    pub fn from_mysql_value(value: mysql_async::Value) -> Self {
        match value {
            mysql_async::Value::NULL => CdcValue::Null,
            mysql_async::Value::Bytes(bytes) => match String::from_utf8(bytes) {
                Ok(s) => CdcValue::String(s),
                // into_bytes() recovers the original Vec<u8> without extra allocation
                Err(e) => CdcValue::Bytes(e.into_bytes()),
            },
            mysql_async::Value::Int(v) => CdcValue::Int(v),
            mysql_async::Value::UInt(v) => CdcValue::Uint(v),
            mysql_async::Value::Float(v) => CdcValue::Float(v as f64),
            mysql_async::Value::Double(v) => CdcValue::Float(v),
            mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
                if hour == 0 && min == 0 && sec == 0 && micro == 0 {
                    CdcValue::Date(format!("{:04}-{:02}-{:02}", year, month, day))
                } else {
                    CdcValue::DateTime(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                        year, month, day, hour, min, sec, micro
                    ))
                }
            }
            mysql_async::Value::Time(neg, days, hours, mins, secs, micro) => {
                let sign = if neg { "-" } else { "" };
                let total_hours = days * 24 + hours as u32;
                CdcValue::Time(format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign, total_hours, mins, secs, micro
                ))
            }
        }
    }

    /// Convert dari BinlogValue (tipe yang dikembalikan BinlogRow::unwrap())
    pub fn from_binlog_value(value: mysql_async::binlog::value::BinlogValue<'static>) -> Self {
        use mysql_async::binlog::value::BinlogValue;
        match value {
            BinlogValue::Value(v) => Self::from_mysql_value(v),
            BinlogValue::Jsonb(jsonb) => {
                // Konversi JSONB ke JSON string
                match serde_json::Value::try_from(jsonb) {
                    Ok(json) => CdcValue::Json(json.to_string()),
                    Err(_) => CdcValue::Null,
                }
            }
            // JsonDiff hanya muncul di PartialUpdateRowsEvent
            BinlogValue::JsonDiff(_) => CdcValue::String("[json_diff]".to_string()),
        }
    }

    pub fn to_clikchouse_type(&self) -> String {
        match self {
            CdcValue::Null => "Null".to_string(),
            CdcValue::Bool(_) => "Bool".to_string(),
            CdcValue::Int(_) => "Int64".to_string(),
            CdcValue::Uint(_) => "UInt64".to_string(),
            CdcValue::Float(_) => "Float64".to_string(),
            CdcValue::Decimal(_) => "Decimal64(18,6)".to_string(),
            CdcValue::String(_) => "String".to_string(),
            CdcValue::Bytes(_) => "String".to_string(),
            CdcValue::Json(_) => "String".to_string(),
            CdcValue::Uuid(_) => "String".to_string(),
            CdcValue::Date(_) => "Date".to_string(),
            CdcValue::Time(_) => "Time".to_string(),
            CdcValue::DateTime(_) => "DateTime".to_string(),
        }
    }

    pub fn to_clickhouse_value(&self) -> String {
        match self {
            CdcValue::Null => "NULL".to_string(),
            CdcValue::Bool(v) => v.to_string(),
            CdcValue::Int(v) => v.to_string(),
            CdcValue::Uint(v) => v.to_string(),
            CdcValue::Float(v) => v.to_string(),
            CdcValue::Decimal(v) => v.to_string(),
            CdcValue::String(v) => format!("'{}'", v.replace("'", "''")),
            CdcValue::Bytes(v) => format!("'{}'", String::from_utf8_lossy(v)),
            CdcValue::Json(v) => format!("'{}'", v.replace("'", "''")),
            CdcValue::Uuid(v) => format!("'{}'", v),
            CdcValue::Date(v) => format!("'{}'", v),
            CdcValue::Time(v) => format!("'{}'", v),
            CdcValue::DateTime(v) => format!("'{}'", v),
        }
    }
}

impl CdcConfig {
    pub fn load_from_file(path: &str) -> Result<Self> {
        let config = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&config)?)
    }
}
