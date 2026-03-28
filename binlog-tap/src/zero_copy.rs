/// Zero-Copy CDC Types with Lifetimes
///
/// These types borrow data from the source buffer instead of allocating.
/// Use these during parsing, then convert to owned CdcEvent when crossing thread boundaries.
use crate::config::{CdcColumn, CdcEvent, CdcValue, EventType};
use mysql_async::binlog::value::BinlogValue;
use std::borrow::Cow;
use std::sync::Arc;

/// Zero-copy event that borrows from the source binlog buffer
#[derive(Debug, Clone)]
pub struct CdcEventRef<'a> {
    pub db_name: &'a str,
    pub table_name: &'a str,
    pub event_type: EventType,
    pub columns: Vec<CdcColumnRef<'a>>,
}

/// Zero-copy column that borrows the column name and value
#[derive(Debug, Clone)]
pub struct CdcColumnRef<'a> {
    pub name: &'a str,
    pub value: CdcValueRef<'a>,
    pub is_primary_key: bool,
}

/// Zero-copy value enum that borrows string/bytes data
#[derive(Debug, Clone)]
pub enum CdcValueRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    /// Decimal stored as borrowed string slice
    Decimal(Cow<'a, str>),
    /// String stored as borrowed slice - zero allocation!
    String(&'a str),
    /// Bytes stored as borrowed slice - zero allocation!
    Bytes(&'a [u8]),
    Json(Cow<'a, str>),
    Uuid(Cow<'a, str>),
    Date(Cow<'a, str>),
    Time(Cow<'a, str>),
    DateTime(Cow<'a, str>),
}

impl<'a> CdcEventRef<'a> {
    /// Convert zero-copy event to owned event for sending through channels
    pub fn to_owned_event(&self, db_name: Arc<str>, table_name: Arc<str>) -> CdcEvent {
        CdcEvent {
            db_name,
            table_name,
            event_type: self.event_type,
            columns: self
                .columns
                .iter()
                .map(|col| col.to_owned_column())
                .collect(),
        }
    }
}

impl<'a> CdcColumnRef<'a> {
    /// Convert zero-copy column to owned column
    pub fn to_owned_column(&self) -> CdcColumn {
        CdcColumn {
            name: Arc::from(self.name),
            value: self.value.to_owned_value(),
            is_primary_key: self.is_primary_key,
        }
    }
}

impl<'a> CdcValueRef<'a> {
    /// Convert zero-copy value to owned value
    pub fn to_owned_value(&self) -> CdcValue {
        match self {
            CdcValueRef::Null => CdcValue::Null,
            CdcValueRef::Bool(v) => CdcValue::Bool(*v),
            CdcValueRef::Int(v) => CdcValue::Int(*v),
            CdcValueRef::Uint(v) => CdcValue::Uint(*v),
            CdcValueRef::Float(v) => CdcValue::Float(*v),
            CdcValueRef::Decimal(v) => CdcValue::Decimal(v.to_string()),
            CdcValueRef::String(v) => CdcValue::String((*v).to_string()),
            CdcValueRef::Bytes(v) => CdcValue::Bytes(v.to_vec()),
            CdcValueRef::Json(v) => CdcValue::Json(v.to_string()),
            CdcValueRef::Uuid(v) => CdcValue::Uuid(v.to_string()),
            CdcValueRef::Date(v) => CdcValue::Date(v.to_string()),
            CdcValueRef::Time(v) => CdcValue::Time(v.to_string()),
            CdcValueRef::DateTime(v) => CdcValue::DateTime(v.to_string()),
        }
    }

    /// Create from BinlogValue with zero-copy where possible
    ///
    /// IMPORTANT: The BinlogValue must have 'static lifetime from mysql_async,
    /// so we can safely cast it to 'a. The returned CdcValueRef borrows from
    /// the BinlogValue's owned data.
    pub fn from_binlog_value_ref(value: &'a BinlogValue<'static>) -> Self {
        match value {
            BinlogValue::Value(v) => Self::from_mysql_value_ref(v),
            BinlogValue::Jsonb(jsonb) => {
                // JSONB requires serialization, so we use Cow::Owned
                match serde_json::Value::try_from(jsonb.clone()) {
                    Ok(json) => CdcValueRef::Json(Cow::Owned(json.to_string())),
                    Err(_) => CdcValueRef::Null,
                }
            }
            BinlogValue::JsonDiff(_) => CdcValueRef::String("[json_diff]"),
        }
    }

    fn from_mysql_value_ref(value: &'a mysql_async::Value) -> Self {
        match value {
            mysql_async::Value::NULL => CdcValueRef::Null,
            mysql_async::Value::Bytes(bytes) => {
                // Try to interpret as UTF-8 string (zero-copy!)
                match std::str::from_utf8(bytes) {
                    Ok(s) => CdcValueRef::String(s),
                    Err(_) => CdcValueRef::Bytes(bytes.as_slice()),
                }
            }
            mysql_async::Value::Int(v) => CdcValueRef::Int(*v),
            mysql_async::Value::UInt(v) => CdcValueRef::Uint(*v),
            mysql_async::Value::Float(v) => CdcValueRef::Float(*v as f64),
            mysql_async::Value::Double(v) => CdcValueRef::Float(*v),
            mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
                if *hour == 0 && *min == 0 && *sec == 0 && *micro == 0 {
                    CdcValueRef::Date(Cow::Owned(format!("{:04}-{:02}-{:02}", year, month, day)))
                } else {
                    CdcValueRef::DateTime(Cow::Owned(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                        year, month, day, hour, min, sec, micro
                    )))
                }
            }
            mysql_async::Value::Time(neg, days, hours, mins, secs, micro) => {
                let sign = if *neg { "-" } else { "" };
                let total_hours = days * 24 + *hours as u32;
                CdcValueRef::Time(Cow::Owned(format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign, total_hours, mins, secs, micro
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_string() {
        let source = "hello world";
        let value = CdcValueRef::String(source);

        // Verify it's actually borrowing
        match value {
            CdcValueRef::String(s) => {
                assert_eq!(s, "hello world");
                // Same pointer = zero copy!
                assert_eq!(s.as_ptr(), source.as_ptr());
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_conversion_to_owned() {
        let source = "test string";
        let zero_copy = CdcValueRef::String(source);
        let owned = zero_copy.to_owned_value();

        match owned {
            CdcValue::String(s) => assert_eq!(s, "test string"),
            _ => panic!("Wrong variant"),
        }
    }
}
