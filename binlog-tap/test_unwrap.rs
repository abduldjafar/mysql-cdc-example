use mysql_async::binlog::events::BinlogRow;
fn foo(row: BinlogRow) { let x: Result<Vec<mysql_async::Value>, _> = row.unwrap(); }
