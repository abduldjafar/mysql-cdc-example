use std::mem::size_of;

enum MinimalValue {
    Null,
    I64(i64),
    String(String),
}

fn main() {
    println!("String: {} bytes", size_of::<String>());
    println!("Vec<String>: {} bytes", size_of::<Vec<String>>());
    println!("MinimalValue: {} bytes", size_of::<MinimalValue>());
}
