/// Benchmark: Zero-Copy vs Arc-based CDC Event Parsing
///
/// This benchmark demonstrates the performance difference between:
/// 1. Original Arc-based CdcEvent (allocates on every field)
/// 2. Zero-copy CdcEventRef (borrows from source, minimal allocations)
///
/// Run with:
/// ```bash
/// cargo bench --bench zero_copy_bench
/// ```
///
/// Or just run the simple comparison:
/// ```bash
/// cargo run --release --bin bench_comparison
/// ```

use binlog_tap::config::{CdcColumn, CdcEvent, CdcValue, EventType};
use binlog_tap::zero_copy::{CdcColumnRef, CdcEventRef, CdcValueRef};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    println!("=== Zero-Copy CDC Benchmark ===\n");

    // Simulate source data (would come from binlog in real scenario)
    let db_name = "test_database";
    let table_name = "users";
    let column_names = vec!["id", "email", "username", "created_at", "status"];
    let column_values = vec![
        "12345",
        "user@example.com",
        "john_doe",
        "2024-01-15 10:30:45.123456",
        "active",
    ];

    const ITERATIONS: usize = 100_000;

    // Benchmark 1: Arc-based (original approach)
    println!("Running Arc-based approach ({} iterations)...", ITERATIONS);
    let start = Instant::now();
    let mut total_allocs_arc = 0;

    for _ in 0..ITERATIONS {
        let event = create_event_arc_based(&db_name, &table_name, &column_names, &column_values);
        total_allocs_arc += count_allocations_arc(&event);
        // Simulate some work
        std::hint::black_box(event);
    }

    let duration_arc = start.elapsed();
    println!("✓ Completed in {:?}", duration_arc);
    println!(
        "  Allocations: {} total ({} per event)\n",
        total_allocs_arc,
        total_allocs_arc / ITERATIONS
    );

    // Benchmark 2: Zero-copy approach
    println!(
        "Running Zero-Copy approach ({} iterations)...",
        ITERATIONS
    );
    let start = Instant::now();
    let mut total_allocs_zero = 0;

    for _ in 0..ITERATIONS {
        let event_ref = create_event_zero_copy(&db_name, &table_name, &column_names, &column_values);
        total_allocs_zero += count_allocations_zero(&event_ref);
        // Simulate some work
        std::hint::black_box(event_ref);
    }

    let duration_zero = start.elapsed();
    println!("✓ Completed in {:?}", duration_zero);
    println!(
        "  Allocations: {} total ({} per event)\n",
        total_allocs_zero,
        total_allocs_zero / ITERATIONS
    );

    // Comparison
    println!("=== Results ===");
    println!("Arc-based:  {:?} ({} alloc/event)", duration_arc, total_allocs_arc / ITERATIONS);
    println!("Zero-Copy:  {:?} ({} alloc/event)", duration_zero, total_allocs_zero / ITERATIONS);
    println!();
    println!(
        "Speed improvement: {:.2}x faster",
        duration_arc.as_secs_f64() / duration_zero.as_secs_f64()
    );
    println!(
        "Allocation reduction: {:.1}% fewer",
        (1.0 - (total_allocs_zero as f64 / total_allocs_arc as f64)) * 100.0
    );

    // Memory estimate
    let mem_arc = total_allocs_arc * 24; // Rough estimate: 24 bytes per Arc
    let mem_zero = total_allocs_zero * 16; // Rough estimate: 16 bytes per ref
    println!(
        "Estimated memory saved: {} MB",
        (mem_arc - mem_zero) / 1_000_000
    );
}

/// Original Arc-based approach
fn create_event_arc_based(
    db_name: &str,
    table_name: &str,
    column_names: &[&str],
    column_values: &[&str],
) -> CdcEvent {
    CdcEvent {
        db_name: Arc::from(db_name),      // Allocation!
        table_name: Arc::from(table_name), // Allocation!
        event_type: EventType::Insert,
        columns: column_names
            .iter()
            .zip(column_values.iter())
            .map(|(name, value)| CdcColumn {
                name: Arc::from(*name),                 // Allocation per column!
                value: CdcValue::String(value.to_string()), // Allocation per value!
            })
            .collect(), // Vec allocation!
    }
}

/// Zero-copy approach
fn create_event_zero_copy<'a>(
    db_name: &'a str,
    table_name: &'a str,
    column_names: &'a [&'a str],
    column_values: &'a [&'a str],
) -> CdcEventRef<'a> {
    CdcEventRef {
        db_name,    // Borrow, no allocation!
        table_name, // Borrow, no allocation!
        event_type: EventType::Insert,
        columns: column_names
            .iter()
            .zip(column_values.iter())
            .map(|(name, value)| CdcColumnRef {
                name,                              // Borrow, no allocation!
                value: CdcValueRef::String(value), // Borrow, no allocation!
            })
            .collect(), // Vec allocation (only 1!)
    }
}

/// Count allocations in Arc-based event
fn count_allocations_arc(event: &CdcEvent) -> usize {
    1 + // db_name Arc
    1 + // table_name Arc
    1 + // columns Vec
    event.columns.len() * 2 // Each column: name Arc + value allocation
}

/// Count allocations in zero-copy event
fn count_allocations_zero(_event: &CdcEventRef) -> usize {
    1 // Only the Vec allocation!
}
