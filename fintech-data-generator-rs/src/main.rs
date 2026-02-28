mod db;
mod ingest;
mod utils;

use colored::*;
use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};
use sqlx::mysql::MySqlPoolOptions;
use std::time::Duration;
use tokio::time::sleep;
use chrono::Local;
use rand::RngExt;
use std::io::Write;

use crate::db::setup_database;
use crate::ingest::inject_batch;

fn print_header() {
    println!("\n{}", "=".repeat(65).cyan());
    println!("{}", "      🏦 FINTECH DATA GENERATOR - LIVE STREAM v6.0 (Rust Async)".white().bold());
    println!("{}\n", "=".repeat(65).cyan());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_header();

    let theme = ColorfulTheme::default();

    let reset_db = Confirm::with_theme(&theme)
        .with_prompt("Reset Database?")
        .default(false)
        .interact()?;

    let modes = &["Batch (Sekali jalan)", "Live Streaming (Terus-menerus)"];
    let mode_idx = Select::with_theme(&theme)
        .with_prompt("Pilih Mode:")
        .items(modes)
        .default(0)
        .interact()?;

    println!("{}", "Connecting to database...".yellow());
    // Try environment variable, fallback to default local connection
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:rootpassword@localhost:3306/fintech_db".to_string());
        
    let pool = MySqlPoolOptions::new()
        .max_connections(20)
        .connect(&db_url)
        .await?;

    setup_database(&pool, reset_db).await?;
    println!("{}", "✅ Database setup complete".green());

    if mode_idx == 0 {
        // Batch
        let volumes = &["Small", "Medium", "Large"];
        let vol_idx = Select::with_theme(&theme)
            .with_prompt("Volume:")
            .items(volumes)
            .default(0)
            .interact()?;

        let (u, m, t) = match vol_idx {
            0 => (10, 2, 50),
            1 => (100, 5, 500),
            _ => (500, 10, 2000),
        };

        println!("{}", "⚡ Starting batch generation...".yellow());
        inject_batch(&pool, u, m, t, false).await?;
        println!("{}", "✅ SUCCESS: Batch generation finished.".green());

    } else {
        // Live Streaming
        let interval: u64 = Input::with_theme(&theme)
            .with_prompt("Interval pengiriman (detik):")
            .default(10)
            .interact_text()?;
            
        let min_u: i32 = Input::with_theme(&theme).with_prompt("Min users per interval:").default(1).interact_text()?;
        let max_u: i32 = Input::with_theme(&theme).with_prompt("Max users per interval:").default(3).interact_text()?;
        let min_t: i32 = Input::with_theme(&theme).with_prompt("Min transactions per interval:").default(5).interact_text()?;
        let max_t: i32 = Input::with_theme(&theme).with_prompt("Max transactions per interval:").default(15).interact_text()?;

        println!("\n{}", "📡 LIVE STREAMING MODE".magenta().bold());
        println!("{}", format!("Interval: {} detik | Press Ctrl+C to stop.\n", interval).cyan());

        let mut rng = rand::rng();

        loop {
            let u_rand = rng.random_range(min_u..=max_u);
            let t_rand = rng.random_range(min_t..=max_t);
            let now = Local::now().format("%H:%M:%S");

            print!("[{}] 📥 Injecting {} users & {} tx...\r", now, u_rand, t_rand);
            std::io::stdout().flush().unwrap();

            if let Err(e) = inject_batch(&pool, u_rand, 0, t_rand, false).await {
                println!("\n{} {}", "❌ ERROR:".red(), e);
            }
            
            sleep(Duration::from_secs(interval)).await;
        }
    }

    Ok(())
}
