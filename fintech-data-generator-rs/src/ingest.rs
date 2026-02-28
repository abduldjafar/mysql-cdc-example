use sqlx::MySqlPool;
use fake::faker::name::en::Name;
use fake::faker::internet::en::SafeEmail;
use fake::faker::phone_number::en::PhoneNumber;
use fake::faker::address::en::SecondaryAddress;
use fake::faker::company::en::{CompanyName, Bs};
use fake::Fake;
use rand::RngExt;
use chrono::{DateTime, Utc, Duration};
use uuid::Uuid;
use rust_decimal::Decimal;

use crate::utils::generate_nik;

pub async fn inject_batch(
    pool: &MySqlPool,
    n_users: i32,
    n_merchants: i32,
    n_transactions: i32,
    is_historical: bool,
) -> Result<(), sqlx::Error> {
    let now = Utc::now();
    let time_range = if is_historical {
        now - Duration::try_days(3 * 365).unwrap()
    } else {
        now - Duration::try_days(1).unwrap()
    };

    let mut rng = rand::rng();

    // 1. Users
    let row: (Option<i32>,) = sqlx::query_as("SELECT MAX(user_id) FROM users")
        .fetch_one(pool)
        .await?;
    let start_u = row.0.unwrap_or(0) + 1;

    // Use transaction for faster batch inserts
    let mut tx = pool.begin().await?;

    for i in start_u..(start_u + n_users) {
        let created_date = get_random_date_between(time_range, now);
        
        let name: String = Name().fake();
        let email: String = SafeEmail().fake();
        let phone: String = PhoneNumber().fake();
        
        sqlx::query("INSERT IGNORE INTO users VALUES (?, ?, ?, ?, ?)")
            .bind(i)
            .bind(name)
            .bind(email)
            .bind(phone)
            .bind(created_date)
            .execute(&mut *tx)
            .await?;

        let address: String = SecondaryAddress().fake();
        sqlx::query("INSERT IGNORE INTO user_identities (user_id, nik, address) VALUES (?, ?, ?)")
            .bind(i)
            .bind(generate_nik())
            .bind(address)
            .execute(&mut *tx)
            .await?;

        sqlx::query("INSERT IGNORE INTO wallets (user_id, balance) VALUES (?, ?)")
            .bind(i)
            .bind(Decimal::ZERO)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    let user_pool: Vec<(i32, chrono::NaiveDateTime)> = sqlx::query_as("SELECT user_id, created_at FROM users")
        .fetch_all(pool)
        .await?;

    // 2. Merchants
    let row_m: (Option<i32>,) = sqlx::query_as("SELECT MAX(merchant_id) FROM merchants")
        .fetch_one(pool)
        .await?;
    let start_m = row_m.0.unwrap_or(0) + 1;
    let mut tx_m = pool.begin().await?;

    for i in start_m..(start_m + n_merchants) {
        let company: String = CompanyName().fake();
        let categories = ["F&B", "Retail", "Transport", "Digital"];
        let category = categories[rng.random_range(0..categories.len())];
        
        sqlx::query("INSERT IGNORE INTO merchants VALUES (?, ?, ?, ?)")
            .bind(i)
            .bind(company)
            .bind(category)
            .bind(Utc::now())
            .execute(&mut *tx_m)
            .await?;
    }
    tx_m.commit().await?;

    // 3. Promos
    let mut tx_p = pool.begin().await?;
    for i in 1..=10 {
        let word: String = fake::faker::lorem::en::Word().fake();
        let promo_name = format!("PROMO_{}_{}", word.to_uppercase(), i);
        let discount_pct = rng.random_range(5..=30);
        let max_discount = 100000.0;
        
        sqlx::query("INSERT IGNORE INTO promotions VALUES (?, ?, ?, ?, ?)")
            .bind(i)
            .bind(promo_name)
            .bind(discount_pct)
            .bind(max_discount)
            .bind("2028-12-31 23:59:59")
            .execute(&mut *tx_p)
            .await?;
    }
    tx_p.commit().await?;

    let all_m: Vec<(i32,)> = sqlx::query_as("SELECT merchant_id FROM merchants")
        .fetch_all(pool)
        .await?;
    
    // 4. Transactions
    if user_pool.is_empty() || all_m.is_empty() {
        return Ok(());
    }

    // Load promos into memory
    let promos_db: Vec<(i32, String, i32, Decimal, chrono::NaiveDateTime)> = 
        sqlx::query_as("SELECT promo_id, promo_name, discount_pct, max_discount, expired_at FROM promotions")
        .fetch_all(pool)
        .await?;

    let mut current_tx = pool.begin().await?;
    let mut batch_count = 0;

    for _ in 0..n_transactions {
        let user = &user_pool[rng.random_range(0..user_pool.len())];
        let m_id = all_m[rng.random_range(0..all_m.len())].0;
        let tx_id = Uuid::new_v4().to_string();
        let tx_date = Utc::now();

        // Topup logic
        let topup_amt = Decimal::from(rng.random_range(500..=5000) * 1000);
        sqlx::query("UPDATE wallets SET balance = balance + ? WHERE user_id = ?")
            .bind(topup_amt)
            .bind(user.0)
            .execute(&mut *current_tx)
            .await?;
            
        let channels = ["OVO", "GOPAY", "BCA"];
        let channel = channels[rng.random_range(0..channels.len())];
        sqlx::query("INSERT INTO topups VALUES (?, ?, ?, ?, ?)")
            .bind(Uuid::new_v4().to_string())
            .bind(user.0)
            .bind(topup_amt)
            .bind(channel)
            .bind(tx_date)
            .execute(&mut *current_tx)
            .await?;
            
        sqlx::query("INSERT INTO wallet_histories (user_id, amount, type, description, created_at) VALUES (?, ?, 'CREDIT', 'Topup', ?)")
            .bind(user.0)
            .bind(topup_amt)
            .bind(tx_date)
            .execute(&mut *current_tx)
            .await?;

        // Transaction logic
        let base_amt = Decimal::from(rng.random_range(20..=500) * 1000);
        let p = if rng.random_bool(0.6) && !promos_db.is_empty() {
            Some(&promos_db[rng.random_range(0..promos_db.len())])
        } else {
            None
        };
        
        let discount = match p {
            Some(promo) => {
                let calc_disc = base_amt * Decimal::from(promo.2) / Decimal::from(100);
                calc_disc.min(promo.3)
            }
            None => Decimal::ZERO
        };
        let final_amt = base_amt - discount;

        sqlx::query("INSERT INTO transactions VALUES (?, ?, ?, ?, 'SUCCESS', ?)")
            .bind(&tx_id)
            .bind(user.0)
            .bind(m_id)
            .bind(final_amt)
            .bind(tx_date)
            .execute(&mut *current_tx)
            .await?;
            
        sqlx::query("UPDATE wallets SET balance = balance - ? WHERE user_id = ?")
            .bind(final_amt)
            .bind(user.0)
            .execute(&mut *current_tx)
            .await?;
            
        sqlx::query("INSERT INTO wallet_histories (user_id, amount, type, description, created_at) VALUES (?, ?, 'DEBIT', 'Payment', ?)")
            .bind(user.0)
            .bind(final_amt)
            .bind(tx_date)
            .execute(&mut *current_tx)
            .await?;
            
        let item_name: String = Bs().fake();
        sqlx::query("INSERT INTO transaction_items (tx_id, item_name, price, qty) VALUES (?, ?, ?, 1)")
            .bind(&tx_id)
            .bind(item_name)
            .bind(final_amt)
            .execute(&mut *current_tx)
            .await?;
            
        if let Some(promo) = p {
            sqlx::query("INSERT INTO transaction_vouchers (tx_id, promo_id, discount_amount) VALUES (?, ?, ?)")
                .bind(&tx_id)
                .bind(promo.0)
                .bind(discount)
                .execute(&mut *current_tx)
                .await?;
        }

        batch_count += 1;
        if batch_count >= 500 { // commit every 500 transactions to manage memory size
            current_tx.commit().await?;
            current_tx = pool.begin().await?;
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        current_tx.commit().await?;
    }

    Ok(())
}

fn get_random_date_between(start: DateTime<Utc>, end: DateTime<Utc>) -> DateTime<Utc> {
    let mut rng = rand::rng();
    let duration = end.timestamp() - start.timestamp();
    if duration <= 0 {
        return start;
    }
    let random_seconds = rng.random_range(0..duration);
    start + Duration::try_seconds(random_seconds).unwrap()
}
