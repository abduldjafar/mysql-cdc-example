use sqlx::{MySqlPool, QueryBuilder};
use fake::faker::name::en::Name;
use fake::faker::internet::en::SafeEmail;
use fake::faker::phone_number::en::PhoneNumber;
use fake::faker::address::en::SecondaryAddress;
use fake::faker::company::en::{CompanyName, Bs};
use fake::Fake;
use rand::{RngExt, SeedableRng};
use chrono::{DateTime, Utc, Duration};
use uuid::Uuid;
use rust_decimal::Decimal;

use crate::utils::generate_nik;

const BATCH_SIZE: usize = 2000;

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
    let mut tx = pool.begin().await?;
    sqlx::query("SET SESSION binlog_row_metadata = FULL").execute(&mut *tx).await?;
    sqlx::query("SET SESSION binlog_row_image = FULL").execute(&mut *tx).await?;

    // 1. Users
    if n_users > 0 {
        let row: (Option<i32>,) = sqlx::query_as("SELECT MAX(user_id) FROM users")
            .fetch_one(&mut *tx)
            .await?;
        let start_u = row.0.unwrap_or(0) + 1;
        let mut created_users = Vec::new();

        for i in start_u..(start_u + n_users) {
            let created_date = get_random_date_between(time_range, now);
            let name: String = Name().fake();
            let email: String = SafeEmail().fake();
            let phone: String = PhoneNumber().fake();
            let address: String = SecondaryAddress().fake();
            
            created_users.push((i, name, email, phone, created_date, generate_nik(), address));
        }

        for chunk in created_users.chunks(BATCH_SIZE) {
            // Push users
            let mut q_users = QueryBuilder::new("INSERT IGNORE INTO users (user_id, full_name, email, phone, created_at) ");
            q_users.push_values(chunk, |mut b, user| {
                b.push_bind(user.0)
                 .push_bind(user.1.clone())
                 .push_bind(user.2.clone())
                 .push_bind(user.3.clone())
                 .push_bind(user.4);
            });
            q_users.build().execute(&mut *tx).await?;

            // Push identities
            let mut q_idents = QueryBuilder::new("INSERT IGNORE INTO user_identities (user_id, nik, address) ");
            q_idents.push_values(chunk, |mut b, user| {
                b.push_bind(user.0)
                 .push_bind(user.5.clone())
                 .push_bind(user.6.clone());
            });
            q_idents.build().execute(&mut *tx).await?;

            // Push wallets
            let mut q_wallets = QueryBuilder::new("INSERT IGNORE INTO wallets (user_id, balance) ");
            q_wallets.push_values(chunk, |mut b, user| {
                b.push_bind(user.0)
                 .push_bind(Decimal::ZERO);
            });
            q_wallets.build().execute(&mut *tx).await?;
        }
    }

    // 2. Merchants
    if n_merchants > 0 {
        let row_m: (Option<i32>,) = sqlx::query_as("SELECT MAX(merchant_id) FROM merchants")
            .fetch_one(&mut *tx)
            .await?;
        let start_m = row_m.0.unwrap_or(0) + 1;
        
        type MerchantTuple = (i32, String, String, DateTime<Utc>);
        let mut created_merchants: Vec<MerchantTuple> = Vec::new();
        let categories = ["F&B", "Retail", "Transport", "Digital"];

        for i in start_m..(start_m + n_merchants) {
            let company: String = CompanyName().fake();
            let category = categories[rng.random_range(0..categories.len())].to_string();
            created_merchants.push((i, company, category, Utc::now()));
        }

        for chunk in created_merchants.chunks(BATCH_SIZE) {
            let mut q_merchants = QueryBuilder::new("INSERT IGNORE INTO merchants (merchant_id, name, category, joined_at) ");
            q_merchants.push_values(chunk, |mut b, merchant| {
                b.push_bind(merchant.0)
                 .push_bind(merchant.1.clone())
                 .push_bind(merchant.2.clone())
                 .push_bind(merchant.3);
            });
            q_merchants.build().execute(&mut *tx).await?;
        }
    }

    // 3. Promos - Only if empty
    let promo_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM promotions").fetch_one(&mut *tx).await?;
    if promo_count.0 == 0 {
        type PromoTuple = (i32, String, i32, Decimal, &'static str);
        let mut created_promos: Vec<PromoTuple> = Vec::new();
        for i in 1..=10 {
            let word: String = fake::faker::lorem::en::Word().fake();
            let promo_name = format!("PROMO_{}_{}", word.to_uppercase(), i);
            let discount_pct = rng.random_range(5..=30);
            let max_discount = Decimal::from(100000);
            created_promos.push((i, promo_name, discount_pct, max_discount, "2028-12-31 23:59:59"));
        }
        
        let mut q_promos = QueryBuilder::new("INSERT IGNORE INTO promotions (promo_id, promo_name, discount_pct, max_discount, expired_at) ");
        q_promos.push_values(created_promos, |mut b, p| {
            b.push_bind(p.0).push_bind(p.1.clone()).push_bind(p.2).push_bind(p.3).push_bind(p.4);
        });
        q_promos.build().execute(&mut *tx).await?;
    }
    tx.commit().await?;

    // 4. Transactions
    if n_transactions == 0 {
        return Ok(());
    }

    let user_pool: Vec<(i32,)> = sqlx::query_as("SELECT user_id FROM users").fetch_all(pool).await?;
    let all_m: Vec<(i32,)> = sqlx::query_as("SELECT merchant_id FROM merchants").fetch_all(pool).await?;
    if user_pool.is_empty() || all_m.is_empty() {
        return Ok(());
    }

    let promos_db: Vec<(i32, i32, Decimal)> = 
        sqlx::query_as("SELECT promo_id, discount_pct, max_discount FROM promotions")
        .fetch_all(pool)
        .await?;

    let user_pool = std::sync::Arc::new(user_pool);
    let all_m = std::sync::Arc::new(all_m);
    let promos_db = std::sync::Arc::new(promos_db);

    let num_tasks = 30;
    let base_tx_per_task = n_transactions / num_tasks;
    let remainder = n_transactions % num_tasks;

    let mut join_set = tokio::task::JoinSet::new();

    for i in 0..num_tasks {
        let pool = pool.clone();
        let user_pool = std::sync::Arc::clone(&user_pool);
        let all_m = std::sync::Arc::clone(&all_m);
        let promos_db = std::sync::Arc::clone(&promos_db);
        
        let tx_for_this_task = if i == num_tasks - 1 {
            base_tx_per_task + remainder
        } else {
            base_tx_per_task
        };

        if tx_for_this_task == 0 {
            continue;
        }

        join_set.spawn(async move {
            let mut rng = rand::rngs::StdRng::from_rng(&mut rand::rng());
            let mut wallet_diff: std::collections::HashMap<i32, Decimal> = std::collections::HashMap::new();

            type TopupTuple = (String, i32, Decimal, String, DateTime<Utc>);
            type TxTuple = (String, i32, i32, Decimal, DateTime<Utc>);
            type HistoryTuple = (i32, Decimal, &'static str, &'static str, DateTime<Utc>);
            type TxItemTuple = (String, String, Decimal);
            type VoucherTuple = (String, i32, Decimal);

            let mut batch_topups: Vec<TopupTuple> = Vec::new();
            let mut batch_trans: Vec<TxTuple> = Vec::new();
            let mut batch_hist: Vec<HistoryTuple> = Vec::new();
            let mut batch_items: Vec<TxItemTuple> = Vec::new();
            let mut batch_vouchers: Vec<VoucherTuple> = Vec::new();

            let mut current_tx = match pool.begin().await {
                Ok(tx) => tx,
                Err(e) => return Err(e),
            };

            for _ in 0..tx_for_this_task {
                let user = user_pool[rng.random_range(0..user_pool.len())].0;
                let m_id = all_m[rng.random_range(0..all_m.len())].0;
                let tx_id = Uuid::new_v4().to_string();
                let tx_date = Utc::now();

                // Topup
                let topup_amt = Decimal::from(rng.random_range(500..=5000) * 1000);
                let channels = ["OVO", "GOPAY", "BCA"];
                let channel = channels[rng.random_range(0..channels.len())].to_string();

                *wallet_diff.entry(user).or_insert(Decimal::ZERO) += topup_amt;
                batch_topups.push((Uuid::new_v4().to_string(), user, topup_amt, channel, tx_date));
                batch_hist.push((user, topup_amt, "CREDIT", "Topup", tx_date));

                // Purchase
                let base_amt = Decimal::from(rng.random_range(20..=500) * 1000);
                let p = if rng.random_bool(0.6) && !promos_db.is_empty() {
                    Some(&promos_db[rng.random_range(0..promos_db.len())])
                } else {
                    None
                };
                
                let discount = match p {
                    Some(promo) => {
                        let calc_disc = base_amt * Decimal::from(promo.1) / Decimal::from(100);
                        calc_disc.min(promo.2)
                    }
                    None => Decimal::ZERO
                };
                let final_amt = base_amt - discount;

                *wallet_diff.entry(user).or_insert(Decimal::ZERO) -= final_amt;

                batch_trans.push((tx_id.clone(), user, m_id, final_amt, tx_date));
                batch_hist.push((user, final_amt, "DEBIT", "Payment", tx_date));
                
                let item_name: String = Bs().fake();
                batch_items.push((tx_id.clone(), item_name, final_amt));
                
                if let Some(promo) = p {
                    batch_vouchers.push((tx_id.clone(), promo.0, discount));
                }

                if batch_trans.len() >= BATCH_SIZE {
                    flush_transactions_memory(
                        &mut current_tx, &mut batch_topups, &mut batch_trans, 
                        &mut batch_hist, &mut batch_items, &mut batch_vouchers, 
                        &mut wallet_diff,
                    ).await?;
                    current_tx.commit().await?;
                    current_tx = pool.begin().await?;
                }
            }

            if !batch_trans.is_empty() {
                flush_transactions_memory(
                    &mut current_tx, &mut batch_topups, &mut batch_trans, 
                    &mut batch_hist, &mut batch_items, &mut batch_vouchers, 
                    &mut wallet_diff,
                ).await?;
                current_tx.commit().await?;
            }

            Ok::<(), sqlx::Error>(())
        });
    }

    while let Some(res) = join_set.join_next().await {
        if let Ok(Err(e)) = res {
            return Err(e);
        }
    }

    Ok(())
}

async fn flush_transactions_memory(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    topups: &mut Vec<(String, i32, Decimal, String, DateTime<Utc>)>,
    trans: &mut Vec<(String, i32, i32, Decimal, DateTime<Utc>)>,
    hist: &mut Vec<(i32, Decimal, &'static str, &'static str, DateTime<Utc>)>,
    items: &mut Vec<(String, String, Decimal)>,
    vouchers: &mut Vec<(String, i32, Decimal)>,
    wallet_diff: &mut std::collections::HashMap<i32, Decimal>,
) -> Result<(), sqlx::Error> {
    
    // Wallets (Batched UPSERT or individual UPDATEs)
    // Since SQLx QueryBuilder for MySQL doesn't natively map to UPDATE easily without ON DUPLICATE KEY 
    // we use a CASE WHEN or just execute individual queries since wallet_diff is aggregated per user.
    // If it's too many users, ON DUPLICATE KEY UPDATE is fastest.
    if !wallet_diff.is_empty() {
        let mut q_wallet = QueryBuilder::new("INSERT INTO wallets (user_id, balance) ");
        let entries: Vec<_> = wallet_diff.drain().collect();
        q_wallet.push_values(&entries, |mut b, (u, diff)| {
            b.push_bind(*u).push_bind(*diff);
        });
        q_wallet.push(" ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)");
        q_wallet.build().execute(&mut **tx).await?;
    }

    if !topups.is_empty() {
        let mut q_topups = QueryBuilder::new("INSERT INTO topups (topup_id, user_id, amount, channel, created_at) ");
        q_topups.push_values(topups.drain(..), |mut b, t| {
            b.push_bind(t.0).push_bind(t.1).push_bind(t.2).push_bind(t.3).push_bind(t.4);
        });
        q_topups.build().execute(&mut **tx).await?;
    }

    if !trans.is_empty() {
        let mut q_trans = QueryBuilder::new("INSERT INTO transactions (tx_id, user_id, merchant_id, total_amount, status, created_at) ");
        q_trans.push_values(trans.drain(..), |mut b, t| {
            b.push_bind(t.0).push_bind(t.1).push_bind(t.2).push_bind(t.3).push_bind("SUCCESS").push_bind(t.4);
        });
        q_trans.build().execute(&mut **tx).await?;
    }

    if !hist.is_empty() {
        let mut q_hist = QueryBuilder::new("INSERT INTO wallet_histories (user_id, amount, type, description, created_at) ");
        q_hist.push_values(hist.drain(..), |mut b, t| {
            b.push_bind(t.0).push_bind(t.1).push_bind(t.2).push_bind(t.3).push_bind(t.4);
        });
        q_hist.build().execute(&mut **tx).await?;
    }

    if !items.is_empty() {
        let mut q_items = QueryBuilder::new("INSERT INTO transaction_items (tx_id, item_name, price, qty) ");
        q_items.push_values(items.drain(..), |mut b, t| {
            b.push_bind(t.0).push_bind(t.1).push_bind(t.2).push_bind(1);
        });
        q_items.build().execute(&mut **tx).await?;
    }

    if !vouchers.is_empty() {
        let mut q_vouch = QueryBuilder::new("INSERT INTO transaction_vouchers (tx_id, promo_id, discount_amount) ");
        q_vouch.push_values(vouchers.drain(..), |mut b, t| {
            b.push_bind(t.0).push_bind(t.1).push_bind(t.2);
        });
        q_vouch.build().execute(&mut **tx).await?;
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
