use sqlx::MySqlPool;

pub async fn setup_database(pool: &MySqlPool, reset: bool) -> Result<(), sqlx::Error> {
    if reset {
        println!("🧨 Resetting database schema...");
        sqlx::query("SET FOREIGN_KEY_CHECKS = 0").execute(pool).await?;
        sqlx::query("SET SESSION binlog_row_metadata = FULL").execute(pool).await?;
        sqlx::query("SET SESSION binlog_row_image = FULL").execute(pool).await?;
        
        let tables = [
            "transaction_vouchers", "transaction_items", "transactions", 
            "topups", "wallet_histories", "wallets", "user_identities", 
            "merchants", "users", "promotions"
        ];
        
        for table in tables {
            let q = format!("DROP TABLE IF EXISTS {}", table);
            sqlx::query(&q).execute(pool).await?;
        }
        
        sqlx::query("SET FOREIGN_KEY_CHECKS = 1").execute(pool).await?;
    }

    // Create Tables
    let schema_queries = [
        "CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, full_name VARCHAR(100), email VARCHAR(100), phone VARCHAR(50), created_at DATETIME)",
        "CREATE TABLE IF NOT EXISTS user_identities (identity_id INT PRIMARY KEY AUTO_INCREMENT, user_id INT UNIQUE, nik CHAR(16), address TEXT, FOREIGN KEY (user_id) REFERENCES users(user_id))",
        "CREATE TABLE IF NOT EXISTS merchants (merchant_id INT PRIMARY KEY, name VARCHAR(100), category ENUM('F&B', 'Retail', 'Transport', 'Digital'), joined_at DATETIME)",
        "CREATE TABLE IF NOT EXISTS wallets (wallet_id INT PRIMARY KEY AUTO_INCREMENT, user_id INT UNIQUE, balance DECIMAL(18,2) DEFAULT 0, last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(user_id))",
        "CREATE TABLE IF NOT EXISTS wallet_histories (history_id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, amount DECIMAL(18,2), type ENUM('DEBIT', 'CREDIT'), description VARCHAR(255), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id))",
        "CREATE TABLE IF NOT EXISTS promotions (promo_id INT PRIMARY KEY, promo_name VARCHAR(100), discount_pct INT, max_discount DECIMAL(18,2), expired_at DATETIME)",
        "CREATE TABLE IF NOT EXISTS transactions (tx_id VARCHAR(50) PRIMARY KEY, user_id INT, merchant_id INT, total_amount DECIMAL(18,2), status ENUM('SUCCESS', 'FAILED', 'PENDING'), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id), FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id))",
        "CREATE TABLE IF NOT EXISTS transaction_items (item_id INT AUTO_INCREMENT PRIMARY KEY, tx_id VARCHAR(50), item_name VARCHAR(100), price DECIMAL(18,2), qty INT, FOREIGN KEY (tx_id) REFERENCES transactions(tx_id))",
        "CREATE TABLE IF NOT EXISTS transaction_vouchers (voucher_id INT AUTO_INCREMENT PRIMARY KEY, tx_id VARCHAR(50), promo_id INT, discount_amount DECIMAL(18,2), FOREIGN KEY (tx_id) REFERENCES transactions(tx_id), FOREIGN KEY (promo_id) REFERENCES promotions(promo_id))",
        "CREATE TABLE IF NOT EXISTS topups (topup_id VARCHAR(50) PRIMARY KEY, user_id INT, amount DECIMAL(18,2), channel VARCHAR(255), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id))",
    ];

    for q in schema_queries {
        sqlx::query(q).execute(pool).await?;
    }

    Ok(())
}
