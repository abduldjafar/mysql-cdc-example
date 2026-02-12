import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
import uuid
import sys
import time
from datetime import datetime

try:
    import questionary
    from colorama import Fore, Style, init
except ImportError:
    print("❌ Library UI belum lengkap. Jalankan: pip install questionary colorama")
    sys.exit(1)

# Inisialisasi
init(autoreset=True)
fake = Faker('id_ID')

def print_header():
    print(f"\n{Fore.CYAN}{'='*65}")
    print(f"{Style.BRIGHT}{Fore.WHITE}      🏦 FINTECH DATA GENERATOR - LIVE STREAM v5.1")
    print(f"{Fore.CYAN}{'='*65}\n")

def generate_nik():
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def setup_database(cursor, reset=False):
    if reset:
        print(f"{Fore.RED}🧨 Resetting database schema...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = ["transaction_vouchers", "transaction_items", "transactions", "topups", "wallet_histories", "wallets", "user_identities", "merchants", "users", "promotions"]
        for t in tables: cursor.execute(f"DROP TABLE IF EXISTS {t}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create Tables
    cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, full_name VARCHAR(100), email VARCHAR(100), phone VARCHAR(50), created_at DATETIME)")
    cursor.execute("CREATE TABLE IF NOT EXISTS user_identities (identity_id INT PRIMARY KEY AUTO_INCREMENT, user_id INT UNIQUE, nik CHAR(16), address TEXT, FOREIGN KEY (user_id) REFERENCES users(user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS merchants (merchant_id INT PRIMARY KEY, name VARCHAR(100), category ENUM('F&B', 'Retail', 'Transport', 'Digital'), joined_at DATETIME)")
    cursor.execute("CREATE TABLE IF NOT EXISTS wallets (wallet_id INT PRIMARY KEY AUTO_INCREMENT, user_id INT UNIQUE, balance DECIMAL(18,2) DEFAULT 0, last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS wallet_histories (history_id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, amount DECIMAL(18,2), type ENUM('DEBIT', 'CREDIT'), description VARCHAR(255), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS promotions (promo_id INT PRIMARY KEY, promo_name VARCHAR(100), discount_pct INT, max_discount DECIMAL(18,2), expired_at DATETIME)")
    cursor.execute("CREATE TABLE IF NOT EXISTS transactions (tx_id VARCHAR(50) PRIMARY KEY, user_id INT, merchant_id INT, total_amount DECIMAL(18,2), status ENUM('SUCCESS', 'FAILED', 'PENDING'), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id), FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS transaction_items (item_id INT AUTO_INCREMENT PRIMARY KEY, tx_id VARCHAR(50), item_name VARCHAR(100), price DECIMAL(18,2), qty INT, FOREIGN KEY (tx_id) REFERENCES transactions(tx_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS transaction_vouchers (voucher_id INT AUTO_INCREMENT PRIMARY KEY, tx_id VARCHAR(50), promo_id INT, discount_amount DECIMAL(18,2), FOREIGN KEY (tx_id) REFERENCES transactions(tx_id), FOREIGN KEY (promo_id) REFERENCES promotions(promo_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS topups (topup_id VARCHAR(50) PRIMARY KEY, user_id INT, amount DECIMAL(18,2), channel VARCHAR(255), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id))")

def inject_batch(cursor, n_users, n_merchants, n_transactions, is_historical):
    """Fungsi inti untuk melakukan satu kali batch injection"""
    time_range = "-3y" if is_historical else "-1d"
    
    # 1. Users
    cursor.execute("SELECT MAX(user_id) as m FROM users")
    start_u = (cursor.fetchone()['m'] or 0) + 1
    for i in range(start_u, start_u + n_users):
        created_date = fake.date_time_between(start_date=time_range, end_date="now")
        cursor.execute("INSERT IGNORE INTO users VALUES (%s, %s, %s, %s, %s)", (i, fake.name(), fake.email(), fake.phone_number(), created_date))
        cursor.execute("INSERT IGNORE INTO user_identities (user_id, nik, address) VALUES (%s, %s, %s)", (i, generate_nik(), fake.address().replace('\n', ' ')))
        cursor.execute("INSERT IGNORE INTO wallets (user_id, balance) VALUES (%s, %s)", (i, 0.0))

    cursor.execute("SELECT user_id, created_at FROM users")
    user_pool = cursor.fetchall()

    # 2. Merchants
    cursor.execute("SELECT MAX(merchant_id) as m FROM merchants")
    start_m = (cursor.fetchone()['m'] or 0) + 1
    for i in range(start_m, start_m + n_merchants):
        cursor.execute("INSERT IGNORE INTO merchants VALUES (%s, %s, %s, %s)", (i, fake.company(), random.choice(['F&B', 'Retail', 'Transport', 'Digital']), datetime.now()))

    # 3. Promos
    for i in range(1, 11):
        cursor.execute("INSERT IGNORE INTO promotions VALUES (%s, %s, %s, %s, %s)", (i, f"PROMO_{fake.word().upper()}_{i}", random.randint(5, 30), 100000.0, '2028-12-31'))

    cursor.execute("SELECT merchant_id FROM merchants"); all_m = [r['merchant_id'] for r in cursor.fetchall()]
    cursor.execute("SELECT * FROM promotions"); all_p = cursor.fetchall()

    # 4. Transactions
    for _ in range(n_transactions):
        if not user_pool or not all_m: break
        u = random.choice(user_pool)
        m_id = random.choice(all_m)
        tx_id = str(uuid.uuid4())
        tx_date = datetime.now()
        
        topup_amt = float(random.randint(500, 5000) * 1000)
        cursor.execute("UPDATE wallets SET balance = balance + %s WHERE user_id = %s", (topup_amt, u['user_id']))
        cursor.execute("INSERT INTO topups VALUES (%s, %s, %s, %s, %s)", (str(uuid.uuid4()), u['user_id'], topup_amt, random.choice(['OVO', 'GOPAY', 'BCA']), tx_date))
        cursor.execute("INSERT INTO wallet_histories (user_id, amount, type, description, created_at) VALUES (%s, %s, 'CREDIT', 'Topup', %s)", (u['user_id'], topup_amt, tx_date))
        
        base_amt = float(random.randint(20, 500) * 1000)
        p = random.choice(all_p) if random.random() > 0.4 else None
        discount = min((base_amt * p['discount_pct'] / 100.0), float(p['max_discount'])) if p else 0.0
        final_amt = base_amt - discount

        cursor.execute("INSERT INTO transactions VALUES (%s, %s, %s, %s, 'SUCCESS', %s)", (tx_id, u['user_id'], m_id, final_amt, tx_date))
        cursor.execute("UPDATE wallets SET balance = balance - %s WHERE user_id = %s", (final_amt, u['user_id']))
        cursor.execute("INSERT INTO wallet_histories (user_id, amount, type, description, created_at) VALUES (%s, %s, 'DEBIT', 'Payment', %s)", (u['user_id'], final_amt, tx_date))
        cursor.execute("INSERT INTO transaction_items (tx_id, item_name, price, qty) VALUES (%s, %s, %s, 1)", (tx_id, fake.bs().title(), final_amt))
        if p:
            cursor.execute("INSERT INTO transaction_vouchers (tx_id, promo_id, discount_amount) VALUES (%s, %s, %s)", (tx_id, p['promo_id'], discount))

def run_ingestion(n_users, n_merchants, n_transactions, reset_db, is_historical, live_config=None):
    conn = None
    try:
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='fintech_db')
        cursor = conn.cursor(dictionary=True)
        setup_database(cursor, reset=reset_db)
        conn.commit()

        if live_config is None:
            inject_batch(cursor, n_users, n_merchants, n_transactions, is_historical)
            conn.commit()
            print(f"{Fore.GREEN}✅ SUCCESS: Batch generation finished.")
        else:
            interval = live_config['interval']
            print(f"\n{Fore.MAGENTA}{Style.BRIGHT}📡 LIVE STREAMING MODE")
            print(f"{Fore.CYAN}Interval: {interval} detik | Press Ctrl+C to stop.\n")
            
            while True:
                u_rand = random.randint(live_config['min_u'], live_config['max_u'])
                t_rand = random.randint(live_config['min_t'], live_config['max_t'])
                
                now = datetime.now().strftime("%H:%M:%S")
                print(f"[{now}] 📥 Injecting {u_rand} users & {t_rand} tx...", end="\r")
                
                inject_batch(cursor, u_rand, 0, t_rand, is_historical=False)
                conn.commit()
                time.sleep(interval)
                
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}🛑 Live streaming stopped.")
    except Error as e:
        print(f"\n{Fore.RED}❌ ERROR: {e}")
    finally:
        if conn and conn.is_connected(): conn.close()

if __name__ == "__main__":
    print_header()
    reset_db = questionary.confirm("Reset Database?").ask()
    
    mode = questionary.select(
        "Pilih Mode:",
        choices=["Batch (Sekali jalan)", "Live Streaming (Terus-menerus)"]
    ).ask()

    if "Batch" in mode:
        vol = questionary.select("Volume:", choices=["Small", "Medium", "Large"]).ask()
        u, m, t = (10, 2, 50) if vol=="Small" else (100, 5, 500) if vol=="Medium" else (500, 10, 2000)
        run_ingestion(u, m, t, reset_db, is_historical=False)
    else:
        # CUSTOM LIVE CONFIG
        interval = int(questionary.text("Interval pengiriman (detik):", default="10").ask())
        min_u = int(questionary.text("Min users per interval:", default="1").ask())
        max_u = int(questionary.text("Max users per interval:", default="3").ask())
        min_t = int(questionary.text("Min transactions per interval:", default="5").ask())
        max_t = int(questionary.text("Max transactions per interval:", default="15").ask())
        
        live_cfg = {'interval': interval, 'min_u': min_u, 'max_u': max_u, 'min_t': min_t, 'max_t': max_t}
        run_ingestion(0, 0, 0, reset_db, False, live_config=live_cfg)
