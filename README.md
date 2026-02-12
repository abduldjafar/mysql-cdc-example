Berikut adalah file `README.md` yang lengkap, profesional, dan siap pakai untuk proyek MySQL CDC Anda. File ini disusun agar mudah dipahami oleh developer lain atau untuk dokumentasi pribadi Anda.

```markdown
# MySQL CDC (Change Data Capture) Setup

Repositori ini berisi konfigurasi Docker untuk menjalankan instance **MySQL 8.0** yang sudah dioptimalkan untuk proses **Change Data Capture (CDC)**. Setup ini kompatibel dengan alat-alat data modern seperti Debezium, Flink CDC, Airbyte, atau integrasi langsung ke **Apache Iceberg**.

## 🚀 Fitur Utama
* **Binary Logging Aktif**: Menggunakan format `ROW` untuk tracking perubahan data yang presisi.
* **Persistent Data**: Menggunakan Docker volumes agar data tidak hilang saat container dihapus.
* **Security Ready**: Dilengkapi dengan script pembuatan user khusus CDC dengan permission minimal.

---

## 🛠️ Persiapan & Instalasi

### 1. Prasyarat
* Docker dan Docker Compose sudah terinstal di mesin Anda.

### 2. Struktur Folder
Pastikan file berikut berada dalam satu direktori:
* `docker-compose.yml`
* `my.cnf` (File konfigurasi MySQL)

### 3. Menjalankan MySQL
Gunakan perintah berikut untuk menyalakan container:
```bash
docker-compose up -d

```

---

## ⚙️ Konfigurasi Teknis

### Parameter Binlog (`my.cnf`)

Untuk mendukung CDC, parameter berikut telah diaktifkan di dalam `my.cnf`:
| Parameter | Nilai | Deskripsi |
| :--- | :--- | :--- |
| `server-id` | `1` | Identitas unik server dalam topologi replikasi. |
| `binlog_format` | `ROW` | Mencatat perubahan data per baris (Wajib untuk CDC). |
| `binlog_row_image` | `FULL` | Menyimpan data lama dan baru dalam log. |
| `expire_logs_days` | `7` | Menghapus log otomatis setelah 7 hari untuk menghemat storage. |

---

## 🔍 Verifikasi Status CDC

Setelah container berjalan, pastikan konfigurasi sudah aktif dengan menjalankan perintah berikut:

1. **Masuk ke MySQL CLI:**
```bash
docker exec -it mysql-cdc mysql -u root -prootpassword

```


2. **Jalankan Query Cek Status:**
```sql
-- Cek apakah binlog aktif (Harus muncul "ON")
SELECT @@log_bin;

-- Cek format log (Harus muncul "ROW")
SELECT @@binlog_format;

```



---

## 👤 Setup User CDC

Jangan gunakan user `root` untuk aplikasi CDC eksternal. Gunakan user khusus dengan akses terbatas:

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

```

---

## 🏗️ Integrasi Berikutnya (Roadmap)

Data dari MySQL ini siap dialirkan menuju:

1. **Apache Iceberg**: Untuk Modern Data Lakehouse menggunakan Flink CDC.
2. **Kafka**: Menggunakan Debezium Connector.
3. **Analytics**: Sinkronisasi real-time ke warehouse atau data lake.

---

**Author:** Abdul Djafar

**Project:** Data Engineering Pipeline

```


