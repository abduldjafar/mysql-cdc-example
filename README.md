# High-Performance MySQL CDC (Change Data Capture) Stack

This repository provides a comprehensive and highly optimized Docker Compose setup for testing and running a powerful **Change Data Capture (CDC)** environment on **MySQL 8.0**. It is bundled with two custom-built, extreme-performance Rust applications to generate data and replicate binlog events at massive scale (>100K TPS).

## 🚀 Repository Components

This stack consists of three main components:

1. **MySQL 8.0 CDC Container**: Pre-configured with optimal `my.cnf` settings for ROW-based binary logging, ready for external CDC connectors (e.g., Debezium, Flink CDC, Airbyte) or immediate tap replication.
2. **`fintech-data-generator-rs`**: A heavily concurrent, asynchronous Rust application that generates millions of realistic dummy fintech transactions (Users, Wallets, Topups, Transactions) in seconds using `sqlx::QueryBuilder` bulk inserts and Tokio Tasks.
3. **`binlog-tap`**: A blazingly fast Rust CDC consumer that connects to the MySQL binlog replication stream, parses events in real-time, and batches them efficiently into memory using zero-copy principles. It acts as an integration layer to route database mutations extremely fast to downstream data warehouses (like ClickHouse) or data lakes.

---

## 🛠️ Prerequisites & Installation

### Requirements
* Docker and Docker Compose
* Rust & Cargo (If you wish to run the generators outside of Docker)

### Running the Stack
Ensure you are in the `infra/` directory containing the `docker-compose.yml` file, then start the services:

```bash
cd infra
docker-compose up -d
```
*Depending on your `docker-compose.yml`, this will spin up MySQL and the `binlog-tap` service.*

---

## ⚡ Generating Data (`fintech-data-generator-rs`)

To flood the database with millions of rows for high-throughput architectural testing, use the included Rust data generator.

1. Navigate to the generator directory:
```bash
cd fintech-data-generator-rs
```
2. Run the interactive CLI (Wait for compilation if running the first time):
```bash
cargo run --release
```
*(Optionally provide `DATABASE_URL` as an environment variable if not connecting to your local Docker container)*

3. Follow the CLI prompts to select "Batch" or "Live Streaming", set your desired scale (Small/Medium/Large), and watch it insert upwards of hundreds of thousands of records per second.

---

## ⚙️ MySQL Configuration (`my.cnf`) Details

To support precise CDC tracking, this environment forces the following flags in MySQL:

| Parameter | Value | Description |
| :--- | :--- | :--- |
| `server-id` | `1` | Unique server identity in the replication topology. |
| `binlog_format` | `ROW` | Logs exact row changes rather than raw SQL statements (Mandatory for CDC). |
| `binlog_row_image` | `FULL` | Ensures both old and new data states are preserved in the log. |
| `expire_logs_days` | `7` | Auto-purges logs after 7 days to preserve disk space. |

### Verifying CDC Status

If you want to manually verify the binlog configuration inside the MySQL container:

```bash
docker exec -it mysql-cdc mysql -u root -prootpassword -e "SELECT @@log_bin, @@binlog_format;"
```
*Both `log_bin` (ON) and `binlog_format` (ROW) must be set correctly.*

---

## 🏗️ Downstream Integrations (Roadmap)

Data captured from this high-speed MySQL pipeline is primed to flow into modern analytical stores:

1. **Apache Iceberg / ClickHouse**: Utilizing the bundled `binlog-tap` to parse ROW events and flush them into OLAP databases in real-time.
2. **Kafka**: Standard Debezium Connector routing.
3. **Machine Learning Pipelines**: Continuous, sub-second latency data feeds for realtime transaction ML models.

---

**Author:** Abdul Djafar  
**Project:** Data Engineering Pipeline
