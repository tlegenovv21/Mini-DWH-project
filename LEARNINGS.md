# What I Learned: Mini Data Warehouse Project

## Project Overview
Built a complete Mini Data Warehouse (DWH) using Docker, demonstrating end-to-end data engineering skills from data ingestion to visualization.

## Technical Stack
- **PostgreSQL**: Source OLTP database
- **ClickHouse**: Analytical data warehouse (columnar storage)
- **Apache Airflow**: Workflow orchestration and ETL
- **dbt**: Data transformation and modeling
- **Apache Superset**: Business intelligence and visualization
- **Docker**: Containerization and infrastructure

---

## Key Skills Demonstrated

### 1. Docker & Infrastructure Management
**What I Did:**
- Configured multi-container application with `docker-compose.yml`
- Set up service dependencies and health checks
- Managed environment variables and secrets via `.env` file
- Created custom Docker images (Superset with ClickHouse drivers)

**Why It Matters:**
Modern data engineering relies heavily on containerization for:
- Reproducible environments
- Easy deployment across different systems
- Isolation of dependencies
- Scalability

**Technical Challenge Solved:**
- **Problem**: Superset couldn't connect to ClickHouse
- **Root Cause**: Missing Python drivers in virtual environment
- **Solution**: Created custom Dockerfile using `--target` flag to install packages in correct location
- **Learning**: Understanding Python virtual environments and Docker layer caching

### 2. Database Connectivity & Authentication
**What I Did:**
- Configured PostgreSQL with proper user credentials
- Set up ClickHouse with HTTP and native protocols
- Resolved Airflow connection authentication issues
- Used environment variables for credential management

**Why It Matters:**
Data engineers must:
- Securely manage database credentials
- Understand different database protocols (HTTP vs native)
- Configure connection strings correctly
- Debug authentication failures

**Technical Challenge Solved:**
- **Problem**: Airflow DAG failing with "password authentication failed"
- **Root Cause**: Default `postgres_default` connection had wrong credentials
- **Solution**: Override via `AIRFLOW_CONN_POSTGRES_DEFAULT` environment variable
- **Learning**: Airflow connection management and environment variable patterns

### 3. ETL Pipeline Development
**What I Did:**
- Analyzed and understood existing Airflow DAG (`load_raw_pg_to_ch.py`)
- Implemented full load strategy for dimension tables
- Implemented incremental load for fact tables
- Used pandas for data chunking and memory management

**Why It Matters:**
ETL is core to data engineering:
- Moving data between systems efficiently
- Handling large datasets with chunking
- Implementing different loading strategies (full vs incremental)
- Managing data quality checks

**Key Concepts:**
- **Full Load**: Complete refresh of dimension tables (customers, products)
- **Incremental Load**: Only new/changed records for fact tables (orders)
- **Temp Tables**: Atomic swaps to prevent data inconsistency
- **Watermarking**: Tracking last loaded timestamp for incremental loads

### 4. Data Warehouse Design
**What I Did:**
- Understood raw layer design (staging area)
- Prepared for data mart layer (analytics-ready tables)
- Worked with star schema concepts (facts and dimensions)

**Why It Matters:**
Data warehouses use layered architecture:
- **Raw Layer**: Exact copy from source (minimal transformation)
- **Staging Layer**: Cleaned and standardized
- **Data Mart Layer**: Business-focused aggregations

**Tables Created:**
- `raw_customers` (1,000 records)
- `raw_products` (50 records)
- `raw_orders` (20,000 records)
- `raw_order_items` (109,575 records)

### 5. BI Tool Integration
**What I Did:**
- Connected Superset to ClickHouse
- Created datasets from raw tables
- Built visualizations and charts
- Understood SQLAlchemy connection strings

**Why It Matters:**
Data engineers must:
- Enable business users to access data
- Configure BI tools correctly
- Understand different database dialects
- Troubleshoot connectivity issues

**Connection String Format:**
```
clickhousedb://default@clickhouse:8123/default
```
- Protocol: `clickhousedb://` (HTTP-based)
- User: `default` (no password)
- Host: `clickhouse` (Docker service name)
- Port: `8123` (HTTP interface)
- Database: `default`

### 6. Troubleshooting & Debugging
**What I Did:**
- Read and analyzed Docker logs
- Debugged Python import errors
- Traced authentication failures
- Verified data loading success

**Skills Developed:**
- Reading stack traces and error messages
- Using Docker exec to inspect containers
- Testing connections manually
- Verifying data with SQL queries

**Debugging Commands Used:**
```bash
# Check container logs
docker compose logs airflow --tail=100

# Test inside container
docker compose exec airflow airflow tasks test <dag> <task> <date>

# Verify data
docker compose exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM raw_customers"

# Check installed packages
docker compose exec airflow pip list | grep clickhouse
```

---

## Data Engineering Concepts Learned

### 1. **ELT vs ETL**
This project uses **ELT** (Extract, Load, Transform):
- Extract from PostgreSQL
- Load to ClickHouse (raw layer)
- Transform using dbt (data marts)

**Advantage**: Leverage ClickHouse's processing power for transformations

### 2. **Columnar Databases**
ClickHouse is columnar (vs PostgreSQL's row-based):
- **Better for analytics**: Aggregate queries on specific columns
- **Compression**: Similar values in columns compress well
- **Fast reads**: Only read needed columns

### 3. **Workflow Orchestration**
Airflow manages:
- Task dependencies (customers before orders)
- Scheduling (daily runs)
- Retries and error handling
- Data quality checks

### 4. **Infrastructure as Code**
Everything defined in code:
- `docker-compose.yml`: Infrastructure
- `.env`: Configuration
- DAGs: Workflows
- SQL: Schema definitions

**Benefit**: Version control, reproducibility, collaboration

---

## Real-World Applications

### As a Junior Data Engineer, This Project Shows:

1. **End-to-End Pipeline**: From source database to visualization
2. **Modern Tools**: Industry-standard stack (Airflow, dbt, ClickHouse)
3. **Problem-Solving**: Debugged real issues with drivers and connections
4. **Best Practices**: 
   - Environment variables for secrets
   - Docker for reproducibility
   - Layered data architecture
   - Data quality checks

### Interview Talking Points:

**"Tell me about a data pipeline you built"**
- Designed ETL pipeline moving 130K+ records from PostgreSQL to ClickHouse
- Used Airflow for orchestration with full and incremental load strategies
- Implemented atomic table swaps for zero-downtime updates
- Built BI dashboards in Superset for business users

**"How do you handle errors in data pipelines?"**
- Configured Airflow retries and error handling
- Implemented data quality checks (null email validation)
- Used temp tables to prevent partial loads
- Monitored via Airflow UI and logs

**"Describe your experience with Docker"**
- Built multi-container data platform with 5 services
- Created custom images with specific dependencies
- Managed service dependencies and health checks
- Debugged container networking and volume issues

---

## Next Steps for Growth

1. **Add dbt Models**: Create data marts with business logic
2. **Implement CI/CD**: Automate testing and deployment
3. **Add Monitoring**: Set up alerts for pipeline failures
4. **Optimize Performance**: Partition tables, add indexes
5. **Add Data Quality**: More comprehensive validation rules
6. **Documentation**: Add data dictionary and lineage

---

## Technical Metrics

- **Data Volume**: 130,625 total records processed
- **Services**: 5 containerized applications
- **Languages**: Python, SQL, YAML
- **Databases**: 2 (PostgreSQL, ClickHouse)
- **Pipeline Tasks**: 4 parallel loads + quality checks + dbt transformations
