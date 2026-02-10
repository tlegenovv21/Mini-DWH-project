# Superset Setup & Dashboards

## Prerequisites

Before setting up Superset dashboards, you need to have data in ClickHouse. Follow these steps first:

### 1. Generate Sample Data
Run the data generator to populate PostgreSQL:
```bash
python seed/generator.py
```

### 2. Load Data to ClickHouse
1. Go to Airflow at [http://localhost:8080](http://localhost:8080)
2. Login with username: `airflow`, password: `airflow`
3. Enable and trigger the `load_raw_pg_to_ch` DAG
4. Wait for it to complete successfully

### 3. Run dbt Transformations
The dbt transformations should run automatically via Airflow to create data mart tables (`dm_sales_daily`, `dm_sales_by_category`, `dm_customers_orders`).

If the data marts don't exist yet, you'll only see raw tables (`raw_orders`, `raw_customers`, `raw_products`, `raw_order_items`).

---

## Superset Setup

### 1. Login
Access Superset at [http://localhost:8088](http://localhost:8088).
- **Username**: `admin`
- **Password**: `admin`

### 2. Connect to ClickHouse
1. Go to **Settings** (top right) → **Database Connections**
2. Click **+ Database**
3. Fill in:
   - **Display Name**: `ClickHouse`
   - **SQLAlchemy URI**: `clickhousedb://default@clickhouse:8123/default`
4. Click **Test Connection** (should succeed)
5. Click **Connect**

### 3. Create Datasets

#### Option A: Using Raw Tables (Available Now)
1. Go to **Datasets** → **+ Dataset**
2. Select Database: `ClickHouse`
3. Schema: `default`
4. Select Table: `raw_orders`
5. Repeat for other raw tables:
   - `raw_customers`
   - `raw_products`
   - `raw_order_items`

#### Option B: Using Data Marts (After dbt Transformation)
Once dbt transformations are complete, create datasets for:
- `dm_sales_daily`
- `dm_sales_by_category`
- `dm_customers_orders`

### 4. Create Simple Dashboard (Using Raw Tables)

**Example: Orders Overview**
1. Go to **Charts** → **+ Chart**
2. Choose Dataset: `raw_orders`
3. Chart Type: **Table**
4. Add columns: `order_id`, `customer_id`, `order_ts`, `status`
5. Save the chart

**Example: Order Count by Status**
1. Chart Type: **Pie Chart**
2. Dataset: `raw_orders`
3. Dimension: `status`
4. Metric: `COUNT(*)`

### 5. Create Advanced Dashboard (After dbt Transformation)

Once data marts are available:

**Revenue Trend**:
- Chart Type: Line Chart
- Dataset: `dm_sales_daily`
- Metric: `Sum(total_revenue)`
- Time Grain: `Day`

**Top Categories**:
- Chart Type: Bar Chart
- Dataset: `dm_sales_by_category`
- Metric: `Sum(total_revenue)`
- Group By: `category`

**Customer Insights**:
- Chart Type: Table
- Dataset: `dm_customers_orders`
- Columns: `full_name`, `total_orders`, `total_spend`
- Order By: `total_spend` desc

### 6. Export Dashboard
Once created:
1. Go to **Dashboards**
2. Select your dashboard
3. Click **Actions** (...) → **Export**
4. Save the downloaded JSON/ZIP file to `superset/import_export/`
