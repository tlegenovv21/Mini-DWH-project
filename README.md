# Mini-DWH Project

This project implements a local Mini-Data Warehouse using PostgreSQL, ClickHouse, Airflow, and Superset, orchestrated via Docker Compose.

## Prerequisites

- Docker Desktop installed and running.
- Python 3.10+ (for local script testing if needed).
- DBeaver (for potential database inspection).

## Quick Start

1.  **Configure Environment**:
    Copy `.env.example` to `.env` (already done if you followed the setup script).
    ```bash
    cp .env.example .env
    ```
    Ensure ports 5432, 8123, 9000, 8080, 8088 are free.

2.  **Start Services**:
    ```bash
    docker compose up -d
    ```
    This will start:
    - PostgreSQL (Source OLTP)
    - ClickHouse (DWH)
    - Airflow (Orchestrator)
    - Superset (BI)

3.  **Access Interfaces**:
    - **Airflow**: [http://localhost:8080](http://localhost:8080) (user: `airflow`, pass: `airflow`)
    - **Superset**: [http://localhost:8088](http://localhost:8088) (user: `admin`, pass: `admin`)
    - **ClickHouse**: [http://localhost:8123](http://localhost:8123)
    - **PostgreSQL**: `localhost:5432`

## Project Structure

- `sql/`: PostgreSQL schema and initialization scripts.
- `seed/`: Python script to generate synthetic data.
- `airflow/`: DAGs and requirements.
- `dbt/`: dbt project for data transformation.
- `superset/`: Dashboard exports and setup.

## Workflow

1.  **Generate Data**: The `sql/pg_schema.sql` creates tables on startup. Run `python seed/generator.py` to populate them.
2.  **Load Data**: Enable `load_raw_pg_to_ch` DAG in Airflow to move data to ClickHouse.
3.  **Transform**: dbt is triggered by Airflow to create marts.

## Troubleshooting

-   **Docker Connection**: If you see "error during connect", ensure Docker Desktop is running.
-   **ClickHouse Healthcheck**: The default `curl` command might fail if not installed in the container. We use `wget` in `docker-compose.yml`.
-   **Airflow/Superset Keys**: Ensure `AIRFLOW__CORE__FERNET_KEY` and `SUPERSET_SECRET_KEY` are set in `.env`.
-   **Airflow DB**: LocalExecutor requires a persistent DB connection (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`).
