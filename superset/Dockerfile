FROM apache/superset:latest
USER root
RUN pip install --target=/app/.venv/lib/python3.10/site-packages clickhouse-connect clickhouse-sqlalchemy
USER superset
