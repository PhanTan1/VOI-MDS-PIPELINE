FROM apache/airflow:3.1.8

# Install Cosmos and dbt directly into the image
RUN pip install --no-cache-dir \
    astronomer-cosmos \
    dbt-postgres