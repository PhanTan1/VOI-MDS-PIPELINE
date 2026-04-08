FROM apache/airflow:3.1.8

# Install Cosmos and dbt directly into the image
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    astronomer-cosmos \
    dbt-postgres