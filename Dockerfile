FROM apache/airflow:3.0.0

# Switch to root to install system dependencies if needed
USER root

# Switch back to airflow user for pip installations
USER airflow

# Install requirements. 
# We include the trusted-host flags here just in case the build 
# also happens behind your corporate proxy.
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    "dbt-postgres" "requests" "psycopg2-binary"