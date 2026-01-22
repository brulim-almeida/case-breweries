# ============================================================================
# Breweries Data Lake - Dockerfile
# Base: Apache Airflow 3.0 with Python 3.11
# ============================================================================

FROM apache/airflow:3.0.0-python3.11

USER root

# ============================================================================
# Install OpenJDK-17 (Required for PySpark) and create directories
# ============================================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/lakehouse/bronze \
    /opt/airflow/lakehouse/silver \
    /opt/airflow/lakehouse/gold \
    /opt/airflow/monitoring \
    /opt/airflow/src \
    /opt/airflow/utils \
    && chown -R airflow:root /opt/airflow/lakehouse \
    && chown -R airflow:root /opt/airflow/monitoring \
    && chown -R airflow:root /opt/airflow/src \
    && chown -R airflow:root /opt/airflow/utils

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

USER airflow

# Remove problematic provider that causes initialization errors
RUN pip uninstall -y apache-airflow-providers-common-messaging || true

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install only essential packages (PySpark, Delta, requests, pandas)
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    requests==2.31.0 \
    pandas==2.1.4 \
    pyarrow==14.0.2

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src:/opt/airflow"

# Verify installations
RUN python --version && \
    java -version && \
    pip show pyspark delta-spark
