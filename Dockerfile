FROM apache/airflow:2.8.1

USER root

# Install system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /requirements.txt

# Change ownership of the requirements file to airflow user
RUN chown airflow: /requirements.txt

# Switch to airflow user and install Python packages
USER airflow
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Keep airflow as the final user
USER airflow

