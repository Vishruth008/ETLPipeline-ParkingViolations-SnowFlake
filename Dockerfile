FROM apache/airflow:2.7.3-python3.10

# Copy your requirements.txt file to the container
COPY requirements.txt ./

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt