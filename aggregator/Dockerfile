FROM python:3.9.7-slim

# Install required dependencies (including protobuf runtime required by generated pb2)
RUN pip install --no-cache-dir protobuf==6.32.1 pika

# Copy all code to root for easier imports
COPY aggregator /aggregator
COPY middleware /middleware
COPY protocol /protocol
COPY protocol2 /protocol2
COPY app_config /app_config
COPY leader_election /leader_election

# Set Python path to include root directory
ENV PYTHONPATH=/:/protocol2

# Set working directory
WORKDIR /aggregator

# Make the entry point executable
RUN chmod +x main.py

ENTRYPOINT ["python3", "main.py"]