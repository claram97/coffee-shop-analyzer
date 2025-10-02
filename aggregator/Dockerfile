FROM python:3.9.7-slim

# Copy requirements.txt if you have one
# COPY aggregator/requirements.txt /tmp/requirements.txt
# RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Install required dependencies
RUN pip install pika

# Copy all code to root for easier imports
COPY aggregator /aggregator
COPY middleware /middleware
COPY protocol /protocol
COPY app_config /app_config

# Set Python path to include root directory
ENV PYTHONPATH=/

# Set working directory
WORKDIR /aggregator

# Make the entry point executable
RUN chmod +x main.py

ENTRYPOINT ["python3", "/aggregator/main.py"]