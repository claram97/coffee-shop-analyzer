FROM python:3.9.7-slim

# Install runtime dependencies for the heartbeat monitor
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir pika protobuf==6.32.1

# Copy application code and shared protocol definitions
COPY heartbeat_monitor /heartbeat_monitor
COPY protocol2 /protocol2

# Ensure generated protobuf modules resolve correctly
ENV PYTHONPATH=/:/protocol2

# Run monitor from its module directory
WORKDIR /heartbeat_monitor

ENTRYPOINT ["python3", "main.py"]
