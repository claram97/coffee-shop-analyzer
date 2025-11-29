FROM python:3.9.7-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# Install required dependencies (including protobuf runtime required by generated pb2)
RUN pip install --no-cache-dir protobuf==6.33.0 pika

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
