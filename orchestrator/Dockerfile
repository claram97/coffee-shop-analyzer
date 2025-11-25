FROM python:3.9.7-slim

# Upgrade pip and install runtime dependencies required by orchestrator
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
	pip install --no-cache-dir pika protobuf==6.33.0

# Copy orchestrator code
COPY orchestrator /

# Copy middleware code to make it available for imports
COPY middleware /middleware

# Copy protocol code to make it available for imports
COPY protocol /protocol

COPY protocol2 /protocol2

# Ensure protocol2 modules are on Python path so generated pb2 imports resolve
ENV PYTHONPATH=/:/protocol2

ENTRYPOINT ["/bin/sh"]