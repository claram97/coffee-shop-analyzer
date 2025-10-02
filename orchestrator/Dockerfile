FROM python:3.9.7-slim

RUN pip install pika

# Copy orchestrator code
COPY orchestrator /

# Copy middleware code to make it available for imports
COPY middleware /middleware

# Copy protocol code to make it available for imports
COPY protocol /protocol

ENTRYPOINT ["/bin/sh"]