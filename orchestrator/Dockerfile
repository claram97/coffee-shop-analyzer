FROM python:3.9.7-slim

# Install required dependencies
RUN pip install pika

# Copy orchestrator code
COPY orchestrator /

# Copy middleware code to make it available for imports
COPY middleware /middleware

ENTRYPOINT ["/bin/sh"]