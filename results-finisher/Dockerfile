# results-finisher/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Add the working directory to the PYTHONPATH so imports work correctly
ENV PYTHONPATH=/app

# Copy requirements first for better Docker layer caching
COPY results-finisher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the protocol folder from repository root
COPY protocol ./protocol

# Copy middleware implementation first
COPY middleware ./middleware

# Copy all the Python scripts and other files from results-finisher
COPY results-finisher .

# Use the improved middleware client implementation
# Force overwrite of any existing middleware_client.py in results-finisher
RUN cp -f middleware/middleware_client.py .
# Ensure middleware directory is a proper package
RUN touch middleware/__init__.py || true
RUN echo "Using improved middleware implementation from middleware directory"

# Create an __init__.py file to make sure the protocol can be imported properly
RUN touch protocol/__init__.py || true

# Set the entry point for the container
CMD ["python", "results_finisher_runner.py"]