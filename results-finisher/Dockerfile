# results-finisher/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Add the working directory to the PYTHONPATH so imports work correctly
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the Python scripts and other files
COPY . .