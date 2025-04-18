# services/subscriber/Dockerfile

# Use an official Python runtime as a parent image
# Using 3.11-slim as an example; adjust if needed
FROM python:3.11-slim

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    # Set the working directory inside the container
    WORKDIR=/app \
    # Set the port the container listens on (same as Gunicorn bind)
    PORT=8080

# Install any system dependencies required by your Python packages (if any)
# Example: RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy the generated requirements file into the container
# Assumes Dockerfile is in services/subscriber/ and requirements.txt is too
COPY requirements.txt .

# Install Python dependencies using pip
# Use --no-cache-dir to reduce image size
# Use --require-hashes if your requirements.txt includes them
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy the application source code from the 'src' directory into the container's WORKDIR
COPY src/ .

# Expose the port the app runs on
EXPOSE 8080

# Define the command to run the application using Gunicorn and the Uvicorn worker
# 'main:app' points to the Flask app instance named 'app' in your main.py file
# Ensure Flask, gunicorn, and uvicorn are in requirements.txt
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]