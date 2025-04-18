# services/listener/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    WORKDIR=/app

# Copy the requirements file first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy the application source code
COPY src/listener.py .

# Command to run the listener script directly
CMD ["python", "listener.py"]