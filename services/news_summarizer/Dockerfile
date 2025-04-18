# services/news_summarizer/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    WORKDIR=/app \
    PORT=8080

# Copy the requirements file first
COPY requirements.txt .

# Install Python dependencies
# Ensure requirements.txt includes Flask, gunicorn, uvicorn, google-generativeai[vertexai] etc.
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy the application source code from src/
COPY src/ .

# Expose the port the app runs on
EXPOSE 8080

# Command to run the application using Gunicorn and the Uvicorn worker
# Assumes the Flask app instance is named 'app' in src/main.py
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]