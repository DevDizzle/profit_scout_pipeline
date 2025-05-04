# Dockerfile
FROM python:3.11

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libstdc++6 \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install with verbose output
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -v -r requirements.txt && \
    pip list

# Copy application code
COPY src/ .

# Command to run
CMD ["python", "main.py"]