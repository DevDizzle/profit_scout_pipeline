# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    WORKDIR=/app \
    # Set timezone info to potentially help tz-aware libraries like pandas/yfinance
    TZ=Etc/UTC \
    DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR ${WORKDIR}

# Install system dependencies (keep if yfinance or dependencies need compilation)
# Hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    # build-essential contains gcc, python3-dev, libc-dev etc.
    build-essential \
    # Add tzdata for timezone support if needed by pandas/yfinance
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
# Removed --require-hashes unless you specifically generate hashes
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
# Assumes your code is in ./src relative to the Dockerfile
COPY src/ .

# Specify the command to run on container start
# Renamed to main.py for consistency with other jobs
CMD ["python", "main.py"]