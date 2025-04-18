# services/price_loader/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    WORKDIR=/app

# Copy the requirements file first
COPY requirements.txt .

# Install Python dependencies
# Note: Ensure yfinance dependencies (like lxml if needed indirectly) install correctly
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy the application source code
COPY src/price_loader.py .

# Command to run the price loader script directly (for batch job)
CMD ["python", "price_loader.py"]