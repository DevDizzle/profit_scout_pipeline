# services/ratio_calculator/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    WORKDIR=/app

# Copy the requirements file first
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy the application source code
COPY src/ratio_calculator.py .
# If ratio_calculator.py imports other custom modules from src/, copy them too:
# COPY src/helpers.py .

# Command to run the ratio calculator script directly (for batch job)
CMD ["python", "ratio_calculator.py"]