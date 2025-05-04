# profit_scout_pipeline/services/price_loader/Dockerfile

# 1) Base image
FROM python:3.11-slim

# 2) Environment
ENV PYTHONUNBUFFERED=1 \
    TZ=UTC

# 3) Working directory
WORKDIR /app

# 4) Install Python deps in one shot
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 5) Copy application code
COPY src/ .

# 6) Default command
CMD ["python", "main.py"]
