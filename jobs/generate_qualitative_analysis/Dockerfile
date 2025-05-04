# jobs/generate_qualitative_analysis/Dockerfile

FROM python:3.9-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ ./src/

# Run your script
CMD ["python", "src/main.py"]
