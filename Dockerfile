# Python + slim image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Install deps first (better cache)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Default dirs (also used by volumes)
RUN mkdir -p /app/data /app/logs /app/mappings

# Entrypoint is provided via docker-compose command
CMD ["bash", "-lc", "python -m app.extract --help"]
