# syntax=docker/dockerfile:1
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip certifi

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DONATIONS_DB_PATH=/data/donations.db \
    GEOCACHE_DB_PATH=/data/geocache.db

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend backend
COPY frontend frontend

# создаём volume-директории для SQLite
VOLUME ["/data"]

EXPOSE 8000
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
