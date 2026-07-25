FROM python:3.12-slim

# ffmpeg: bazı formatların birleştirilmesi için (güvenli taraf)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates ./templates

EXPOSE 8000

# Geliştirme sunucusu değil, üretim için gunicorn. timeout: uzun indirmeler için.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "180", "app:app"]
