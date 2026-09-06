FROM python:3.12-slim

# ffmpeg: bazı formatların birleştirilmesi için (güvenli taraf)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY plworker.py .
COPY templates ./templates

# Playlist işlerinin geçici çıktısı burada tutulur (compose'da bind mount).
ENV DOWNLOAD_DIR=/downloads
RUN mkdir -p /downloads

EXPOSE 8000

# Geliştirme sunucusu değil, üretim için gunicorn.
# gthread: senkron worker timeout sayacını yalnız istekler arasında sıfırlar,
# bu yüzden 180 sn'yi aşan büyük dosya aktarımları worker'ı öldürüyordu.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", \
     "--worker-class", "gthread", "--workers", "2", "--threads", "4", \
     "--timeout", "600", "--graceful-timeout", "30", \
     "app:app"]
