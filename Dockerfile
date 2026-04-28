FROM python:3.12-slim

# Zależności systemowe Chromium – instalowane ręcznie (--with-deps nie działa na Debianie trixie)
RUN apt-get update && apt-get install -y --no-install-recommends \
        # Core
        ca-certificates wget \
        # Chromium runtime
        libnss3 libnspr4 libdbus-1-3 \
        libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
        libcups2 libdrm2 libxkbcommon0 \
        libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
        libgbm1 libasound2 \
        libx11-6 libxcb1 libxext6 libxshmfence1 \
        # Fonts (tylko te faktycznie dostępne w trixie)
        fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium

COPY app/ ./app/

RUN mkdir -p /data/pdfs /config

ENV CONFIG_PATH=/config/config.yaml \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
