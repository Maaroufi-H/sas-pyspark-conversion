# ═══════════════════════════════════════════════════════════════
# Dockerfile – SAS → PySpark Converter Web App
# ═══════════════════════════════════════════════════════════════
# Build:  docker build -t sas-converter .
# Run:    docker run -p 5000:5000 sas-converter
# Compose:docker-compose up -d
# ═══════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Metadati
LABEL maintainer="sas-pyspark-conversion"
LABEL description="Convertitore SAS → PySpark con interfaccia web Flask"
LABEL version="1.0"

# Variabili d'ambiente
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_ENV=production \
    PORT=5000

WORKDIR /app

# ── Dipendenze di sistema ────────────────────────────────────────
# gcc è necessario per compilare alcune estensioni Python (es. pandas)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# ── Dipendenze Python ────────────────────────────────────────────
# Copiamo prima solo requirements.txt per sfruttare la cache dei layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Codice sorgente ──────────────────────────────────────────────
COPY . .

# Crea cartella output per i file temporanei
RUN mkdir -p output INPUT

# ── Porta esposta ────────────────────────────────────────────────
EXPOSE ${PORT}

# ── Health check ─────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')" || exit 1

# ── Avvio ────────────────────────────────────────────────────────
# Usa il server di sviluppo Flask (sufficiente per uso interno/Azure VM)
# Per produzione ad alto traffico sostituire con:
#   gunicorn -w 4 -b 0.0.0.0:5000 webapp:app
CMD ["python", "webapp.py"]
