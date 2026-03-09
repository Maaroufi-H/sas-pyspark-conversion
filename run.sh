#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# run.sh — Avvio SAS → PySpark Converter senza docker-compose
# ═══════════════════════════════════════════════════════════════
#
# Uso:
#   chmod +x run.sh
#   ./run.sh            # avvia il container
#   ./run.sh stop       # ferma e rimuove il container
#   ./run.sh rebuild    # rebuild completo e riavvio
#   ./run.sh logs       # mostra i log in tempo reale
#   ./run.sh status     # stato del container
#
# Configurazione:
#   OLLAMA_MODEL=deepseek-coder:6.7b ./run.sh   # cambia modello

set -e

# ── Configurazione ──────────────────────────────────────────────
CONTAINER_NAME="sas-pyspark-web"
IMAGE_NAME="sas-converter:latest"
HOST_PORT="${PORT:-80}"
OLLAMA_MODEL="${OLLAMA_MODEL:-codestral}"

# Crea le directory necessarie se non esistono
mkdir -p "$(pwd)/output"
mkdir -p "$(pwd)/INPUT"

# ── Funzioni ────────────────────────────────────────────────────

build() {
    echo "[run.sh] Build immagine Docker..."
    docker build -t "$IMAGE_NAME" .
    echo "[run.sh] Build completata: $IMAGE_NAME"
}

start() {
    # Rimuovi il container precedente se esiste
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "[run.sh] Rimozione container precedente..."
        docker rm -f "$CONTAINER_NAME"
    fi

    echo "[run.sh] Avvio container sulla porta ${HOST_PORT}..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        --restart unless-stopped \
        --add-host host.docker.internal:host-gateway \
        -p "${HOST_PORT}:5000" \
        -e FLASK_ENV=production \
        -e PORT=5000 \
        -e OLLAMA_HOST=http://host.docker.internal:11434 \
        -e OLLAMA_MODEL="$OLLAMA_MODEL" \
        -v "$(pwd)/output:/app/output" \
        -v "$(pwd)/INPUT:/app/INPUT:ro" \
        "$IMAGE_NAME"

    echo "[run.sh] Container avviato: $CONTAINER_NAME"
    echo "[run.sh] Applicazione disponibile su: http://localhost:${HOST_PORT}/"
    echo "[run.sh] Health check: http://localhost:${HOST_PORT}/health"
}

stop() {
    echo "[run.sh] Arresto container $CONTAINER_NAME..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || echo "[run.sh] Container non in esecuzione."
    echo "[run.sh] Fatto."
}

logs() {
    docker logs -f "$CONTAINER_NAME"
}

status() {
    echo "[run.sh] Stato container:"
    docker ps -a --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""
    echo "[run.sh] Health check:"
    curl -s "http://localhost:${HOST_PORT}/health" 2>/dev/null || echo "(non raggiungibile)"
}

# ── Dispatch comandi ─────────────────────────────────────────────

case "${1:-start}" in
    start)
        # Controlla se l'immagine esiste già
        if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
            build
        fi
        start
        ;;
    rebuild)
        build
        start
        ;;
    stop)
        stop
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    build)
        build
        ;;
    *)
        echo "Uso: $0 {start|stop|rebuild|logs|status|build}"
        echo ""
        echo "  start    — avvia il container (build se necessario)"
        echo "  rebuild  — forza rebuild e riavvia"
        echo "  stop     — ferma e rimuove il container"
        echo "  logs     — mostra i log in tempo reale"
        echo "  status   — stato e health check"
        echo "  build    — solo build immagine"
        exit 1
        ;;
esac
