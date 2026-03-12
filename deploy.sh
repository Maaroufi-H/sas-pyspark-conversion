#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Automazione build + deploy Docker per sas-pyspark-conversion
# Uso:
#   ./deploy.sh           → git pull + kill + build datato + run
#   ./deploy.sh --no-pull → salta git pull
# =============================================================================

set -euo pipefail

CONTAINER_NAME="sas-pyspark-web"
IMAGE_BASE="sas-converter"
APP_PORT="${PORT:-80}"
OLLAMA_HOST="${OLLAMA_HOST:-http://host.docker.internal:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-llama3.1:70b}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
IMAGE_TAG="${IMAGE_BASE}:${TIMESTAMP}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${BLUE}[deploy]${NC} $*"; }
ok()   { echo -e "${GREEN}[ok]${NC}    $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }
err()  { echo -e "${RED}[err]${NC}   $*" >&2; }

NO_PULL=false
for arg in "$@"; do
  [[ "$arg" == "--no-pull" ]] && NO_PULL=true
done

cd "$SCRIPT_DIR"
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  SAS→PySpark Converter — Deploy  ${TIMESTAMP}${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""

# ── STEP 1: Git pull ──────────────────────────────────────────────────────────
if [[ "$NO_PULL" == false ]]; then
  BRANCH=$(git rev-parse --abbrev-ref HEAD)
  log "Git pull — branch: ${BRANCH}"
  git pull origin "$BRANCH" && ok "Pull completato" || warn "Git pull fallito — continuo con codice locale"
  log "Ultimi commit:"; git log --oneline -3 | sed 's/^/         /'
else
  warn "--no-pull: salto il git pull"
fi
echo ""

# ── STEP 2: Verifica Ollama ───────────────────────────────────────────────────
log "Verifico Ollama su localhost:11434..."
if curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; then
  ok "Ollama raggiungibile"
  if ollama list 2>/dev/null | grep -q "${OLLAMA_MODEL}"; then
    ok "Modello ${OLLAMA_MODEL} disponibile"
  else
    warn "Modello ${OLLAMA_MODEL} NON trovato — esegui: ollama pull ${OLLAMA_MODEL}"
  fi
else
  warn "Ollama non raggiungibile — LLM non funzionerà nel container"
fi
echo ""

# ── STEP 3: Directory ─────────────────────────────────────────────────────────
mkdir -p output INPUT logs
ok "Directory output/ INPUT/ logs/ verificate"
echo ""

# ── STEP 4: Kill container esistente ─────────────────────────────────────────
log "Cerco container esistente '${CONTAINER_NAME}'..."
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  docker stop "${CONTAINER_NAME}" > /dev/null 2>&1 || true
  docker rm   "${CONTAINER_NAME}" > /dev/null 2>&1 || true
  ok "Container precedente rimosso"
else
  log "Nessun container precedente trovato"
fi
echo ""

# ── STEP 5: Build immagine con tag datato ─────────────────────────────────────
log "Build immagine: ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" . 2>&1 | while IFS= read -r line; do echo "  $line"; done

if ! docker image inspect "${IMAGE_TAG}" > /dev/null 2>&1; then
  err "Build fallita — immagine ${IMAGE_TAG} non trovata"; exit 1
fi
ok "Build completata: ${IMAGE_TAG}"

docker tag "${IMAGE_TAG}" "${IMAGE_BASE}:latest"
ok "Tag 'latest' aggiornato → ${IMAGE_BASE}:latest"
echo ""

# ── STEP 6: Avvio container ───────────────────────────────────────────────────
log "Avvio container..."
if [[ -f "docker-compose.yml" ]]; then
  docker-compose up -d
  ok "Container avviato via docker-compose"
else
  docker run -d \
    --name "${CONTAINER_NAME}" \
    --restart unless-stopped \
    -p "${APP_PORT}:5000" \
    -v "$(pwd)/output:/app/output" \
    -v "$(pwd)/INPUT:/app/INPUT:ro" \
    -v "$(pwd)/logs:/app/logs" \
    --add-host=host.docker.internal:host-gateway \
    -e FLASK_ENV=production \
    -e PORT=5000 \
    -e OLLAMA_HOST="${OLLAMA_HOST}" \
    -e OLLAMA_MODEL="${OLLAMA_MODEL}" \
    "${IMAGE_BASE}:latest"
  ok "Container avviato via docker run"
fi
echo ""

# ── STEP 7: Health check ──────────────────────────────────────────────────────
log "Attendo health check (max 30s)..."
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${APP_PORT}/health" > /dev/null 2>&1; then
    ok "App online dopo ${i}s"; break
  fi
  [[ $i -eq 30 ]] && warn "Timeout — controlla: docker logs ${CONTAINER_NAME} --tail=20"
  sleep 1
done
echo ""

# ── STEP 8: Riepilogo ─────────────────────────────────────────────────────────
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Deploy completato${NC}"
echo -e "  Immagine:  ${CYAN}${IMAGE_TAG}${NC}"
echo -e "  URL:       ${CYAN}http://localhost:${APP_PORT}${NC}"
echo -e "  Ollama:    ${CYAN}${OLLAMA_MODEL} @ ${OLLAMA_HOST}${NC}"
echo ""
echo -e "  Storico immagini:"
docker images | grep "${IMAGE_BASE}" | awk '{printf "    %-35s %s %s\n", $1":"$2, $4, $5}'
echo ""
echo -e "  Comandi utili:"
echo -e "    docker logs ${CONTAINER_NAME} -f"
echo -e "    tail -f logs/converter.log"
echo -e "    curl http://localhost:${APP_PORT}/health"
echo -e "    docker exec -it ${CONTAINER_NAME} bash"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""

# ── STEP 9: Ultimi log ────────────────────────────────────────────────────────
sleep 2
log "Ultimi log del container:"
docker logs "${CONTAINER_NAME}" --tail=20 2>&1 | sed 's/^/  /'
echo ""
