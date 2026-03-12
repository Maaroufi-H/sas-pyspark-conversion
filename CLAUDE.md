# SAS → PySpark Converter — Agent Context

## Ruolo dell'agente
Sei un agente esperto su questo progetto. Ad ogni sessione:
- Leggi questo file per ricostruire il contesto completo
- Esegui `git status` e `git log --oneline -5` per capire lo stato attuale
- Controlla se il container Docker è attivo: `docker ps | grep sas-pyspark-web`

---

## path del progetto
/home/iet/sas-pyspark-conversion

## Architettura del progetto (3 livelli)

```
SAS Input
  ↓
[L0] sas_macro_preprocessor.py   → espande &var, &&var, %do/%let
  ↓
[L1] trova_sas3_tracker.py       → parsing blocchi SAS
     convert_engine.py           → conversione deterministica con regole JSON
  ↓
[L2] llm_local.py (Ollama)       → LLM fallback per blocchi non convertibili
  ↓
Flask webapp.py                  → interfaccia web (porta 80 → 5000 nel container)
```

---

## File chiave

| File | Ruolo |
|---|---|
| `webapp.py` | Flask app: route `/`, `/convert`, `/health`, `/docs/<file>` |
| `convert_engine.py` | Motore conversione (1550 righe) |
| `llm_local.py` | OllamaConverter + CodestralConverter |
| `trova_sas3_tracker.py` | Parser SAS → albero di blocchi |
| `sas_macro_preprocessor.py` | Pre-processore macro (5 passi) |
| `llm_config.json` | Strategia LLM + endpoint Ollama |
| `regole_classificazione.json` | Regole blocchi SAS (convertibile sì/no) |
| `docker-compose.yml` | Orchestrazione container + env vars |
| `deploy.sh` | Script automazione build/deploy Docker |
| `logs/converter.log` | Log unificato app (rotazione 2MB×3) |

---

## Configurazione Ollama

- **Modello attivo:** `llama3.1:70b`
- **Host dal container:** `http://host.docker.internal:11434`
- **Host dall'host Ubuntu:** `http://localhost:11434`
- **Env var Docker:** `OLLAMA_HOST=http://host.docker.internal:11434`
- **Verifica Ollama attivo:** `curl http://localhost:11434/api/tags`
- **Lista modelli:** `ollama list`

### BUG NOTO — localhost vs host.docker.internal
Se i log mostrano `GET http://localhost:11434/api/tags` invece di
`GET http://host.docker.internal:11434/api/tags` il problema è in
`llm_local.py` nella funzione `create_llm_backend()`: usa il valore di
`llm_config.json` invece dell'env var. Fix: parametro esplicito deve
avere precedenza sul config file (sentinel pattern con `_UNSET`).

---

## Comandi Docker operativi

```bash
# Stato container
docker ps -a | grep sas-pyspark-web

# Log in tempo reale
docker logs sas-pyspark-web -f --tail=50

# Log applicazione (montato sull'host)
tail -f logs/converter.log

# Health check
curl -s http://localhost/health | python3 -m json.tool

# Entrare nel container
docker exec -it sas-pyspark-web bash

# Build + deploy completo
./deploy.sh

# Build senza git pull
./deploy.sh --no-pull

# Storico immagini
docker images | grep sas-converter
```

---

## Diagnosi problemi Ollama

Sequenza di controllo quando Ollama non risponde:

```bash
# 1. Ollama è attivo sull'host?
curl http://localhost:11434/api/tags

# 2. Il modello è caricato?
ollama list | grep llama3.1

# 3. Dal container, l'host è raggiungibile?
docker exec sas-pyspark-web curl http://host.docker.internal:11434/api/tags

# 4. Controlla env var nel container
docker exec sas-pyspark-web env | grep OLLAMA

# 5. Verifica extra_hosts in docker-compose
grep host-gateway docker-compose.yml

# 6. Leggi log Ollama nell'app
grep -i ollama logs/converter.log | tail -30
```

### Pattern di errori comuni

| Errore nel log | Causa | Fix |
|---|---|---|
| `Connection refused localhost:11434` | `llm_config.json` sovrascrive env var | Fix sentinel in `create_llm_backend()` |
| `Connection refused host.docker.internal` | `extra_hosts` mancante in docker-compose | Aggiungere `host-gateway` |
| `model not found` | Modello non scaricato | `ollama pull llama3.1:70b` |
| `context length exceeded` | Blocco SAS troppo lungo | Ridurre chunk size in `llm_local.py` |

---

## Git — convenzioni branch

I branch dell'agente iniziano sempre con `claude/` e finiscono con `-h8NEE`.

```bash
git branch -a | grep claude
git pull origin <branch>
git checkout -b claude/<feature>-h8NEE
git push -u origin claude/<feature>-h8NEE
```

---

## Variabili d'ambiente Docker

```
FLASK_ENV=production
PORT=5000
OLLAMA_HOST=http://host.docker.internal:11434
OLLAMA_MODEL=llama3.1:70b
```

---

## Checklist dopo ogni git pull

1. `git log --oneline -5` — verifica cosa è cambiato
2. `git diff HEAD~1 HEAD --name-only` — lista file modificati
3. Se cambiati `*.py`, `Dockerfile`, `requirements.txt` → `./deploy.sh`
4. Se cambiato solo `llm_config.json` → `docker restart sas-pyspark-web`
5. Se cambiati solo `templates/` → `docker restart sas-pyspark-web`
6. Verifica: `curl http://localhost/health`

