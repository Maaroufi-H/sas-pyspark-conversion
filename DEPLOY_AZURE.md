# Deploy su Azure VM – SAS → PySpark Converter + Ollama

Guida completa da A a Z per avviare il convertitore web con LLM locale (Ollama)
su una macchina virtuale Azure.

---

## Architettura sulla VM

```
┌─────────────── Azure VM (Ubuntu 22.04) ───────────────┐
│                                                        │
│   ┌──────────────────────────────────┐                │
│   │  Docker: sas-pyspark-web         │                │
│   │  (Flask app su porta 5000)       │──── :80 ──→ web│
│   │                                  │                │
│   │  OLLAMA_HOST=host.docker.internal│                │
│   └──────────────┬───────────────────┘                │
│                  │ http://host.docker.internal:11434   │
│                  ▼                                     │
│   ┌──────────────────────────────────┐                │
│   │  Ollama (nativo sull'host)       │                │
│   │  Modello: codestral              │                │
│   │  Porta: localhost:11434          │                │
│   └──────────────────────────────────┘                │
│                                                        │
└────────────────────────────────────────────────────────┘
```

L'app Flask gira in un container Docker. Ollama gira direttamente sull'host
(fuori dal container). Il container raggiunge Ollama via `host.docker.internal`.

---

## 1. Requisiti VM consigliati

| Parametro         | Senza Ollama (solo regole)         | Con Ollama (LLM)                   |
|-------------------|------------------------------------|-------------------------------------|
| **Dimensione**    | Standard_B2s (2 vCPU, 4 GB RAM)  | Standard_D4s_v3 (4 vCPU, 16 GB RAM)|
| **OS**            | Ubuntu Server 22.04 LTS           | Ubuntu Server 22.04 LTS            |
| **Disco**         | 30 GB SSD                         | 64 GB SSD (per i modelli LLM)      |
| **Rete**          | Porta 80 aperta nell'NSG          | Porta 80 aperta nell'NSG           |

> Per codestral (~14 GB in memoria) servono almeno 16 GB di RAM.
> Per modelli piu' leggeri (deepseek-coder:6.7b) bastano 8 GB.

---

## 2. Connessione SSH alla VM

```bash
ssh <utente>@<IP-PUBBLICO-VM>
```

---

## 3. Comandi da eseguire sulla VM (copia-incolla)

### Step 1 — Aggiornamento sistema

```bash
sudo apt-get update && sudo apt-get upgrade -y
```

### Step 2 — Installazione Docker

```bash
sudo apt-get install -y docker.io docker-compose-plugin git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

### Step 3 — Installazione Ollama + download modello

```bash
# Installa Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Avvia Ollama come servizio
sudo systemctl enable --now ollama

# Verifica che Ollama sia attivo
curl http://localhost:11434/api/tags
# Risposta attesa: {"models":[]}

# Scarica il modello (scegli UNO):
# Opzione A: Codestral (consigliato, ~14 GB download)
ollama pull codestral

# Opzione B: DeepSeek Coder (piu' leggero, ~4 GB)
# ollama pull deepseek-coder:6.7b

# Opzione C: Mistral 7B (equilibrato, ~4 GB)
# ollama pull mistral:7b

# Verifica che il modello sia disponibile
ollama list
```

### Step 4 — Clone del repository

```bash
git clone https://github.com/Maaroufi-H/sas-pyspark-conversion.git
cd sas-pyspark-conversion
git checkout main
```

### Step 5 — Build e avvio

```bash
docker compose up -d --build
```

Questo comando:
1. Costruisce l'immagine Docker (Python 3.11 + Flask + pandas)
2. Avvia il container sulla porta 80
3. Si connette automaticamente a Ollama via `host.docker.internal:11434`

### Step 6 — Verifica

```bash
# Controlla lo stato del container
docker compose ps

# Controlla i log dell'applicazione
docker compose logs -f

# Test health check (include status Ollama)
curl http://localhost/health
# Risposta attesa: {"status":"ok","version":"1.0","ollama":true}

# Test diretto Ollama dall'host
curl http://localhost:11434/api/tags
# Risposta attesa: {"models":[{"name":"codestral:latest",...}]}
```

### Step 7 — Apertura porta 80 nell'NSG Azure

Dalla console Azure (portal.azure.com):

1. Vai alla tua VM → **Rete** (Networking)
2. Clicca **Aggiungi regola porta in ingresso** (Add inbound port rule)
3. Compila:
   - Porta destinazione: **80**
   - Protocollo: **TCP**
   - Azione: **Consenti** (Allow)
   - Priorita': **100**
   - Nome: **Allow-HTTP**
4. Salva

Oppure via Azure CLI:

```bash
az vm open-port --resource-group <GRUPPO-RISORSE> --name <NOME-VM> --port 80 --priority 100
```

### Step 8 — Accesso da browser

```
http://<IP-PUBBLICO-VM>/
```

Nella pagina:
1. Incolla il codice SAS nella textarea
2. (Opzionale) Spunta **"Pre-processore macro Livello 2"**
3. (Opzionale) Spunta **"Usa LLM Ollama"** per attivare il fallback LLM sui blocchi complessi
4. Clicca **Converti**

---

## 4. Cambiare il modello Ollama

```bash
# Scarica un altro modello
ollama pull deepseek-coder:6.7b

# Aggiorna la variabile nel docker-compose.yml:
#   OLLAMA_MODEL=deepseek-coder:6.7b
# Oppure direttamente:
docker compose down
OLLAMA_MODEL=deepseek-coder:6.7b docker compose up -d
```

Per rendere il cambio permanente, modifica `docker-compose.yml`:

```yaml
environment:
  - OLLAMA_MODEL=deepseek-coder:6.7b
```

---

## 5. Aggiornamento dell'applicazione

```bash
cd sas-pyspark-conversion
git pull origin main
docker compose up -d --build
```

---

## 6. Comandi Docker utili

```bash
# Avvia in background
docker compose up -d

# Arresta
docker compose stop

# Arresta e rimuovi container
docker compose down

# Visualizza i log in tempo reale
docker compose logs -f sas-converter

# Accedi alla shell del container (debug)
docker compose exec sas-converter bash

# Forza rebuild completo
docker compose up -d --build --force-recreate
```

---

## 7. Comandi Ollama utili

```bash
# Lista modelli installati
ollama list

# Scarica un nuovo modello
ollama pull <modello>

# Rimuovi un modello
ollama rm <modello>

# Test conversione dalla riga di comando
ollama run codestral "Convert this SAS to PySpark: data x; set y; where z>0; run;"

# Stato del servizio
sudo systemctl status ollama

# Riavvia Ollama
sudo systemctl restart ollama

# Log di Ollama
sudo journalctl -u ollama -f
```

---

## 8. HTTPS con Nginx (opzionale, produzione)

```bash
sudo apt-get install -y nginx certbot python3-certbot-nginx

sudo tee /etc/nginx/sites-available/sas-converter << 'EOF'
server {
    listen 80;
    server_name tuo-dominio.example.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/sas-converter /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# Certificato SSL gratuito
sudo certbot --nginx -d tuo-dominio.example.com
```

Con Nginx, aggiorna `docker-compose.yml`: cambia `"80:5000"` in `"5000:5000"`.

---

## 9. Monitoraggio risorse

```bash
# CPU/RAM del container Flask
docker stats sas-pyspark-web

# CPU/RAM di Ollama (processo host)
ps aux | grep ollama

# Spazio disco usato dai modelli
du -sh ~/.ollama/models/
```

---

## 10. Risoluzione problemi

| Problema | Causa | Soluzione |
|---|---|---|
| `{"ollama": false}` nel health check | Ollama non raggiungibile dal container | Verifica: `sudo systemctl status ollama` e che `OLLAMA_HOST` sia corretto nel docker-compose.yml |
| Porta 80 non accessibile | NSG non configurato | Aggiungi regola inbound porta 80 nell'NSG Azure |
| Ollama risponde lentamente | RAM insufficiente | Usa un modello piu' leggero o una VM con piu' RAM |
| `docker compose up` fallisce | Docker non nel gruppo | Esegui `sudo usermod -aG docker $USER && newgrp docker` |
| Errore "model not found" in Ollama | Modello non scaricato | Esegui `ollama pull codestral` |
| Container si riavvia in loop | Errore Python | Esegui `docker compose logs -f` per vedere l'errore |
| Conversione timeout | File SAS troppo grande | Aumenta `MAX_CONTENT_LENGTH` in webapp.py |
| Ollama usa troppa RAM | Modello troppo grande | Passa a `deepseek-coder:6.7b` (4GB) o `mistral:7b` |
