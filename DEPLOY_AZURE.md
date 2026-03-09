# Deploy su Azure VM – SAS → PySpark Converter + Ollama

Guida completa da A a Z per avviare il convertitore web con LLM locale (Ollama)
su una macchina virtuale Azure, **senza docker-compose**.

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
sudo apt-get install -y docker.io git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

> **Nota:** Non e' necessario installare docker-compose. Si usa solo `docker` standard.

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

### Step 5 — Build e avvio (con run.sh)

```bash
# Rendi lo script eseguibile
chmod +x run.sh

# Build dell'immagine + avvio del container
./run.sh rebuild
```

Questo comando:
1. Costruisce l'immagine Docker (Python 3.11 + Flask + pandas)
2. Avvia il container sulla porta 80
3. Si connette automaticamente a Ollama via `host.docker.internal:11434`
4. Configura il riavvio automatico in caso di crash o reboot

**In alternativa, i comandi manuali equivalenti:**

```bash
# Build
docker build -t sas-converter:latest .

# Avvio
docker run -d \
  --name sas-pyspark-web \
  --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  -p 80:5000 \
  -e FLASK_ENV=production \
  -e PORT=5000 \
  -e OLLAMA_HOST=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=codestral \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/INPUT:/app/INPUT:ro" \
  sas-converter:latest
```

### Step 6 — Verifica

```bash
# Stato del container
docker ps

# Log dell'applicazione
docker logs -f sas-pyspark-web

# Test health check (include status Ollama)
curl http://localhost/health
# Risposta attesa: {"status":"ok","version":"1.0","ollama":true}

# Test diretto Ollama dall'host
curl http://localhost:11434/api/tags
# Risposta attesa: {"models":[{"name":"codestral:latest",...}]}

# Oppure con lo script
./run.sh status
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

## 4. Comandi run.sh

```bash
./run.sh            # avvia (build automatico se immagine assente)
./run.sh rebuild    # forza rebuild completo e riavvio
./run.sh stop       # ferma e rimuove il container
./run.sh logs       # log in tempo reale
./run.sh status     # stato + health check
./run.sh build      # solo build immagine
```

---

## 5. Cambiare il modello Ollama

```bash
# Scarica un altro modello
ollama pull deepseek-coder:6.7b

# Riavvia con il nuovo modello
./run.sh stop
OLLAMA_MODEL=deepseek-coder:6.7b ./run.sh start
```

Oppure con i comandi manuali:

```bash
docker rm -f sas-pyspark-web

docker run -d \
  --name sas-pyspark-web \
  --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  -p 80:5000 \
  -e FLASK_ENV=production \
  -e PORT=5000 \
  -e OLLAMA_HOST=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=deepseek-coder:6.7b \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/INPUT:/app/INPUT:ro" \
  sas-converter:latest
```

---

## 6. Aggiornamento dell'applicazione

```bash
cd sas-pyspark-conversion
git pull origin main
./run.sh rebuild
```

---

## 7. Comandi Docker utili

```bash
# Lista container in esecuzione
docker ps

# Lista tutti i container (anche fermi)
docker ps -a

# Log in tempo reale
docker logs -f sas-pyspark-web

# Ferma il container
docker stop sas-pyspark-web

# Avvia il container fermato
docker start sas-pyspark-web

# Rimuovi il container
docker rm -f sas-pyspark-web

# Accedi alla shell del container (debug)
docker exec -it sas-pyspark-web bash

# Statistiche CPU/RAM
docker stats sas-pyspark-web

# Lista immagini
docker images

# Rimuovi immagine vecchia
docker rmi sas-converter:latest
```

---

## 8. Comandi Ollama utili

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

## 9. HTTPS con Nginx (opzionale, produzione)

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

Con Nginx, cambia la porta nel `docker run`: sostituisci `-p 80:5000` con `-p 5000:5000`.

---

## 10. Monitoraggio risorse

```bash
# CPU/RAM del container Flask
docker stats sas-pyspark-web

# CPU/RAM di Ollama (processo host)
ps aux | grep ollama

# Spazio disco usato dai modelli
du -sh ~/.ollama/models/
```

---

## 11. Risoluzione problemi

| Problema | Causa | Soluzione |
|---|---|---|
| `{"ollama": false}` nel health check | Ollama non raggiungibile dal container | Verifica: `sudo systemctl status ollama` e che `--add-host host.docker.internal:host-gateway` sia presente nel `docker run` |
| Porta 80 non accessibile | NSG non configurato | Aggiungi regola inbound porta 80 nell'NSG Azure |
| Ollama risponde lentamente | RAM insufficiente | Usa un modello piu' leggero o una VM con piu' RAM |
| `docker: permission denied` | Utente non nel gruppo docker | Esegui `sudo usermod -aG docker $USER && newgrp docker` |
| Errore "model not found" in Ollama | Modello non scaricato | Esegui `ollama pull codestral` |
| Container si riavvia in loop | Errore Python | Esegui `docker logs sas-pyspark-web` per vedere l'errore |
| Conversione timeout | File SAS troppo grande | Aumenta `MAX_CONTENT_LENGTH` in webapp.py |
| Ollama usa troppa RAM | Modello troppo grande | Passa a `deepseek-coder:6.7b` (4GB) o `mistral:7b` |
| Container non si riavvia dopo reboot | Flag `--restart` assente | Aggiungi `--restart unless-stopped` nel comando `docker run` |
