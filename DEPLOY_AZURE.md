# Deploy su Azure VM – SAS → PySpark Converter

Guida rapida per avviare il convertitore web su una macchina virtuale Azure.

---

## 1. Requisiti VM consigliati

| Parametro        | Valore consigliato                        |
|------------------|-------------------------------------------|
| **Dimensione**   | Standard_B2s (2 vCPU, 4 GB RAM)          |
| **OS**           | Ubuntu Server 22.04 LTS                   |
| **Disco**        | 30 GB SSD Premium (P4)                    |
| **Rete**         | Porta **80** (HTTP) aperta nell'NSG        |
| **IP**           | IP Pubblico statico (opzionale ma utile)  |

---

## 2. Configurazione NSG (Network Security Group)

Aggiungi una regola **Inbound** nel NSG della VM:

| Campo            | Valore        |
|------------------|---------------|
| Priorità         | 100           |
| Nome             | Allow-HTTP    |
| Protocollo       | TCP           |
| Porta destinazione | 80          |
| Origine          | Any (o il tuo IP)  |
| Azione           | Allow         |

> Per HTTPS (porta 443) aggiungi un secondo record e configura un reverse proxy (nginx + Let's Encrypt).

---

## 3. Installazione sulla VM

Connettiti via SSH alla VM e lancia i seguenti comandi:

```bash
# ── 1. Aggiorna il sistema ──────────────────────────────────
sudo apt-get update && sudo apt-get upgrade -y

# ── 2. Installa Docker ──────────────────────────────────────
sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker   # oppure chiudi e riapri la sessione SSH

# ── 3. Clona il repository ──────────────────────────────────
git clone https://github.com/Maaroufi-H/sas-pyspark-conversion.git
cd sas-pyspark-conversion

# Passa al branch con la webapp
git checkout claude/webapp-docker-h8NEE

# ── 4. Avvia il servizio ────────────────────────────────────
docker compose up -d --build
```

---

## 4. Verifica che il servizio sia attivo

```bash
# Controlla lo stato del container
docker compose ps

# Controlla i log
docker compose logs -f

# Test health check
curl http://localhost/health
# Risposta attesa: {"status": "ok", "version": "1.0"}
```

L'app è ora raggiungibile all'indirizzo:

```
http://<IP-PUBBLICO-VM>/
```

---

## 5. Aggiornamento dell'applicazione

```bash
cd sas-pyspark-conversion

# Scarica le ultime modifiche
git pull origin claude/webapp-docker-h8NEE

# Rebuild e restart
docker compose up -d --build
```

---

## 6. Comandi Docker utili

```bash
# Avvia in background
docker compose up -d

# Arresta senza rimuovere
docker compose stop

# Arresta e rimuove i container
docker compose down

# Visualizza i log in tempo reale
docker compose logs -f sas-converter

# Accedi alla shell del container (debug)
docker compose exec sas-converter bash

# Forza rebuild dell'immagine
docker compose up -d --build --force-recreate
```

---

## 7. Output persistente

I file convertiti nella cartella `output/` sono montati come volume Docker.
Rimangono disponibili anche dopo il restart del container:

```bash
ls sas-pyspark-conversion/output/
```

---

## 8. HTTPS con Nginx (opzionale, produzione)

Per abilitare HTTPS con un dominio:

```bash
sudo apt-get install -y nginx certbot python3-certbot-nginx

# Crea configurazione Nginx
sudo tee /etc/nginx/sites-available/sas-converter << 'EOF'
server {
    listen 80;
    server_name tuo-dominio.example.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/sas-converter /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# Ottieni certificato SSL gratuito
sudo certbot --nginx -d tuo-dominio.example.com
```

Aggiorna `docker-compose.yml`: cambia `"80:5000"` in `"5000:5000"` (Nginx farà da proxy).

---

## 9. Monitoraggio risorse

```bash
# Utilizzo CPU/RAM del container
docker stats sas-pyspark-web

# Dimensione immagine Docker
docker images sas-converter
```

---

## 10. Risoluzione problemi comuni

| Problema | Soluzione |
|---|---|
| Porta 80 occupata | Cambia in `"8080:5000"` nel docker-compose.yml |
| Container si riavvia in loop | Controlla `docker compose logs` per l'errore |
| File SAS non trovato | Verifica che INPUT/ sia montato correttamente |
| Timeout su conversioni grandi | Aumenta `MAX_CONTENT_LENGTH` in webapp.py |
| Errore 502 Nginx | Verifica che il container Flask sia in ascolto su 5000 |
