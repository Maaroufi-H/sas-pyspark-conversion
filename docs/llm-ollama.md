# Usa LLM Ollama

## Cosa fa

Quando il motore deterministico (Livello 1) classifica un blocco come
**non convertibile** (affidabilità BASSA o `convertibile=NO`), l'LLM Ollama
viene chiamato come **fallback** per tentare una conversione generativa.

L'LLM gira **localmente** sul server/VM — nessun dato esce dalla rete aziendale.

---

## Logica di attivazione

```
Per ogni blocco SAS:
    1. Il motore deterministico analizza il blocco con regole JSON
    2. Se convertibile=SI → genera PySpark standard
    3. Se convertibile=NO e use_llm=True → chiama OllamaConverter.convert()
    4. Se Ollama risponde → usa il codice generato (marcato [LLM-generated])
    5. Se Ollama non risponde/errore → genera un blocco # TODO
```

---

## Come funziona internamente

Il `OllamaConverter`:
1. Chiama `GET /api/tags` per verificare che Ollama sia attivo e il modello
   caricato (timeout: 3 secondi)
2. Costruisce un prompt con:
   - Fino a 5 esempi di correzioni manuali precedenti (few-shot da `learning_db.jsonl`)
   - Il tipo del blocco SAS (es. `data`, `proc_sql`, `macro`)
   - Il codice SAS da convertire
3. Chiama `POST /api/generate` con `stream: false` (timeout: 120 secondi)
4. Restituisce il codice Python generato

---

## Configurazione (`llm_config.json`)

```json
{
  "strategy": "auto",
  "ollama": {
    "host": "http://localhost:11434",
    "model": "codestral"
  }
}
```

| Parametro | Descrizione |
|-----------|-------------|
| `strategy` | `auto` \| `ollama` \| `none` |
| `ollama.host` | URL del server Ollama (default: `http://localhost:11434`) |
| `ollama.model` | Nome del modello (es. `codestral`, `deepseek-coder:6.7b`) |

Variabili d'ambiente equivalenti: `OLLAMA_HOST`, `OLLAMA_MODEL`.

---

## Modelli consigliati

| Modello | RAM richiesta | Qualità conversione |
|---------|--------------|---------------------|
| `deepseek-coder:6.7b` | ~5 GB | Buona — ottimo sul codice |
| `codestral` | ~12 GB | Ottima — specializzato nel codice |
| `codellama:13b` | ~10 GB | Buona |
| `mistral:7b` | ~5 GB | Discreta — uso generale |

---

## Logging

Con l'LLM attivo, tutte le comunicazioni con Ollama sono registrate in
`logs/ollama_comms.log`:

```
2026-03-10 14:22:01 [INFO ] [Ollama] Check disponibilità → GET http://localhost:11434/api/tags
2026-03-10 14:22:01 [INFO ] [Ollama] Modelli presenti: ['codestral:latest'] | 'codestral' trovato: True
2026-03-10 14:22:01 [INFO ] [Ollama] Invio richiesta | model=codestral | categoria='data' | SAS=312 chars
2026-03-10 14:22:09 [INFO ] [Ollama] Risposta ricevuta | 8.2s | 487 chars output
```

---

## Limitazioni e problemi pending

- **Qualità variabile**: i modelli locali producono codice corretto in circa
  60-80% dei casi. L'output è sempre marcato `[LLM-generated]` per permettere
  revisione manuale.
- **Tempo di risposta**: ogni blocco richiede 5-60 secondi a seconda del modello
  e della lunghezza del codice.
- **RAM**: i modelli 7B+ richiedono GPU o molta RAM (la conversione lenta su CPU
  è possibile ma richiede minuti per blocco).
- **Correzione/feedback**: le correzioni manuali possono essere salvate in
  `learning_db.jsonl` tramite `OllamaConverter.record_correction()` per
  migliorare i prompt futuri (few-shot).
- **Nessuna validazione sintattica**: il codice Python generato non viene
  verificato prima di essere inserito nell'output.

---

## File sorgente

`llm_local.py` — classe `OllamaConverter` (≈200 righe)

<a href="/" style="color:#38bdf8">← Torna all'app</a>
