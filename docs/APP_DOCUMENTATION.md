# SAS → PySpark Converter — Documentazione Generale

## Architettura

L'app è organizzata in **3 livelli**:

```
┌─────────────────────────────────────────────────────┐
│  LIVELLO 0 — PRE-PROCESSORE (opzionale)             │
│  sas_macro_preprocessor.py                          │
│  Risolve &var, &&var&i, srotola %do loops           │
└─────────────────────────┬───────────────────────────┘
                          │ codice SAS "piatto"
┌─────────────────────────▼───────────────────────────┐
│  LIVELLO 1 — MOTORE DETERMINISTICO                  │
│  trova_sas3_tracker.py  →  Parser gerarchico        │
│  regole_classificazione.json  →  Regole per blocco  │
│  convert_engine.py  →  Generatore PySpark           │
└─────────────────────────┬───────────────────────────┘
                          │ PySpark + TODO per blocchi irrisolti
┌─────────────────────────▼───────────────────────────┐
│  LIVELLO 2 — LLM FALLBACK (opzionale)               │
│  llm_local.py  →  OllamaConverter                   │
│  Chiamato solo per blocchi con convertibile=NO      │
└─────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│  WEBAPP — Flask                                     │
│  webapp.py  →  route /, /convert, /health, /docs    │
│  templates/index.html  →  form input                │
│  templates/result.html →  output + analisi blocchi  │
└─────────────────────────────────────────────────────┘
```

---

## File principali

| File | Ruolo |
|------|-------|
| `webapp.py` | Applicazione Flask (entry point web) |
| `convert_engine.py` | Generatore di codice PySpark (≈1550 righe) |
| `trova_sas3_tracker.py` | Parser gerarchico SAS → blocchi |
| `sas_macro_preprocessor.py` | Pre-processore macro (5 passate) |
| `llm_local.py` | Backend LLM locale (Ollama) e anonimizzatore |
| `regole_classificazione.json` | Regole di classificazione per tipo di blocco |
| `llm_config.json` | Configurazione LLM (strategia, host, modello) |
| `learning_db.jsonl` | Correzioni manuali per few-shot LLM |

---

## Logica di `regole_classificazione.json`

Il file contiene le regole che il motore usa per classificare ogni blocco
SAS e decidere se può essere convertito automaticamente.

**Struttura**:
```json
{
  "data": [ { regola }, ... ],
  "proc_sql": [ { regola }, ... ],
  "macro": [ { regola }, ... ],
  ...
}
```

Ogni regola ha:

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `nome_regola` | stringa | Nome descrittivo della regola |
| `keywords_presenti` | lista di regex | Pattern da cercare nel codice SAS |
| `logica` | `ANY` / `COUNT_GT_15` / `DEFAULT` | Come combinare i match |
| `convertibile` | `SI` / `NO` | Se il blocco è convertibile |
| `affidabilita` | `ALTA` / `MEDIA` / `BASSA` | Fiducia nella conversione |

**Logica di matching**:
- `ANY` → basta che almeno una keyword faccia match
- `COUNT_GT_15` → il blocco supera 15 righe (usato per blocchi lunghi)
- `DEFAULT` → regola di fallback (sempre applicata se nessuna altra fa match)

---

## Logica di `llm_config.json`

```json
{
  "_comment": "...",
  "strategy": "auto",
  "ollama": { "host": "...", "model": "..." },
  "codestral": { "host": "...", "api_key": "...", "model": "..." }
}
```

| `strategy` | Comportamento |
|------------|---------------|
| `auto` | Prova Ollama locale; se non disponibile, nessun LLM |
| `ollama` | Usa sempre Ollama (errore se non disponibile) |
| `codestral` | Usa endpoint OpenAI-compatibile (Codestral, vLLM, ecc.) |
| `anon` | Anonimizza il codice, poi usa Codestral |
| `none` | Disabilita completamente l'LLM |

---

## Flusso dati end-to-end

```
Input SAS (form web)
    │
    ├─ [se preprocess=True] SASMacroPreprocessor.process()
    │       → risolve &var, srotola %do
    │
    ├─ parse_sas_blocks_tracked(tmp_file)
    │       → lista di blocchi con tipo, righe, testo SAS
    │
    ├─ convert_tree(blocks, llm_backend)
    │       → per ogni blocco:
    │           - classifica con regole_classificazione.json
    │           - se convertibile=SI → genera PySpark deterministico
    │           - se convertibile=NO e LLM attivo → OllamaConverter.convert()
    │           - altrimenti → # TODO block
    │       → codice PySpark completo (stringa)
    │
    ├─ convert_blocks_to_map(blocks, llm_backend)
    │       → dizionario {uid → codice PySpark del blocco}
    │
    ├─ build_dataframe(blocks, pyspark_map)
    │       → DataFrame pandas con 16 colonne per analisi
    │
    ├─ build_stats(df)
    │       → statistiche aggregate (per tipo, per affidabilità)
    │
    └─ render_template("result.html", ...)
            → pagina HTML con codice PySpark + tabella blocchi
```

---

## Problemi pending

### Pre-processore (Livello 0)
- `%macro` con argomenti non viene espanso
- `%if`/`%else` condizionali non sempre risolvibili
- `%do` con limiti dipendenti da query (`&sqlobs`) non viene srotolato
- `%include` non legge i file inclusi
- Doppio ampersand con più di 2 livelli non gestito

### Motore deterministico (Livello 1)
- **RETAIN statement** → conversione parziale con Window functions
- **HASH object** → `# TODO` (richiede logica complessa)
- **ARRAY SAS** → non supportato
- **PROC TRANSPOSE** → `# TODO`
- **PROC MEANS / PROC FREQ** → copertura parziale
- **Formati SAS personalizzati** (`FORMAT`, `PICTURE`) → `# TODO`
- **Macro `%MACRO`/`%MEND` annidate** → non srotolate
- **`PUT` / `INPUT` con format-list** → conversione sommaria
- **`FILENAME` / `LIBNAME` statement** → ignorati o commentati

### LLM Fallback (Livello 2)
- Nessuna validazione sintattica del codice generato
- Qualità dipende fortemente dal modello Ollama installato
- Timeout fisso a 120s per blocco
- La learning DB (`learning_db.jsonl`) non viene aggiornata automaticamente

### Webapp
- Nessun sistema di autenticazione
- Nessun download diretto del file `.py` generato
- Il codice delle singole righe nella tabella non ha il proprio pulsante di copia

---

## Deploy

Vedi `DEPLOY_AZURE.md` per le istruzioni complete su Azure VM + Docker.

Avvio rapido locale:
```bash
pip install flask pandas openpyxl
python webapp.py
# → http://localhost:5000
```

Con Docker:
```bash
docker-compose up -d
```
