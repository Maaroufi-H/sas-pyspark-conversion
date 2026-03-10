# Variabili Macro

## Cosa sono

Le variabili macro SAS (`&nome`) sono placeholder che vengono risolti dal
macro-processore SAS prima dell'esecuzione. Per convertire correttamente il
codice, il convertitore ha bisogno di conoscere i loro valori.

---

## Come funziona nella webapp

Nella sezione **Variabili macro** dell'app puoi aggiungere coppie
`nome` + `valore` che vengono passate al Pre-processore Livello 2 e al
motore di conversione.

### Aggiungere una variabile

1. Clicca **"+ Aggiungi variabile"**
2. Inserisci il **nome** della variabile (senza `&`, es. `nid`)
3. Inserisci il **valore** corrispondente (es. `3`)
4. Clicca **"✕ Elimina"** su una riga per rimuoverla

Puoi aggiungere quante variabili vuoi.

### Cosa succede al submit

Le righe vengono serializzate nel formato `nome=valore` separato da spazi
e passate al backend:

```
nid=3 livAgregg=1 path_excel=/data
```

---

## Come il backend le usa

La funzione `_parse_macro_vars(raw)` in `webapp.py` converte la stringa in
un dizionario Python:

```python
{"nid": "3", "livAgregg": "1", "path_excel": "/data"}
```

Questo dizionario viene passato a:
- `SASMacroPreprocessor(known_vars=...)` → risolve `&nid` → `3` nel testo SAS
- `ConversionContext` → disponibile durante la generazione del codice PySpark

---

## Quando servono

Le variabili macro sono necessarie quando il codice SAS usa:

| Pattern SAS | Esempio | Quando serve il valore |
|-------------|---------|----------------------|
| `&var` diretto | `where id = "&prodotto"` | Sempre |
| `&&var&i` | `&&id_prodotto&i` con `i` noto | Quando `i` dipende da `&nid` |
| `%do i=1 %to &nid` | Ciclo con limite variabile | Per srotolare il ciclo |

---

## Limitazioni e problemi pending

- **Variabili calcolate da query**: se il valore di `&var` dipende da
  `&sqlobs` (numero di righe di una query precedente), non può essere
  fornito staticamente. Il ciclo che dipende da quel valore non verrà
  srotolato.
- **Variabili di sistema**: `&sysdate`, `&systime`, `&syserr` non vengono
  risolte automaticamente — fornirle manualmente se necessario.
- **Maiuscole/minuscole**: i nomi delle variabili vengono normalizzati in
  minuscolo internamente (come fa SAS).

---

## File sorgente

- `webapp.py` — `_parse_macro_vars()` (parsing form input)
- `sas_macro_preprocessor.py` — `SASMacroPreprocessor` (utilizzo variabili)

<a href="/" style="color:#38bdf8">← Torna all'app</a>
