# Pre-processore Macro Livello 2

## Cosa fa

Il pre-processore macro di Livello 2 risolve il codice SAS **prima** che venga
passato al motore di conversione. Agisce su costrutti tipicamente generati dai
macro-processori SAS:

- Raccoglie le definizioni `%let var = valore;`
- Risolve i riferimenti `&var` (singolo ampersand)
- Risolve i riferimenti `&&var&i` (doppio ampersand — due passate, come SAS)
- Srotola i cicli `%do i=1 %to N; ... %end;` quando `N` è noto
- Valuta `%sysfunc(int(...))` per determinare limiti numerici

Il risultato è un file `.sas` "piatto", senza macro variabili irrisolte, che
il motore deterministico (Livello 1) riesce a convertire con affidabilità
molto più alta.

---

## Algoritmo — 5 passate

| Passata | Funzione interna | Esempio |
|---------|-----------------|---------|
| 1 | `_collect_let_statements()` | `%let nid=3;` → `macro_vars["nid"]="3"` |
| 2 | `_resolve_sysfunc_int()` | `%sysfunc(int(&var))` → valore numerico |
| 3 | `_resolve_double_ampersand()` | `&&id_prodotto&i` → `id_prodotto2` (2 sottoppassate) |
| 4 | `_unroll_do_loops()` | `%do i=1 %to 3; ... %end;` → 3 copie espanse |
| 5 | `_resolve_single_ampersand()` | `&var` → valore noto o lasciato come `&var` |

---

## Come si attiva

Nella webapp: **spunta il checkbox** "Pre-processore macro Livello 2" prima
di cliccare Converti.

Da riga di comando:
```bash
python sas_macro_preprocessor.py INPUT/code.sas \
       --output INPUT/code.resolved.sas \
       --macro-vars nid=3 livAgregg=1
```

Le variabili macro che non compaiono nel codice SAS (non definite con `%let`)
possono essere fornite manualmente nella sezione **Variabili macro** della
webapp (o via `--macro-vars` da CLI).

---

## Parametri

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `known_vars` | `{}` | Dict con variabili macro pre-note (es. `{"nid": "3"}`) |
| `max_loop_iterations` | 50 | Limite massimo di iterazioni per `%do` loop |

---

## Limitazioni e problemi pending

- **`%macro` con argomenti**: il pre-processore **non espande** le chiamate
  a macro che ricevono parametri (es. `%importa_dati(flusso=A)`). Questo
  richiede un interprete macro completo — fuori scope per ora.
- **`%if`/`%else` condizionali**: solo il ramo attivo viene mantenuto se
  la condizione è risolvibile; altrimenti entrambi i rami restano nel codice.
- **Cicli `%do` con limiti non numerici**: se `N` dipende da `&sqlobs` o da
  una query, il ciclo NON viene srotolato e rimane un `# TODO` nel codice.
- **`%include`**: i file inclusi non vengono letti automaticamente.
- **Doppio ampersand con più di 2 livelli** (`&&&var`): non gestito.

---

## File sorgente

`sas_macro_preprocessor.py` — classe `SASMacroPreprocessor` (circa 500 righe)

<a href="/" style="color:#38bdf8">← Torna all'app</a>
