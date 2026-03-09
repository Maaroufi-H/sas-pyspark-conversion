"""
sas_macro_preprocessor.py
=========================
Pre-processore SAS Livello 2: risolve le macro SAS *prima* della conversione
PySpark, producendo un file SAS "risolto" con SQL pulito.

Cosa fa questo modulo:
  1. Raccoglie tutti i  %let var = valore;  (variabili macro a valore costante)
  2. Risolve  &&var&i  con doppio-ampersand in due passate (come il pre-processore SAS)
  3. Srotola i loop  %do i=1 %to N; ... %end;  quando N è un letterale o un %let noto
  4. Produce un file  <nome>.resolved.sas  con SQL e DATA step privi di macro

Utilizzo da riga di comando:
  python sas_macro_preprocessor.py INPUT/code.sas --output INPUT/code.resolved.sas

Utilizzo programmatico:
  from sas_macro_preprocessor import SASMacroPreprocessor
  pre = SASMacroPreprocessor(known_vars={"nid": "3", "livAgregg": "1"})
  resolved_text = pre.process(sas_source_text)

Autore  : sas-pyspark-conversion
Versione: 1.0  (Livello 2 – senza LLM)
"""
from __future__ import annotations

import re
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════
# COSTANTI E PATTERN REGEX
# ═══════════════════════════════════════════════════════════════════════

# Pattern per %let var = valore;
_RE_LET = re.compile(
    r'%let\s+(\w+)\s*=\s*([^;]*?)\s*;',
    re.I
)

# Pattern per && (doppio ampersand, primo livello di indirezione)
_RE_DBL_AMP = re.compile(r'&&(\w+)', re.I)

# Pattern per &var. (singolo ampersand, con punto opzionale come delimitatore)
_RE_SINGLE_AMP = re.compile(r'&(\w+)\.?')

# Pattern per %do i = start %to stop;  (loop con variabile indice)
_RE_DO_LOOP = re.compile(
    r'%do\s+(\w+)\s*=\s*(\S+)\s+%to\s+(\S+?)\s*(?:%by\s+(\S+?)\s*)?;',
    re.I
)

# Pattern per %end; (fine blocco %do)
_RE_END = re.compile(r'%end\s*;', re.I)

# Pattern per %sysfunc(int(...))
_RE_SYSFUNC_INT = re.compile(r'%sysfunc\s*\(\s*int\s*\(([^)]+)\)\s*\)', re.I)


# ═══════════════════════════════════════════════════════════════════════
# CLASSE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════

class SASMacroPreprocessor:
    """
    Pre-processore per risolvere le variabili macro SAS e srotolare i loop
    prima della conversione PySpark.

    Parametri
    ---------
    known_vars : dict
        Variabili macro già note (nome_minuscolo → valore_stringa).
        Esempio: {"nid": "3", "livagregg": "1", "path_excel": "/data"}
        Vengono integrate con i %let trovati nel sorgente.
    max_loop_iterations : int
        Numero massimo di iterazioni per srotolare un singolo loop.
        Protegge da loop infiniti o molto grandi.
    """

    def __init__(
        self,
        known_vars: Optional[Dict[str, str]] = None,
        max_loop_iterations: int = 50,
    ) -> None:
        self.macro_vars: Dict[str, str] = {
            k.lower(): v for k, v in (known_vars or {}).items()
        }
        self.max_loop_iterations = max_loop_iterations
        self._warnings: List[str] = []

    # ── API pubblica ───────────────────────────────────────────────────

    def process(self, sas_text: str) -> str:
        """
        Elabora il testo SAS e restituisce il testo risolto.

        Passate eseguite in ordine:
          1. Raccolta %let
          2. Risoluzione %sysfunc(int(...))
          3. Risoluzione doppio-ampersand (&&var → &var → valore)
          4. Srotolamento loop %do i=1 %to N
          5. Risoluzione singolo-ampersand residuo
        """
        self._warnings.clear()

        # Passata 1: raccoglie tutti i %let nel sorgente
        self._collect_let_statements(sas_text)

        # Passata 2: risolve %sysfunc(int(expr))
        sas_text = self._resolve_sysfunc_int(sas_text)

        # Passata 3: risolve doppio-ampersand
        sas_text = self._resolve_double_ampersand(sas_text)

        # Passata 4: srotola i loop %do i=1 %to N (quando N è noto)
        sas_text = self._unroll_do_loops(sas_text)

        # Passata 5: risolve singolo-ampersand residuo
        sas_text = self._resolve_single_ampersand(sas_text)

        return sas_text

    @property
    def warnings(self) -> List[str]:
        """Lista degli avvertimenti generati durante l'elaborazione."""
        return list(self._warnings)

    # ── Passata 1: raccolta %let ───────────────────────────────────────

    def _collect_let_statements(self, sas_text: str) -> None:
        """
        Raccoglie tutte le istruzioni %let var = valore; dal testo SAS.
        I valori vengono aggiunti a self.macro_vars (priorità al primo trovato).
        """
        for m in _RE_LET.finditer(sas_text):
            var_name = m.group(1).lower()
            var_value = m.group(2).strip()
            if var_name not in self.macro_vars:
                self.macro_vars[var_name] = var_value

    # ── Passata 2: %sysfunc(int(...)) ─────────────────────────────────

    def _resolve_sysfunc_int(self, text: str) -> str:
        """
        Risolve %sysfunc(int(expr)) → valore intero se expr è un numero o
        un riferimento &var noto.
        Esempio: %sysfunc(int(&livAgregg)) con livAgregg=1.5 → 1
        """
        def _replace(m: re.Match) -> str:
            inner = m.group(1).strip()
            # Risolve eventuali &var nell'argomento
            inner_resolved = _RE_SINGLE_AMP.sub(
                lambda mm: self.macro_vars.get(mm.group(1).lower(), mm.group(0)),
                inner,
            )
            try:
                return str(int(float(inner_resolved)))
            except ValueError:
                return m.group(0)  # non risolvibile → lascia invariato

        return _RE_SYSFUNC_INT.sub(_replace, text)

    # ── Passata 3: doppio-ampersand ────────────────────────────────────

    def _resolve_double_ampersand(self, text: str) -> str:
        """
        Risolve i riferimenti &&var&i SAS in due passate:
          Passata A: &&nome  →  &nome  (rimuove un & )
          Passata B: &nome   →  valore (risoluzione finale)

        Esempio: &&id_prodotto&i  con i=2
          Passata A → &id_prodotto&i → non ancora risolvibile
          Passata B → &id_prodotto2  → valore se presente nei macro_vars

        Nota: il risultato finale potrebbe ancora contenere &var non risolti
        se i valori non sono noti — vengono lasciati per la passata 5.
        """
        # Passata A: && → & (rimozione del primo &)
        text = _RE_DBL_AMP.sub(lambda m: f"&{m.group(1)}", text)

        # Passata B: risolve i singoli &var con sostituzione
        text = _RE_SINGLE_AMP.sub(
            lambda m: self.macro_vars.get(m.group(1).lower(), m.group(0)),
            text,
        )
        return text

    # ── Passata 4: srotolamento loop %do ──────────────────────────────

    def _unroll_do_loops(self, text: str) -> str:
        """
        Srotola i loop  %do i=start %to stop; ... %end;  quando start e stop
        sono valori numerici noti (letterali o variabili macro risolte).

        Viene eseguita più volte fino a quando non ci sono più loop da srotolare.
        """
        max_passes = 10
        for _ in range(max_passes):
            new_text, changed = self._unroll_one_pass(text)
            if not changed:
                break
            text = new_text
        return text

    def _unroll_one_pass(self, text: str) -> Tuple[str, bool]:
        """
        Esegue una singola passata di srotolamento.
        Restituisce (nuovo_testo, modificato).
        """
        # Salta i loop già marcati come non-srotolabili (evita annidamento dei commenti)
        # Costruisce un indice delle posizioni già marcate
        marked_positions = set()
        for mm in re.finditer(r'/\* LOOP_(?:NON_SROTOLATO|TROPPO_GRANDE):', text):
            marked_positions.add(mm.start())

        m = _RE_DO_LOOP.search(text)
        # Se il match è dentro un blocco già marcato, cerca il prossimo non marcato
        while m is not None:
            # Controlla se la posizione è preceduta da un marker
            prefix = text[:m.start()]
            if '/* LOOP_NON_SROTOLATO:' in prefix[-50:] or '/* LOOP_TROPPO_GRANDE:' in prefix[-50:]:
                m = _RE_DO_LOOP.search(text, m.end())
            else:
                break
        if not m:
            return text, False

        idx_var = m.group(1).lower()
        start_raw = self._resolve_single_val(m.group(2))
        stop_raw = self._resolve_single_val(m.group(3))
        step_raw = self._resolve_single_val(m.group(4) or "1")

        # Verifica che start, stop, step siano numerici
        try:
            start = int(float(start_raw))
            stop = int(float(stop_raw))
            step = int(float(step_raw))
            if step == 0:
                raise ValueError("step=0")
        except (ValueError, TypeError):
            # Valori non noti o non numerici → impossibile srotolare
            self._warnings.append(
                f"[PREPROCESSORE] Loop '%do {idx_var}={m.group(2)} %to {m.group(3)}' "
                f"non srotolabile: valori non numerici o non noti."
            )
            # Segna questo loop come non srotolabile aggiungendo un commento
            marked = text[:m.start()] + f"/* LOOP_NON_SROTOLATO: {m.group(0)} */\n" + text[m.end():]
            return marked, True  # changed=True per evitare loop infinito

        iterations = list(range(start, stop + 1, step)) if step > 0 else list(range(start, stop - 1, step))
        if len(iterations) > self.max_loop_iterations:
            self._warnings.append(
                f"[PREPROCESSORE] Loop '%do {idx_var}={start} %to {stop}' "
                f"ha {len(iterations)} iterazioni > max {self.max_loop_iterations}: saltato."
            )
            marked = text[:m.start()] + f"/* LOOP_TROPPO_GRANDE: {m.group(0)} */\n" + text[m.end():]
            return marked, True

        # Trova il blocco fino al %end; corrispondente
        body_start = m.end()
        body, end_pos = self._extract_do_body(text, body_start)
        if body is None:
            self._warnings.append(
                f"[PREPROCESSORE] Loop '%do {idx_var}': %end; corrispondente non trovato."
            )
            return text, False

        # Srotola: sostituisce &i (o &idx_var) con il valore corrente in ogni iterazione
        expanded_parts = []
        for val in iterations:
            iteration_body = re.sub(
                r'&' + re.escape(idx_var) + r'\.?',
                str(val),
                body,
                flags=re.I,
            )
            expanded_parts.append(iteration_body)

        expanded = "".join(expanded_parts)

        # Ricostruisce il testo: prima del loop + corpo srotolato + dopo il %end;
        new_text = text[:m.start()] + expanded + text[end_pos:]
        return new_text, True

    def _extract_do_body(self, text: str, start: int) -> Tuple[Optional[str], int]:
        """
        Estrae il corpo di un blocco %do ... %end; gestendo la profondità.

        Restituisce (corpo, posizione_dopo_%end;) oppure (None, -1) se non trovato.
        """
        depth = 1
        pos = start
        while pos < len(text) and depth > 0:
            m_do = _RE_DO_LOOP.search(text, pos)
            m_end = _RE_END.search(text, pos)

            if m_end is None:
                return None, -1  # %end; non trovato

            if m_do is not None and m_do.start() < m_end.start():
                depth += 1
                pos = m_do.end()
            else:
                depth -= 1
                if depth == 0:
                    body = text[start:m_end.start()]
                    return body, m_end.end()
                pos = m_end.end()

        return None, -1

    # ── Passata 5: singolo-ampersand residuo ──────────────────────────

    def _resolve_single_ampersand(self, text: str) -> str:
        """
        Risolve i riferimenti &var residui usando self.macro_vars.
        I riferimenti non noti vengono lasciati invariati.
        """
        return _RE_SINGLE_AMP.sub(
            lambda m: self.macro_vars.get(m.group(1).lower(), m.group(0)),
            text,
        )

    # ── Helper ─────────────────────────────────────────────────────────

    def _resolve_single_val(self, val: str) -> str:
        """Risolve un singolo valore che può essere &var o un letterale."""
        if val is None:
            return "1"
        val = val.strip()
        # Risolve &var
        resolved = _RE_SINGLE_AMP.sub(
            lambda m: self.macro_vars.get(m.group(1).lower(), m.group(0)),
            val,
        )
        return resolved


# ═══════════════════════════════════════════════════════════════════════
# FUNZIONE DI UTILITÀ PER L'INTEGRAZIONE CON process_input.py
# ═══════════════════════════════════════════════════════════════════════

def preprocess_sas_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    known_vars: Optional[Dict[str, str]] = None,
    max_loop_iterations: int = 50,
) -> Tuple[Path, List[str]]:
    """
    Pre-processa un file SAS e salva il risultato.

    Parametri
    ---------
    input_path : Path
        Percorso del file SAS originale.
    output_path : Path, opzionale
        Percorso di destinazione. Se None, viene usato  <nome>.resolved.sas
        nella stessa cartella del file originale.
    known_vars : dict, opzionale
        Variabili macro note fornite dall'utente (es. nid, livAgregg).
    max_loop_iterations : int
        Limite iterazioni per srotolamento loop.

    Restituisce
    -----------
    (output_path, warnings) : percorso del file generato e lista avvertimenti.
    """
    input_path = Path(input_path)
    if output_path is None:
        output_path = input_path.with_suffix(".resolved.sas")
    output_path = Path(output_path)

    sas_text = input_path.read_text(encoding="utf-8", errors="replace")

    pre = SASMacroPreprocessor(known_vars=known_vars, max_loop_iterations=max_loop_iterations)
    resolved = pre.process(sas_text)

    output_path.write_text(resolved, encoding="utf-8")

    # Statistiche di risoluzione
    n_unresolved = len(re.findall(r'&\w+', resolved))
    n_loops_left = len(_RE_DO_LOOP.findall(resolved))
    warnings = pre.warnings + []
    if n_unresolved > 0:
        warnings.append(
            f"[PREPROCESSORE] {n_unresolved} riferimenti macro &var non risolti rimasti nel file. "
            f"Fornisci i valori con --macro-vars."
        )
    if n_loops_left > 0:
        warnings.append(
            f"[PREPROCESSORE] {n_loops_left} loop %do non srotolati rimasti nel file."
        )

    return output_path, warnings


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT CLI
# ═══════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pre-processore SAS Livello 2: risolve macro e srotola loop %do.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("input", help="File SAS di input (es. INPUT/code.sas)")
    p.add_argument(
        "--output", "-o",
        default=None,
        help="File di output (default: <input>.resolved.sas)",
    )
    p.add_argument(
        "--macro-vars", "-m",
        nargs="*",
        metavar="VAR=VALORE",
        default=[],
        help=(
            "Variabili macro note fornite dall'utente.\n"
            "Esempio: --macro-vars nid=3 livAgregg=1 path_excel=/data/excel"
        ),
    )
    p.add_argument(
        "--max-loop", "-l",
        type=int,
        default=50,
        help="Numero massimo iterazioni per srotolare un loop (default: 50).",
    )
    p.add_argument(
        "--show-vars",
        action="store_true",
        help="Mostra le variabili macro raccolte dal file e termina.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    # Costruisce il dizionario di variabili note dall'utente
    known_vars: Dict[str, str] = {}
    for kv in (args.macro_vars or []):
        if "=" in kv:
            k, v = kv.split("=", 1)
            known_vars[k.strip().lower()] = v.strip()
        else:
            print(f"[WARN] Formato non valido per --macro-vars: '{kv}' (atteso VAR=VALORE)", file=sys.stderr)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERRORE] File non trovato: {input_path}", file=sys.stderr)
        sys.exit(1)

    sas_text = input_path.read_text(encoding="utf-8", errors="replace")

    pre = SASMacroPreprocessor(known_vars=known_vars, max_loop_iterations=args.max_loop)

    # Modalità --show-vars: mostra solo le variabili trovate
    if args.show_vars:
        pre._collect_let_statements(sas_text)
        print("=== Variabili macro %let trovate nel file ===")
        for k, v in sorted(pre.macro_vars.items()):
            print(f"  &{k} = {v!r}")
        if known_vars:
            print("\n=== Variabili fornite dall'utente (--macro-vars) ===")
            for k, v in sorted(known_vars.items()):
                print(f"  &{k} = {v!r}")
        return

    output_path = Path(args.output) if args.output else None
    out_path, warnings = preprocess_sas_file(
        input_path=input_path,
        output_path=output_path,
        known_vars=known_vars,
        max_loop_iterations=args.max_loop,
    )

    print(f"[OK] File risolto salvato in: {out_path}")

    # Statistiche
    original_lines = sas_text.count("\n")
    resolved_text = out_path.read_text(encoding="utf-8")
    resolved_lines = resolved_text.count("\n")
    unresolved_refs = len(re.findall(r'&\w+', resolved_text))
    loops_left = len(_RE_DO_LOOP.findall(resolved_text))

    print(f"    Righe originali : {original_lines}")
    print(f"    Righe risolte   : {resolved_lines}")
    print(f"    Riferimenti &var non risolti : {unresolved_refs}")
    print(f"    Loop %do non srotolati       : {loops_left}")

    if warnings:
        print("\n=== Avvertimenti ===")
        for w in warnings:
            print(f"  {w}")


if __name__ == "__main__":
    main()
