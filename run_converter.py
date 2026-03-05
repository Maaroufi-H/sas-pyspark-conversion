"""
run_converter.py
================
Punto di ingresso unico del pipeline SAS -> PySpark.

Regola di nomenclatura:
    <nome>.sas   ->   <nome>.py      (script PySpark generato)
    <nome>.sas   ->   <nome>.json    (albero dei blocchi, intermedio)
    <nome>.sas   ->   <nome>.xlsx    (rapporto di analisi, opzionale)

Esempi di utilizzo:
    # File singolo
    python run_converter.py code.sas

    # File con cartella di output esplicita
    python run_converter.py estratto/CodeTask-xxx/code.sas --output-dir ./pyspark_output

    # Tutti i .sas di una cartella (batch)
    python run_converter.py --batch estratto/ --output-dir ./pyspark_output

    # Senza rapporto Excel
    python run_converter.py code.sas --no-excel
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

# -- Import dai moduli del progetto -----------------------------------
from trova_sas3_tracker import (
    parse_sas_blocks_tracked,
    build_dataframe,
    build_stats,
    export_excel,
    export_json,
)
from convert_engine import convert_tree, convert_blocks_to_map
from llm_local import create_llm_backend


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE SINGLE FILE
# ═══════════════════════════════════════════════════════════════════════

def convert_sas_file(
    sas_path: Path,
    output_dir: Path,
    *,
    excel: bool = True,
    json_intermediate: bool = True,
    verbose: bool = True,
    rules_path: Optional[Path] = None,
    llm_backend: object = None,
) -> Path:
    """
    Converte un singolo file SAS in PySpark.

    Nomi di output derivati direttamente dal nome del file SAS:
        input  : <qualsiasi_percorso>/<nome>.sas
        output : <output_dir>/<nome>.py
                 <output_dir>/<nome>.json   (se json_intermediate=True)
                 <output_dir>/<nome>.xlsx   (se excel=True)

    Returns:
        Path del file .py generato.
    """
    sas_path   = Path(sas_path).resolve()
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = sas_path.stem   # "code"  (senza estensione)

    py_path   = output_dir / f"{stem}.py"
    json_path = output_dir / f"{stem}.json"
    xlsx_path = output_dir / f"{stem}.xlsx"

    if not sas_path.exists():
        print(f"[ERRORE] File non trovato: {sas_path}")
        return None

    if verbose:
        print(f"\n{'='*60}")
        print(f"  INPUT  : {sas_path}")
        print(f"  OUTPUT : {py_path}")
        print(f"{'='*60}")

    # -- Passo 1 : Parsing --------------------------------------------
    if verbose:
        print("  [1/3] Parsing blocchi SAS...")

    blocks = parse_sas_blocks_tracked(sas_path)

    if verbose:
        print(f"        Blocchi trovati: {len(blocks)}")

    # Genera la mappa uid -> codice PySpark per la colonna Excel
    pyspark_map = convert_blocks_to_map(blocks, llm_backend=llm_backend)

    # -- Passo 2 : Export intermedi (JSON + Excel) --------------------
    if json_intermediate:
        export_json(blocks, json_path)
        if verbose:
            print(f"  [2a]  JSON  -> {json_path.name}")

    if excel:
        df    = build_dataframe(blocks, pyspark_map=pyspark_map)
        stats = build_stats(df)
        export_excel(df, stats, xlsx_path)
        if verbose:
            print(f"  [2b]  Excel -> {xlsx_path.name}")

    # -- Passo 3 : Conversione PySpark --------------------------------
    if verbose:
        print("  [3/3] Conversione in PySpark...")

    pyspark_code = convert_tree(blocks, llm_backend=llm_backend)

    # Aggiunge l'intestazione con riferimento al file SAS originale
    origin_comment = (
        f"# SAS originale : {sas_path}\n"
        f"# Generato da   : run_converter.py\n"
    )
    pyspark_code = pyspark_code.replace(
        "# Script generato automaticamente da convert_engine.py\n",
        "# Script generato automaticamente da convert_engine.py\n" + origin_comment,
        1
    )

    py_path.write_text(pyspark_code, encoding="utf-8")

    if verbose:
        lines = pyspark_code.count("\n")
        print(f"        PySpark -> {py_path.name}  ({lines} righe)")

    return py_path


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE BATCH (cartella intera)
# ═══════════════════════════════════════════════════════════════════════

def convert_batch(
    sas_dir: Path,
    output_dir: Path,
    *,
    recursive: bool = True,
    excel: bool = True,
    verbose: bool = True,
) -> List[Dict]:
    """
    Converte tutti i file .sas in una directory (e sottodirectory).

    La struttura delle sottocartelle viene preservata nell'output:
        input  : estratto/CodeTask-ABC/code.sas
        output : pyspark_output/CodeTask-ABC/code.py

    Returns:
        Lista di report per ogni file processato.
    """
    sas_dir    = Path(sas_dir).resolve()
    output_dir = Path(output_dir).resolve()

    pattern = "**/*.sas" if recursive else "*.sas"
    sas_files = sorted(sas_dir.glob(pattern))

    if not sas_files:
        print(f"[BATCH] Nessun file .sas trovato in: {sas_dir}")
        return []

    print(f"\n[BATCH] {len(sas_files)} file .sas trovati in {sas_dir}")
    print(f"        Output -> {output_dir}\n")

    report = []
    ok, ko = 0, 0

    for sas_path in sas_files:
        # Preserva struttura sottocartelle relativa alla sas_dir
        relative = sas_path.relative_to(sas_dir)
        file_output_dir = output_dir / relative.parent

        try:
            py_path = convert_sas_file(
                sas_path,
                file_output_dir,
                excel=excel,
                verbose=verbose,
            )
            status = "OK" if py_path else "SKIP"
            ok += 1 if py_path else 0
        except Exception as e:
            status = f"ERRORE: {e}"
            py_path = None
            ko += 1
            print(f"  [!] {sas_path.name}: {e}")

        report.append({
            "sas":    str(sas_path),
            "py":     str(py_path) if py_path else None,
            "status": status,
        })

    print(f"\n[BATCH] Completato: {ok} OK, {ko} errori su {len(sas_files)} file")
    return report


# ═══════════════════════════════════════════════════════════════════════
# RIEPILOGO CONSOLE
# ═══════════════════════════════════════════════════════════════════════

def _print_summary(py_path: Path) -> None:
    """Stampa un riepilogo leggibile del file .py generato."""
    code = py_path.read_text(encoding="utf-8")
    lines = code.splitlines()

    print(f"\n{'-'*60}")
    print(f"  FILE GENERATO : {py_path}")
    print(f"  Righe totali  : {len(lines)}")
    print(f"{'-'*60}")

    # Conta i TODO

    todo_count = sum(1 for l in lines if "# TODO" in l)
    auto_count = sum(1 for l in lines
                     if any(kw in l for kw in
                            ['.filter(', '.select(', '.orderBy(',
                             '.join(', '.union(', '.withColumn(']))
    print(f"  Conversioni automatiche : {auto_count} operazioni")
    print(f"  TODO manuali            : {todo_count}")
    print(f"{'-'*60}")

    # Preview prime 30 righe di codice (skip header commenti)
    code_lines = [l for l in lines if not l.startswith("#") and l.strip()]
    print("\n  Preview (prime 20 righe di codice):")
    for l in code_lines[:20]:
        print(f"    {l}")
    print(f"{'-'*60}\n")


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Converte file SAS in PySpark.\n"
            "Nomi output: <nome>.sas -> <nome>.py / <nome>.json / <nome>.xlsx"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Modalità singolo file
    parser.add_argument(
        "sas_file",
        nargs="?",
        help="File SAS da convertire (es. code.sas)",
    )

    # Modalità batch
    parser.add_argument(
        "--batch",
        metavar="DIR",
        help="Converti tutti i .sas nella directory specificata",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="In modalità batch: non cerca nelle sottocartelle",
    )

    # Output
    parser.add_argument(
        "--output-dir", "-o",
        default="./pyspark_output",
        help="Directory di output (default: ./pyspark_output)",
    )

    # Opzioni
    parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Non generare il report Excel",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Non salvare il JSON intermedio",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Output minimale",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Stampa riepilogo del file generato",
    )

    # Opzioni LLM
    llm_group = parser.add_argument_group(
        "LLM (opzionale)",
        "Integrazione con Ollama o Codestral su cloud privato per migliorare la conversione.\n"
        "Alternativa: crea llm_config.json nella root del progetto."
    )
    llm_group.add_argument(
        "--llm-strategy",
        metavar="STRATEGY",
        default=None,
        help="Strategia LLM: 'ollama', 'codestral', 'auto', 'none' (default: legge da llm_config.json)",
    )
    llm_group.add_argument(
        "--llm-host",
        metavar="URL",
        default=None,
        help="URL del server LLM (es. http://mon-cloud:11434 per Ollama, http://mon-cloud:8080/v1 per Codestral)",
    )
    llm_group.add_argument(
        "--llm-model",
        metavar="MODEL",
        default=None,
        help="Modello LLM (es. codestral, deepseek-coder:6.7b, codestral-latest)",
    )
    llm_group.add_argument(
        "--llm-api-key",
        metavar="KEY",
        default="not-needed",
        help="API key per il server LLM privato (default: 'not-needed')",
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    verbose    = not args.quiet

    # -- Backend LLM --------------------------------------------------
    llm_backend = None
    llm_strategy = args.llm_strategy  # None se non specificato → letto da config

    if llm_strategy != "none":
        # Costruisce i kwargs solo per i parametri esplicitamente passati
        llm_kwargs: dict = {}
        if llm_strategy:
            llm_kwargs["strategy"] = llm_strategy
        if args.llm_host and args.llm_strategy == "ollama":
            llm_kwargs["ollama_host"] = args.llm_host
        if args.llm_host and args.llm_strategy in ("codestral", None, "auto"):
            llm_kwargs["codestral_host"] = args.llm_host
        if args.llm_model and args.llm_strategy == "ollama":
            llm_kwargs["ollama_model"] = args.llm_model
        if args.llm_model and args.llm_strategy in ("codestral", None, "auto"):
            llm_kwargs["codestral_model"] = args.llm_model
        if args.llm_api_key and args.llm_api_key != "not-needed":
            llm_kwargs["codestral_api_key"] = args.llm_api_key

        llm_backend = create_llm_backend(**llm_kwargs)

    # -- Modalità BATCH -----------------------------------------------
    if args.batch:
        convert_batch(
            sas_dir    = Path(args.batch),
            output_dir = output_dir,
            recursive  = not args.no_recursive,
            excel      = not args.no_excel,
            verbose    = verbose,
        )
        return

    # -- Modalità SINGOLO FILE ----------------------------------------
    if not args.sas_file:
        parser.print_help()
        sys.exit(1)

    py_path = convert_sas_file(
        sas_path   = Path(args.sas_file),
        output_dir = output_dir,
        excel      = not args.no_excel,
        json_intermediate = not args.no_json,
        verbose    = verbose,
        llm_backend = llm_backend,
    )

    if py_path and args.summary:
        _print_summary(py_path)


if __name__ == "__main__":
    main()
