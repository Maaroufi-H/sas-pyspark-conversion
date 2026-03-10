"""
webapp.py
=========
Interfaccia web Flask per il convertitore SAS → PySpark.

Avvio locale:
    pip install flask pandas openpyxl
    python webapp.py

Avvio Docker:
    docker-compose up -d

Route:
    GET  /          → form per incollare il codice SAS
    POST /convert   → esegue la conversione, mostra risultati in HTML
    GET  /health    → health check per Azure / load balancer
"""
from __future__ import annotations

import logging
import os
import re
import tempfile
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

from flask import Flask, render_template, request, redirect, url_for, send_from_directory

# ── Import pipeline di conversione ──────────────────────────────────
from trova_sas3_tracker import (
    parse_sas_blocks_tracked,
    build_dataframe,
    build_stats,
    build_regole_dataframe,
)
from convert_engine import convert_tree, convert_blocks_to_map
from sas_macro_preprocessor import SASMacroPreprocessor
from llm_local import create_llm_backend


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURAZIONE FLASK
# ═══════════════════════════════════════════════════════════════════════

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB max upload

_VERSION = "1.0"
_REPO    = "https://github.com/Maaroufi-H/sas-pyspark-conversion"


# ═══════════════════════════════════════════════════════════════════════
# LOGGING COMUNICAZIONE OLLAMA
# ═══════════════════════════════════════════════════════════════════════
# I log vengono scritti in logs/ollama_comms.log (rotazione a 2 MB, 3 backup)
# e anche su stdout per visibilità in Docker.

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(exist_ok=True)

ollama_logger = logging.getLogger("ollama")
ollama_logger.setLevel(logging.DEBUG)
ollama_logger.propagate = False  # evita duplicati nel root logger

_log_fmt = logging.Formatter("%(asctime)s [%(levelname)-5s] %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")

_fh = RotatingFileHandler(_LOG_DIR / "ollama_comms.log",
                           maxBytes=2_000_000, backupCount=3, encoding="utf-8")
_fh.setFormatter(_log_fmt)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)

ollama_logger.addHandler(_fh)
ollama_logger.addHandler(_ch)


# ═══════════════════════════════════════════════════════════════════════
# BACKEND LLM (Ollama locale, opzionale)
# ═══════════════════════════════════════════════════════════════════════
# Se la variabile d'ambiente OLLAMA_HOST è definita (es. dal docker-compose),
# tenta la connessione a Ollama all'avvio. Se Ollama non è disponibile,
# l'app funziona comunque con le sole regole deterministiche.
_OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_LLM_BACKEND = None

def _init_llm_backend():
    """Inizializza il backend LLM (Ollama) se disponibile."""
    global _LLM_BACKEND
    model = os.environ.get("OLLAMA_MODEL", "codestral")
    ollama_logger.info(f"[WEBAPP] Inizializzazione Ollama | host={_OLLAMA_HOST} | model={model}")
    _LLM_BACKEND = create_llm_backend(
        strategy="ollama",
        ollama_host=_OLLAMA_HOST,
        ollama_model=model,
    )
    if _LLM_BACKEND:
        ollama_logger.info(f"[WEBAPP] Ollama ATTIVO su {_OLLAMA_HOST}")
    else:
        ollama_logger.warning(f"[WEBAPP] Ollama NON disponibile su {_OLLAMA_HOST} — solo regole deterministiche")

# Tenta la connessione all'avvio
_init_llm_backend()


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE DI CONVERSIONE (in memoria)
# ═══════════════════════════════════════════════════════════════════════

def run_conversion(
    sas_code: str,
    preprocess: bool = False,
    macro_vars: Optional[Dict[str, str]] = None,
    use_llm: bool = False,
) -> dict:
    """
    Esegue la pipeline completa SAS → PySpark in memoria.

    1. (opzionale) Pre-processore macro Livello 2
    2. Parser gerarchico → blocks dict
    3. Motore conversione → codice PySpark completo
    4. Mappa conversione per blocco → pyspark_map {uid → code}
    5. DataFrame blocchi (16 colonne)
    6. DataFrame statistiche
    7. DataFrame regole classificazione

    use_llm: se True e Ollama è disponibile, usa il LLM come fallback
             per i blocchi non convertibili dalle regole deterministiche.

    Restituisce un dizionario con tutti i dati per il template HTML.
    """
    # Pre-processore Livello 2 (opzionale)
    pre_warnings = []
    if preprocess:
        pre = SASMacroPreprocessor(known_vars=macro_vars or {})
        sas_code_resolved = pre.process(sas_code)
        pre_warnings = pre.warnings
    else:
        sas_code_resolved = sas_code

    # Scrive su file temporaneo (il parser accetta path)
    with tempfile.NamedTemporaryFile(
        suffix=".sas", mode="w", delete=False, encoding="utf-8"
    ) as f:
        f.write(sas_code_resolved)
        tmp_path = Path(f.name)

    try:
        # 1. Parser gerarchico
        blocks = parse_sas_blocks_tracked(tmp_path)

        # 2. Codice PySpark completo (con LLM fallback se attivato)
        llm_backend = _LLM_BACKEND if use_llm else None
        pyspark_code = convert_tree(blocks, llm_backend=llm_backend)

        # 3. Mappa conversione per blocco (per colonna codice_pyspark nell'HTML)
        pyspark_map = convert_blocks_to_map(blocks, llm_backend=llm_backend)

        # 4. DataFrame blocchi con codice PySpark per blocco
        df = build_dataframe(blocks, pyspark_map=pyspark_map)

        # 5. Statistiche
        stats_df = build_stats(df)

        # 6. Regole di classificazione
        rules_df = build_regole_dataframe()

        # Conteggio TODO per il banner
        todo_count = pyspark_code.count("# TODO")

        return {
            "ok": True,
            "pyspark_code": pyspark_code,
            "blocks_df": df,
            "stats_df": stats_df,
            "rules_df": rules_df,
            "todo_count": todo_count,
            "block_count": len(blocks),
            "pre_warnings": pre_warnings,
            "sas_code_original": sas_code,
            "sas_code_resolved": sas_code_resolved if preprocess else None,
        }

    finally:
        tmp_path.unlink(missing_ok=True)


def _parse_macro_vars(raw: str) -> Dict[str, str]:
    """
    Converte stringa "nid=3 livAgregg=1 path=/data" → {"nid": "3", ...}
    """
    result = {}
    for token in raw.split():
        if "=" in token:
            k, v = token.split("=", 1)
            result[k.strip().lower()] = v.strip()
    return result


def _df_to_records(df) -> list:
    """Converte DataFrame pandas in lista di dict (per Jinja2)."""
    if df is None or df.empty:
        return []
    return df.reset_index(drop=True).to_dict(orient="records")


# ═══════════════════════════════════════════════════════════════════════
# ROUTE
# ═══════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def index():
    """Pagina principale con form di input SAS."""
    return render_template("index.html", version=_VERSION)


@app.route("/convert", methods=["POST"])
def convert():
    """Esegue la conversione e mostra i risultati."""
    sas_code = request.form.get("sas_code", "").strip()
    if not sas_code:
        return redirect(url_for("index"))

    preprocess   = bool(request.form.get("preprocess"))
    use_llm      = bool(request.form.get("use_llm"))
    macro_vars_r = request.form.get("macro_vars", "").strip()
    macro_vars   = _parse_macro_vars(macro_vars_r) if macro_vars_r else {}

    t0 = time.perf_counter()
    try:
        result = run_conversion(
            sas_code, preprocess=preprocess, macro_vars=macro_vars, use_llm=use_llm,
        )
    except Exception as exc:
        return render_template(
            "index.html",
            version=_VERSION,
            error=str(exc),
            sas_code=sas_code,
        )
    elapsed = time.perf_counter() - t0

    # Converte DataFrame → liste di dict per Jinja2
    blocks_records = _df_to_records(result["blocks_df"])
    stats_records  = _df_to_records(result["stats_df"])
    rules_records  = _df_to_records(result["rules_df"])

    # Categorie uniche per i filtri dropdown
    categories = sorted(set(r.get("macro_categoria", "") for r in blocks_records))

    return render_template(
        "result.html",
        version=_VERSION,
        repo=_REPO,
        elapsed=round(elapsed, 2),
        pyspark_code=result["pyspark_code"],
        todo_count=result["todo_count"],
        block_count=result["block_count"],
        pre_warnings=result["pre_warnings"],
        blocks=blocks_records,
        stats=stats_records,
        rules=rules_records,
        categories=categories,
        preprocess=preprocess,
        macro_vars_raw=macro_vars_r,
    )


@app.route("/docs/<path:filename>")
def serve_docs(filename):
    """Serve i file HTML della documentazione da docs/."""
    return send_from_directory("docs", filename)


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint per Azure / load balancer."""
    return {
        "status": "ok",
        "version": _VERSION,
        "ollama": _LLM_BACKEND is not None,
    }, 200


# ═══════════════════════════════════════════════════════════════════════
# AVVIO
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV", "production") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)
