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
    GET  /docs/<f>  → documentazione HTML
    GET  /pipeline  → pagina "Come funziona" con architettura pipeline

Log:
    logs/converter.log   (rotazione 2 MB × 3 backup) — eventi applicativi
    logs/io_requests.log (rotazione 5 MB × 3 backup) — input/output per richiesta
    Seguire in tempo reale:  tail -f logs/converter.log
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

from flask import Flask, render_template, request, redirect, url_for, send_from_directory, Response, stream_with_context

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


# ═══════════════════════════════════════════════════════════════════════
# LOGGING UNIFICATO  →  logs/converter.log
# ═══════════════════════════════════════════════════════════════════════
# Un unico file raccoglie:
#   [APP]    — eventi applicativi (richieste, config, risultati)
#   [OLLAMA] — comunicazioni con Ollama (check, invio, risposta, errori)
#
# Per seguire in tempo reale:
#   tail -f logs/converter.log
# ──────────────────────────────────────────────────────────────────────

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(exist_ok=True)

_log_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Handler su file (rotazione) — unico file per tutti i logger
_fh = RotatingFileHandler(
    _LOG_DIR / "converter.log",
    maxBytes=2_000_000,
    backupCount=3,
    encoding="utf-8",
)
_fh.setFormatter(_log_fmt)

# Handler su stdout (visibile in Docker logs)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)


def _make_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    lg.setLevel(logging.DEBUG)
    lg.propagate = False
    lg.addHandler(_fh)
    lg.addHandler(_ch)
    return lg


# Logger applicativo (richieste, conversioni, errori)
log = _make_logger("converter.app")

# Logger Ollama (importato anche da llm_local.py tramite getLogger("ollama"))
ollama_logger = _make_logger("ollama")

# ── Logger I/O dedicato: registra input SAS e output PySpark per richiesta ──
_io_fmt = logging.Formatter("%(message)s")   # formato libero, già strutturato
_io_fh  = RotatingFileHandler(
    _LOG_DIR / "io_requests.log",
    maxBytes=5_000_000,
    backupCount=3,
    encoding="utf-8",
)
_io_fh.setFormatter(_io_fmt)

_io_log = logging.getLogger("converter.io")
_io_log.setLevel(logging.INFO)
_io_log.propagate = False
_io_log.addHandler(_io_fh)

# Contatore globale richieste (incrementato ad ogni POST /convert)
_request_counter = 0


# ═══════════════════════════════════════════════════════════════════════
# BACKEND LLM (Ollama locale, opzionale)
# ═══════════════════════════════════════════════════════════════════════
_OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_LLM_BACKEND = None


def _init_llm_backend():
    """Inizializza il backend LLM (Ollama) se disponibile."""
    global _LLM_BACKEND
    model = os.environ.get("OLLAMA_MODEL", "codestral")
    ollama_logger.info(
        f"[OLLAMA] Inizializzazione | host={_OLLAMA_HOST} | model={model}"
    )
    _LLM_BACKEND = create_llm_backend(
        strategy="ollama",
        ollama_host=_OLLAMA_HOST,
        ollama_model=model,
    )
    if _LLM_BACKEND:
        ollama_logger.info(f"[OLLAMA] Connesso e pronto → {_OLLAMA_HOST}")
    else:
        ollama_logger.warning(
            f"[OLLAMA] Non disponibile su {_OLLAMA_HOST} "
            f"— conversione solo con regole deterministiche"
        )


# Tenta la connessione all'avvio
log.info("[APP] ═══ Avvio SAS→PySpark Converter v%s ═══", _VERSION)
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
    """
    t_pipeline = time.perf_counter()

    # ── Pre-processore Livello 2 (opzionale) ────────────────────────
    pre_warnings = []
    if preprocess:
        log.info("[APP] Pre-processore Livello 2: ATTIVO | macro_vars=%s", macro_vars or {})
        pre = SASMacroPreprocessor(known_vars=macro_vars or {})
        sas_code_resolved = pre.process(sas_code)
        pre_warnings = pre.warnings
        log.info(
            "[APP] Pre-processore completato | righe SAS: %d→%d | warning: %d",
            len(sas_code.splitlines()),
            len(sas_code_resolved.splitlines()),
            len(pre_warnings),
        )
        if pre_warnings:
            for w in pre_warnings:
                log.debug("[APP]   warning pre-proc: %s", w)
    else:
        log.info("[APP] Pre-processore Livello 2: DISATTIVO")
        sas_code_resolved = sas_code

    # ── Scrivi su file temporaneo (il parser accetta path) ──────────
    with tempfile.NamedTemporaryFile(
        suffix=".sas", mode="w", delete=False, encoding="utf-8"
    ) as f:
        f.write(sas_code_resolved)
        tmp_path = Path(f.name)

    try:
        # 1. Parser gerarchico
        log.info("[APP] Parser SAS → blocchi...")
        blocks = parse_sas_blocks_tracked(tmp_path)
        log.info("[APP] Parser completato | blocchi trovati: %d", len(blocks))

        # 2. Codice PySpark completo (con LLM fallback se attivato)
        llm_backend = _LLM_BACKEND if use_llm else None
        if use_llm:
            if llm_backend:
                log.info("[APP] LLM Ollama: ATTIVO come fallback")
            else:
                log.warning("[APP] LLM Ollama richiesto ma NON disponibile — solo regole")
        else:
            log.info("[APP] LLM Ollama: DISATTIVO")

        log.info("[APP] Conversione blocchi → PySpark...")
        pyspark_code = convert_tree(blocks, llm_backend=llm_backend)

        # 3. Mappa conversione per blocco
        pyspark_map = convert_blocks_to_map(blocks, llm_backend=llm_backend)

        # 4. DataFrame blocchi con codice PySpark per blocco
        df = build_dataframe(blocks, pyspark_map=pyspark_map)

        # 5. Statistiche
        stats_df = build_stats(df)

        # 6. Regole di classificazione
        rules_df = build_regole_dataframe()

        # Conteggio TODO per il banner
        todo_count = pyspark_code.count("# TODO")
        py_lines   = len(pyspark_code.splitlines())

        elapsed_pipeline = time.perf_counter() - t_pipeline
        log.info(
            "[APP] Conversione completata | blocchi: %d | righe PySpark: %d "
            "| TODO: %d | tempo pipeline: %.2fs",
            len(blocks), py_lines, todo_count, elapsed_pipeline,
        )

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


def _log_io_request(
    req_num: int,
    sas_code: str,
    pyspark_code: str,
    use_llm: bool,
    elapsed_ms: int,
    block_count: int,
) -> None:
    """
    Scrive una voce leggibile su logs/io_requests.log con input SAS e output PySpark.
    Tronca a 500 chars con indicazione se il testo è più lungo.
    Formato:
        [TIMESTAMP] REQUEST #N | use_llm=X | Xms | X blocks
        INPUT (X chars):
          ...codice SAS...
        OUTPUT (X chars):
          ...codice PySpark...
        ---
    """
    _TRUNC = 500

    def _trunc(text: str) -> str:
        if len(text) <= _TRUNC:
            return text
        return text[:_TRUNC] + f"\n  ...[truncated — {len(text)} chars total]"

    sas_preview   = "\n".join(f"  {l}" for l in _trunc(sas_code).splitlines())
    py_preview    = "\n".join(f"  {l}" for l in _trunc(pyspark_code).splitlines())

    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    entry = (
        f"[{ts}] REQUEST #{req_num}"
        f" | use_llm={use_llm}"
        f" | {elapsed_ms}ms"
        f" | {block_count} block{'s' if block_count != 1 else ''}\n"
        f"INPUT ({len(sas_code)} chars):\n{sas_preview}\n"
        f"OUTPUT ({len(pyspark_code)} chars):\n{py_preview}\n"
        f"---"
    )
    _io_log.info(entry)


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
    log.info("[APP] GET / — pagina principale")
    return render_template("index.html", version=_VERSION)


@app.route("/convert", methods=["POST"])
def convert():
    """Esegue la conversione e mostra i risultati."""
    global _request_counter
    _request_counter += 1
    req_num = _request_counter

    sas_code = request.form.get("sas_code", "").strip()
    if not sas_code:
        log.warning("[APP] POST /convert — testo SAS vuoto, redirect a home")
        return redirect(url_for("index"))

    preprocess   = bool(request.form.get("preprocess"))
    use_llm      = bool(request.form.get("use_llm"))
    macro_vars_r = request.form.get("macro_vars", "").strip()
    macro_vars   = _parse_macro_vars(macro_vars_r) if macro_vars_r else {}

    client_ip    = request.remote_addr or "?"
    sas_lines    = len(sas_code.splitlines())
    sas_chars    = len(sas_code)

    log.info(
        "[APP] ── Nuova richiesta di conversione ──────────────────────────"
    )
    log.info(
        "[APP] client=%s | SAS: %d righe, %d chars",
        client_ip, sas_lines, sas_chars,
    )
    log.info(
        "[APP] config | preprocess=%s | use_llm=%s | macro_vars=%s",
        "ON" if preprocess else "OFF",
        "ON" if use_llm    else "OFF",
        macro_vars or "(nessuna)",
    )

    t0 = time.perf_counter()
    try:
        result = run_conversion(
            sas_code, preprocess=preprocess, macro_vars=macro_vars, use_llm=use_llm,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        log.error(
            "[APP] ERRORE durante la conversione dopo %.2fs: %s: %s",
            elapsed, type(exc).__name__, exc,
        )
        return render_template(
            "index.html",
            version=_VERSION,
            error=str(exc),
            sas_code=sas_code,
        )

    elapsed = time.perf_counter() - t0
    log.info(
        "[APP] Risposta pronta | tempo totale: %.2fs | "
        "blocchi: %d | TODO: %d",
        elapsed, result["block_count"], result["todo_count"],
    )
    log.info("[APP] ─────────────────────────────────────────────────────────")

    # Scrivi voce I/O su io_requests.log
    _log_io_request(
        req_num=req_num,
        sas_code=sas_code,
        pyspark_code=result["pyspark_code"],
        use_llm=use_llm,
        elapsed_ms=int(elapsed * 1000),
        block_count=result["block_count"],
    )

    # Converte DataFrame → liste di dict per Jinja2
    blocks_records = _df_to_records(result["blocks_df"])
    stats_records  = _df_to_records(result["stats_df"])
    rules_records  = _df_to_records(result["rules_df"])

    # Categorie uniche per i filtri dropdown
    categories = sorted(set(r.get("macro_categoria", "") for r in blocks_records))

    return render_template(
        "result.html",
        version=_VERSION,
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


@app.route("/stream_llm", methods=["POST"])
def stream_llm():
    """
    Endpoint SSE: converte un singolo blocco SAS via LLM in streaming.
    Accetta JSON body: {"sas_code": "...", "block_category": "..."}
    Ritorna text/event-stream con token progressivi.

    Formato SSE per ogni token:
        data: {"token": "...", "done": false}\n\n
    Quando finito:
        data: {"token": "", "done": true}\n\n

    Usato dalla UI quando use_llm=true per mostrare la risposta in tempo reale.
    """
    if _LLM_BACKEND is None:
        def _err():
            yield 'data: {"token": "# LLM non disponibile", "done": false}\n\n'
            yield 'data: {"token": "", "done": true}\n\n'
        return Response(stream_with_context(_err()), mimetype="text/event-stream")

    # Accetta sia JSON body che form
    if request.is_json:
        body         = request.get_json(silent=True) or {}
        sas_code     = body.get("sas_code", "").strip()
        block_cat    = body.get("block_category", "")
    else:
        sas_code     = request.form.get("sas_code", "").strip()
        block_cat    = request.form.get("block_category", "")

    if not sas_code:
        def _empty():
            yield 'data: {"token": "", "done": true}\n\n'
        return Response(stream_with_context(_empty()), mimetype="text/event-stream")

    log.info(
        "[APP] GET /stream_llm | %d chars SAS | categoria=%r",
        len(sas_code), block_cat,
    )

    # Verifica che il backend supporti lo streaming
    if not hasattr(_LLM_BACKEND, "convert_stream"):
        def _nosupport():
            yield 'data: {"token": "# Streaming non supportato da questo backend LLM", "done": false}\n\n'
            yield 'data: {"token": "", "done": true}\n\n'
        return Response(stream_with_context(_nosupport()), mimetype="text/event-stream")

    def _token_generator():
        for sse_json in _LLM_BACKEND.convert_stream(
            sas_code=sas_code,
            block_category=block_cat,
        ):
            yield f"data: {sse_json}\n\n"

    return Response(
        stream_with_context(_token_generator()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disabilita il buffering nginx/proxy
        },
    )


@app.route("/docs/<path:filename>")
def serve_docs(filename):
    """Serve i file HTML della documentazione da docs/."""
    log.debug("[APP] GET /docs/%s", filename)
    return send_from_directory("docs", filename)


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint per Azure / load balancer."""
    return {
        "status": "ok",
        "version": _VERSION,
        "ollama": _LLM_BACKEND is not None,
    }, 200


@app.route("/pipeline", methods=["GET"])
def pipeline():
    """Pagina 'Come funziona' — architettura della pipeline di conversione."""
    log.debug("[APP] GET /pipeline")
    llm_model = os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:0.5b")
    return render_template(
        "pipeline.html",
        version=_VERSION,
        llm_model=llm_model,
        ollama_ok=_LLM_BACKEND is not None,
    )


# ═══════════════════════════════════════════════════════════════════════
# AVVIO
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV", "production") == "development"
    log.info("[APP] Server Flask avviato | porta=%d | debug=%s", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug)
