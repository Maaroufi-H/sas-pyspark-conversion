"""
llm_local.py
============
Alternativa LLM 100% on-premise per la conversione SAS → PySpark.

PROBLEMA RISOLTO:
  Gli script SAS contengono dati sensibili dell'azienda
  (nomi di tabelle di business, logica proprietaria, colonne riservate).
  Inviarli a un LLM cloud (OpenAI, ecc.) è inaccettabile
  senza un accordo DPA esplicito dell'azienda.

DUE STRATEGIE PROPOSTE:
  ┌─────────────────────────────────────────────────────────────────┐
  │ Strategia A : LLM LOCALE via Ollama                            │
  │   - Il modello gira sul server/PC dell'azienda                 │
  │   - Nessun dato esce dalla rete                                │
  │   - Modelli consigliati: codellama:13b, deepseek-coder:6.7b    │
  │   - Qualità ≈ 70-80% dei migliori LLM cloud (sufficiente)      │
  ├─────────────────────────────────────────────────────────────────┤
  │ Strategia B : ANONIMIZZATORE + LLM Cloud                       │
  │   - Sostituisce nomi tabelle/colonne/valori con placeholder     │
  │   - Invia il codice anonimizzato al LLM cloud                  │
  │   - Ricostruisce il codice reale con i nomi veri               │
  │   - Il LLM non vede mai i nomi di business reali               │
  └─────────────────────────────────────────────────────────────────┘

Installazione Ollama:
  Windows : https://ollama.com/download
  Poi     : ollama pull codellama:13b
  Oppure  : ollama pull deepseek-coder:6.7b
"""
from __future__ import annotations

import logging
import re
import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Logger condiviso con webapp.py (configurato lì; qui è no-op se usato standalone)
_log = logging.getLogger("ollama")


# ═══════════════════════════════════════════════════════════════════════
# STRATEGIA A : LLM LOCALE VIA OLLAMA
# ═══════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT_SAS = """\
You are a SAS-to-PySpark expert. Convert the SAS code block to clean PySpark.

Rules:
- Use `spark` as the SparkSession variable (already available).
- Use `F.col()`, `F.expr()` for column expressions.
- RETAIN → use Window functions with `unboundedPreceding`.
- HASH OBJECT → use broadcast join or Python dict.
- FIRST./LAST. → use Window with `row_number()` or `lag()`.
- PROC SQL → use `spark.sql(...)` or DataFrame API.
- If conversion is impossible, generate a commented TODO block.
- Output ONLY the Python code, no markdown, no explanation.
"""


class OllamaConverter:
    """
    LLM fallback locale via Ollama (https://ollama.com).
    Tutte le chiamate rimangono sulla rete interna.

    Modelli consigliati in ordine di qualità/dimensione:
      - deepseek-coder:6.7b  (leggero, ottimo sul codice)
      - codellama:13b        (migliore, richiede ~10 GB RAM)
      - codellama:34b        (eccellente, richiede ~30 GB RAM)
      - mistral:7b           (equilibrato, buon compromesso)
    """

    def __init__(
        self,
        model: str = "deepseek-coder:6.7b",
        host: str = "http://localhost:11434",
        learning_db_path: str = "learning_db.jsonl",
        temperature: float = 0.1,   # basse pour reproductibilité
        num_predict: int = 2048,
    ):
        self.model     = model
        self.host      = host.rstrip("/")
        self.db_path   = Path(learning_db_path)
        self.temperature = temperature
        self.num_predict = num_predict
        self._examples: List[dict] = self._load_few_shots()

    # ── Verifica disponibilità ───────────────────────────────────────

    def is_available(self) -> bool:
        """Verifica che Ollama sia in esecuzione e che il modello sia disponibile."""
        url = f"{self.host}/api/tags"
        _log.debug(f"[Ollama] Check disponibilità → GET {url}")
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                data = json.loads(resp.read())
            models = [m["name"] for m in data.get("models", [])]
            found = any(self.model.split(":")[0] in m for m in models)
            _log.info(f"[Ollama] Modelli presenti: {models} | '{self.model}' trovato: {found}")
            return found
        except Exception as e:
            _log.warning(f"[Ollama] Non raggiungibile ({url}): {e}")
            return False

    # ── Conversione ─────────────────────────────────────────────────

    def convert(
        self,
        sas_code: str,
        parent_context: str = "",
        block_category: str = "",
    ) -> Optional[str]:
        """
        Invia il blocco SAS a Ollama e restituisce il codice PySpark.
        Restituisce None se Ollama non è disponibile.
        """
        if not self.is_available():
            _log.info("[Ollama] convert() annullato: Ollama non disponibile")
            return None

        prompt = self._build_prompt(sas_code, parent_context, block_category)

        _log.info(
            f"[Ollama] Invio richiesta | model={self.model} | "
            f"categoria={block_category!r} | SAS={len(sas_code)} chars"
        )

        payload = json.dumps({
            "model": self.model,
            "prompt": prompt,
            "system": _SYSTEM_PROMPT_SAS,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.num_predict,
            },
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.host}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read())
                response = result.get("response", "").strip()
            elapsed = time.perf_counter() - t0
            _log.info(
                f"[Ollama] Risposta ricevuta | {elapsed:.1f}s | "
                f"{len(response)} chars output"
            )
            return response
        except urllib.error.URLError as e:
            elapsed = time.perf_counter() - t0
            _log.error(f"[Ollama] Errore di rete dopo {elapsed:.1f}s: {e}")
            return None

    def _build_prompt(
        self,
        sas_code: str,
        parent_context: str,
        block_category: str,
    ) -> str:
        """Costruisce il prompt con i few-shot examples appresi."""
        parts = []

        # Few-shot examples (correzioni umane precedenti)
        if self._examples:
            parts.append("# Examples of previous corrections:\n")
            for ex in self._examples[-5:]:  # max 5 esempi
                parts.append(
                    f"## SAS input:\n```sas\n{ex['sas_input']}\n```\n"
                    f"## PySpark output:\n```python\n{ex['corrected']}\n```\n"
                )
            parts.append("---\n")

        # Contesto padre
        if parent_context:
            parts.append(f"# Parent context: {parent_context}\n")
        if block_category:
            parts.append(f"# Block type: {block_category}\n")

        # Codice da convertire
        parts.append(f"# Convert this SAS block:\n```sas\n{sas_code}\n```")

        return "\n".join(parts)

    # ── Apprendimento ────────────────────────────────────────────────

    def record_correction(
        self,
        sas_code: str,
        generated_py: str,
        corrected_py: str,
        block_category: str = "",
    ) -> None:
        """
        Registra una correzione manuale dello sviluppatore.
        Queste coppie vengono usate come few-shot nelle chiamate successive.

        Utilizzo:
            converter.record_correction(
                sas_code    = "data x; set y; where z > 0; run;",
                generated_py= "x = spark.table('y')  # TODO",
                corrected_py= "x = spark.table('y').filter(F.col('z') > 0)",
            )
        """
        entry = {
            "sas_input":      sas_code,
            "generated":      generated_py,
            "corrected":      corrected_py,
            "block_category": block_category,
            "source":         "human_correction",
        }
        with open(self.db_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        self._examples = self._load_few_shots()
        print(f"[Learning] Correzione registrata. "
              f"Totale esempi: {len(self._examples)}")

    def _load_few_shots(self) -> List[dict]:
        """Carica gli esempi corretti dal file JSONL."""
        if not self.db_path.exists():
            return []
        examples = []
        with open(self.db_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("source") == "human_correction":
                        examples.append(entry)
                except json.JSONDecodeError:
                    pass
        return examples

    def export_finetuning_dataset(self, output_path: str) -> None:
        """
        Esporta le correzioni in formato JSONL per il fine-tuning futuro.
        Formato compatibile con Unsloth / HuggingFace Trainer.
        """
        data = self._load_few_shots()
        with open(output_path, "w", encoding="utf-8") as f:
            for entry in data:
                ft_entry = {
                    "instruction": _SYSTEM_PROMPT_SAS,
                    "input": entry["sas_input"],
                    "output": entry["corrected"],
                }
                f.write(json.dumps(ft_entry, ensure_ascii=False) + "\n")
        print(f"Dataset fine-tuning esportato: {output_path} "
              f"({len(data)} esempi)")


# ═══════════════════════════════════════════════════════════════════════
# STRATEGIA B : ANONIMIZZATORE SAS
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class AnonymizationMap:
    """Mappa di corrispondenza tra nomi reali e placeholder."""
    tables:  Dict[str, str] = field(default_factory=dict)   # real → placeholder
    columns: Dict[str, str] = field(default_factory=dict)
    strings: Dict[str, str] = field(default_factory=dict)   # valori letterali
    macrovars: Dict[str, str] = field(default_factory=dict)
    # inverse maps
    _inv_tables:  Dict[str, str] = field(default_factory=dict)
    _inv_columns: Dict[str, str] = field(default_factory=dict)
    _inv_strings: Dict[str, str] = field(default_factory=dict)
    _inv_macrovars: Dict[str, str] = field(default_factory=dict)


class SASAnonymizer:
    """
    Sostituisce i nomi di business sensibili con placeholder neutri
    prima di inviare il codice a un LLM esterno.

    Esempio:
        SAS reale    : data result.register; set poste_op.clienti; ...
        Anonimizzato : data TABLE_001; set TABLE_002; ...
        Dopo LLM     : TABLE_001 = spark.table("TABLE_002").filter(...)
        Ricostituito : result_register = spark.table("poste_op_clienti").filter(...)
    """

    # Pattern per rilevare i nomi SAS sensibili
    _RE_DATASET = re.compile(
        r'\b(?:data|set|merge|from|base\s*=|out\s*=|data\s*=)'
        r'\s+((?:\w+\.)?\w+)',
        re.I
    )
    _RE_MACROVAR = re.compile(r'&(\w+)\.?')
    _RE_STRING   = re.compile(r'"([^"]{4,})"')   # stringhe > 3 caratteri

    def __init__(self, preserve_keywords: bool = True):
        """
        preserve_keywords : se True, non anonimizza parole SAS standard
                            (result, work, sashelp, ecc.)
        """
        self._preserve = {
            'work', 'sashelp', 'sasuser', 'result', 'temp',
            'select', 'where', 'from', 'group', 'order', 'by',
            'inner', 'left', 'right', 'outer', 'join', 'on',
        } if preserve_keywords else set()
        self._tbl_counter = 0
        self._col_counter = 0
        self._str_counter = 0
        self._var_counter = 0

    def anonymize(self, sas_code: str) -> Tuple[str, AnonymizationMap]:
        """
        Anonimizza il codice SAS e ritorna (codice_anonimo, mappa).
        La mappa serve per ri-sostituire i nomi reali nel codice generato.
        """
        amap = AnonymizationMap()
        code = sas_code

        # ── 1. Macro variables (&var.) ─────────────────────────────
        for m in self._RE_MACROVAR.finditer(code):
            real = m.group(1)
            if real.lower() in self._preserve:
                continue
            if real not in amap.macrovars:
                ph = f"MVAR_{self._var_counter:03d}"
                self._var_counter += 1
                amap.macrovars[real] = ph
                amap._inv_macrovars[ph] = real

        for real, ph in amap.macrovars.items():
            code = re.sub(r'&' + re.escape(real) + r'\.?', f'&{ph}.', code)

        # ── 2. Nomi dataset (lib.dataset o solo dataset) ───────────
        for m in self._RE_DATASET.finditer(code):
            raw = m.group(1).strip().rstrip(';').strip()
            parts = raw.split(".")
            for part in parts:
                p = part.strip()
                if not p or p.lower() in self._preserve:
                    continue
                if p not in amap.tables:
                    ph = f"TABLE_{self._tbl_counter:03d}"
                    self._tbl_counter += 1
                    amap.tables[p] = ph
                    amap._inv_tables[ph] = p

        # Sostituisce i nomi (parola intera, case-insensitive)
        for real, ph in sorted(amap.tables.items(), key=lambda x: -len(x[0])):
            code = re.sub(r'\b' + re.escape(real) + r'\b', ph, code, flags=re.I)

        # ── 3. Stringhe letterali sensibili ───────────────────────
        for m in self._RE_STRING.finditer(code):
            val = m.group(1)
            # Non anonimizza valori tecnici corti o date
            if re.match(r'^[\d/\-:.]+$', val) or len(val) < 5:
                continue
            if val not in amap.strings:
                ph = f"STR_{self._str_counter:03d}"
                self._str_counter += 1
                amap.strings[val] = ph
                amap._inv_strings[ph] = val

        for val, ph in sorted(amap.strings.items(), key=lambda x: -len(x[0])):
            code = code.replace(f'"{val}"', f'"{ph}"')

        return code, amap

    def deanonymize(self, py_code: str, amap: AnonymizationMap) -> str:
        """
        Riapplica i nomi reali al codice PySpark generato dal LLM.
        """
        code = py_code

        # Tabelle (cerca il placeholder come token Python)
        for ph, real in sorted(
            amap._inv_tables.items(), key=lambda x: -len(x[0])
        ):
            # In Python, il nome potrebbe essere in string o come variabile
            py_var = re.sub(r'[.\-]', '_', real.lower())  # real → py_varname
            code = re.sub(
                r'\b' + re.escape(ph) + r'\b',
                py_var,
                code,
                flags=re.I
            )
            # Anche nelle stringhe Spark (saveAsTable, tableExists)
            code = code.replace(f'"{ph}"', f'"{real}"')
            code = code.replace(f"'{ph}'", f"'{real}'")

        # Stringhe
        for ph, real in amap._inv_strings.items():
            code = code.replace(f'"{ph}"', f'"{real}"')
            code = code.replace(f"'{ph}'", f"'{real}'")

        # Macro vars
        for ph, real in amap._inv_macrovars.items():
            code = code.replace(f'&{ph}.', f'&{real}.')
            code = code.replace(ph, real)

        return code


# ═══════════════════════════════════════════════════════════════════════
# CONVERTITORE IBRIDO (Anonimizzatore + LLM Cloud)
# ═══════════════════════════════════════════════════════════════════════

class AnonymizedCloudConverter:
    """
    Utilizza un LLM cloud (API Anthropic, ecc.) MA
    anonimizza il codice SAS PRIMA dell'invio.

    Il LLM cloud non vede mai i nomi di business reali.

    Prerequisito: pip install anthropic
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-opus-4-6",
        learning_db_path: str = "learning_db_cloud.jsonl",
    ):
        self.api_key   = api_key
        self.model     = model
        self.anonymizer = SASAnonymizer()
        self.db_path   = Path(learning_db_path)

    def convert(self, sas_code: str, parent_context: str = "") -> Optional[str]:
        """
        1. Anonimizza il codice SAS
        2. Invia al LLM cloud (mai i nomi reali)
        3. Rianonimizza il PySpark generato
        """
        try:
            import anthropic
        except ImportError:
            print("[AnonymizedCloud] anthropic non installato: pip install anthropic")
            return None

        # Step 1: Anonimizzazione
        anon_code, amap = self.anonymizer.anonymize(sas_code)

        # Step 2: Invio al LLM (solo codice anonimizzato)
        client = anthropic.Anthropic(api_key=self.api_key)
        msg = client.messages.create(
            model=self.model,
            max_tokens=2048,
            system=_SYSTEM_PROMPT_SAS,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Context: {parent_context}\n"
                        f"Convert this SAS block:\n```sas\n{anon_code}\n```"
                    ),
                },
            ],
        )
        anon_py = msg.content[0].text.strip()

        # Step 3: Restituzione dei nomi reali
        real_py = self.anonymizer.deanonymize(anon_py, amap)
        return real_py


# ═══════════════════════════════════════════════════════════════════════
# FACTORY – SCEGLIE AUTOMATICAMENTE LA STRATEGIA
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# STRATEGIA C : CODESTRAL / MISTRAL API (OpenAI-compatible endpoint)
# ═══════════════════════════════════════════════════════════════════════

class CodestralConverter:
    """
    Convertitore via API OpenAI-compatible (Codestral, Mistral, vLLM, llama.cpp).
    Funziona con qualsiasi server che espone /v1/chat/completions.

    Esempi di endpoint:
      - Codestral cloud Mistral  : https://codestral.mistral.ai/v1
      - Cloud privato vLLM       : http://mon-serveur:8080/v1
      - Cloud privato llama.cpp  : http://mon-serveur:8081/v1
      - Ollama (OpenAI compat)   : http://mon-serveur:11434/v1

    Modelli Codestral consigliati:
      - codestral-latest         (migliore per il codice, ~22B)
      - mistral-large-latest     (generico ma potente)
      - open-codestral-mamba     (leggero, veloce)
    """

    def __init__(
        self,
        host: str = "http://localhost:8080/v1",
        api_key: str = "not-needed",          # molti server privati non richiedono auth
        model: str = "codestral-latest",
        learning_db_path: str = "learning_db.jsonl",
        temperature: float = 0.1,
        max_tokens: int = 2048,
        timeout: int = 120,
    ):
        self.host        = host.rstrip("/")
        self.api_key     = api_key
        self.model       = model
        self.db_path     = Path(learning_db_path)
        self.temperature = temperature
        self.max_tokens  = max_tokens
        self.timeout     = timeout
        self._examples: List[dict] = self._load_few_shots()

    def is_available(self) -> bool:
        """Verifica che il server risponda (GET /v1/models o ping base)."""
        try:
            url = f"{self.host}/models"
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def convert(
        self,
        sas_code: str,
        parent_context: str = "",
        block_category: str = "",
    ) -> Optional[str]:
        """Invia il blocco SAS al modello e restituisce il PySpark."""
        messages = self._build_messages(sas_code, parent_context, block_category)

        payload = json.dumps({
            "model":       self.model,
            "messages":    messages,
            "temperature": self.temperature,
            "max_tokens":  self.max_tokens,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.host}/chat/completions",
            data=payload,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                result = json.loads(resp.read())
                text = result["choices"][0]["message"]["content"].strip()
                # Rimuove eventuali blocchi markdown ```python ... ```
                text = re.sub(r"^```(?:python)?\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
                return text.strip()
        except urllib.error.URLError as e:
            print(f"[CodestralConverter] Errore di rete: {e}")
            return None

    def _build_messages(
        self,
        sas_code: str,
        parent_context: str,
        block_category: str,
    ) -> List[dict]:
        """Costruisce la lista di messaggi con few-shot examples."""
        msgs: List[dict] = [{"role": "system", "content": _SYSTEM_PROMPT_SAS}]

        # Few-shot examples dalle correzioni umane
        for ex in self._examples[-4:]:
            msgs.append({"role": "user",      "content": f"```sas\n{ex['sas_input']}\n```"})
            msgs.append({"role": "assistant", "content": ex["corrected"]})

        # Richiesta corrente
        user_content = ""
        if parent_context:
            user_content += f"Context: {parent_context}\n"
        if block_category:
            user_content += f"Block type: {block_category}\n"
        user_content += f"```sas\n{sas_code}\n```"
        msgs.append({"role": "user", "content": user_content})
        return msgs

    def record_correction(
        self,
        sas_code: str,
        generated_py: str,
        corrected_py: str,
        block_category: str = "",
    ) -> None:
        """Registra una correzione manuale per il few-shot learning."""
        entry = {
            "sas_input":      sas_code,
            "generated":      generated_py,
            "corrected":      corrected_py,
            "block_category": block_category,
            "source":         "human_correction",
        }
        with open(self.db_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._examples = self._load_few_shots()
        print(f"[Learning] Correzione registrata. Totale esempi: {len(self._examples)}")

    def _load_few_shots(self) -> List[dict]:
        if not self.db_path.exists():
            return []
        examples = []
        with open(self.db_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("source") == "human_correction":
                        examples.append(entry)
                except json.JSONDecodeError:
                    pass
        return examples

    def export_finetuning_dataset(self, output_path: str) -> None:
        """Esporta le correzioni in formato JSONL per il fine-tuning."""
        data = self._load_few_shots()
        with open(output_path, "w", encoding="utf-8") as f:
            for entry in data:
                ft_entry = {
                    "instruction": _SYSTEM_PROMPT_SAS,
                    "input":  entry["sas_input"],
                    "output": entry["corrected"],
                }
                f.write(json.dumps(ft_entry, ensure_ascii=False) + "\n")
        print(f"Dataset fine-tuning esportato: {output_path} ({len(data)} esempi)")


# ═══════════════════════════════════════════════════════════════════════
# CARICAMENTO DA FILE DI CONFIGURAZIONE
# ═══════════════════════════════════════════════════════════════════════

def load_config(config_path: str = "llm_config.json") -> dict:
    """
    Carica la configurazione LLM da llm_config.json.
    Restituisce un dict vuoto se il file non esiste.
    """
    p = Path(config_path)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def create_llm_backend(
    strategy: str = "auto",
    ollama_model: str = "codestral",
    ollama_host: str = "http://localhost:11434",
    codestral_host: str = "http://localhost:8080/v1",
    codestral_api_key: str = "not-needed",
    codestral_model: str = "codestral-latest",
    cloud_api_key: str = "",
    cloud_model: str = "claude-opus-4-6",
    learning_db: str = "learning_db.jsonl",
    config_path: str = "llm_config.json",
):
    """
    Crea il backend LLM secondo la strategia scelta.
    Se esiste llm_config.json, i parametri vengono letti da li'
    (i parametri espliciti hanno la precedenza sul file).

    strategy:
      "ollama"     → Ollama locale o su cloud privato (API /api/generate)
      "codestral"  → API OpenAI-compatible (Codestral, Mistral, vLLM, llama.cpp)
      "anon"       → Anonimizzatore + LLM cloud pubblico (OpenAI ecc.)
      "auto"       → prova ollama, poi codestral, poi solo regole
      "none"       → nessun LLM (solo regole deterministiche)

    Esempio con cloud privato Ollama:
        backend = create_llm_backend(
            strategy="ollama",
            ollama_host="http://mon-serveur-prive:11434",
            ollama_model="codestral",
        )

    Esempio con Codestral su cloud privato:
        backend = create_llm_backend(
            strategy="codestral",
            codestral_host="http://mon-cloud:8080/v1",
            codestral_model="codestral-latest",
        )
    """
    # Legge configurazione da file (i parametri espliciti hanno precedenza)
    cfg = load_config(config_path)

    if not strategy or strategy == "auto":
        strategy = cfg.get("strategy", "auto")

    ollama_host      = cfg.get("ollama", {}).get("host",  ollama_host)
    ollama_model     = cfg.get("ollama", {}).get("model", ollama_model)
    codestral_host   = cfg.get("codestral", {}).get("host",    codestral_host)
    codestral_model  = cfg.get("codestral", {}).get("model",   codestral_model)
    codestral_api_key= cfg.get("codestral", {}).get("api_key", codestral_api_key)

    if strategy == "none":
        print("[LLM] LLM disabilitato: solo regole deterministiche.")
        return None

    if strategy in ("ollama", "auto"):
        ollama = OllamaConverter(
            model=ollama_model,
            host=ollama_host,
            learning_db_path=learning_db,
        )
        if ollama.is_available():
            print(f"[LLM] Ollama OK → {ollama_host} | modello: {ollama_model}")
            return ollama
        elif strategy == "ollama":
            print(
                f"[LLM] ERRORE: Ollama non raggiungibile su {ollama_host}\n"
                f"  Verifica che Ollama sia in esecuzione e che il modello sia disponibile:\n"
                f"    ollama pull {ollama_model}\n"
                f"  Se e' su un cloud privato, verifica la porta e il firewall."
            )
            return None

    if strategy in ("codestral", "auto"):
        coder = CodestralConverter(
            host=codestral_host,
            api_key=codestral_api_key,
            model=codestral_model,
            learning_db_path=learning_db,
        )
        if coder.is_available():
            print(f"[LLM] Codestral OK → {codestral_host} | modello: {codestral_model}")
            return coder
        elif strategy == "codestral":
            print(
                f"[LLM] ERRORE: endpoint non raggiungibile su {codestral_host}\n"
                f"  Verifica che il server sia attivo e che {codestral_host}/models risponda."
            )
            return None
        else:
            print("[LLM] Nessun backend LLM disponibile → solo regole deterministiche.")
            return None

    if strategy == "anon":
        if not cloud_api_key:
            print("[LLM] La strategia 'anon' richiede cloud_api_key.")
            return None
        print("[LLM] Strategia anon → codice SAS mascherato prima dell'invio al cloud")
        return AnonymizedCloudConverter(
            api_key=cloud_api_key,
            model=cloud_model,
            learning_db_path=learning_db,
        )

    raise ValueError(
        f"Strategia sconosciuta: {strategy!r}. "
        f"Valori validi: 'ollama', 'codestral', 'anon', 'auto', 'none'"
    )


# ═══════════════════════════════════════════════════════════════════════
# CLI DI TEST
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Test LLM local SAS→PySpark")
    p.add_argument("--strategy",    default="auto",
                   choices=["local", "anon", "auto", "none"])
    p.add_argument("--model",       default="deepseek-coder:6.7b")
    p.add_argument("--host",        default="http://localhost:11434")
    p.add_argument("--sas-file",    default=None,
                   help="File SAS da convertire (test)")
    p.add_argument("--test-anon",   action="store_true",
                   help="Testa l'anonimizzatore senza chiamata LLM")
    args = p.parse_args()

    if args.test_anon:
        # Test dell'anonimizzatore da solo
        anon = SASAnonymizer()
        test_sas = """
data result.register;
    attrib computing_datetime length = 8 format = datetime26.;
    attrib fase length = $50;
    set poste_op.clienti_2024(where=(regione = "Lombardia"));
    fase = "INIZIO: CARICAMENTO DATI";
run;
        """
        anon_code, amap = anon.anonymize(test_sas)
        print("=== Codice anonimizzato ===")
        print(anon_code)
        print("\n=== Mappa tabelle ===")
        for real, ph in amap.tables.items():
            print(f"  {real!r:30s} → {ph}")
        print("\n=== Mappa stringhe ===")
        for real, ph in amap.strings.items():
            print(f"  {real!r:30s} → {ph}")
        # Test di de-anonimizzazione
        simulated_py = f"""
TABLE_000 = spark.table("TABLE_001").filter(F.col("regione") == "STR_000")
TABLE_000.write.saveAsTable("TABLE_000")
"""
        print("\n=== PySpark deanonimizzato ===")
        print(anon.deanonymize(simulated_py, amap))

    else:
        # Test del backend LLM
        backend = create_llm_backend(
            strategy=args.strategy,
            ollama_model=args.model,
            ollama_host=args.host,
        )

        if backend and args.sas_file:
            sas_code = Path(args.sas_file).read_text(encoding="utf-8")
            print("=== Conversione in corso ===")
            result = backend.convert(sas_code[:500])  # test su 500 caratteri
            if result:
                print(result)
            else:
                print("Conversione fallita o LLM non disponibile.")
        elif backend:
            # Test rapido con un DATA step semplice
            test = "data x; set y; where age > 18; run;"
            print(f"Test: {test}")
            result = backend.convert(test, "root level")
            print(result or "LLM non disponibile.")
        else:
            print("Nessun backend LLM attivo (normale se Ollama non è installato).")
            print("Regole deterministiche usate da sole.")
