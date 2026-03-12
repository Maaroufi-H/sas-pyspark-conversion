"""
convert_engine.py
=================
Motore di conversione SAS → PySpark.

Input  : dizionario di blocchi con gerarchia (da trova_sas3_tracker.py)
Output : script Python/PySpark completo come stringa

Architettura a 3 strati:
  Strato 1 – Orchestratore  : visita post-order dell'albero dei blocchi
  Strato 2 – Analizzatore   : estrazione della struttura interna di ogni blocco
  Strato 3 – Generatore     : produzione del codice PySpark

Autore  : generato da sas-converter-analyst
Date    : 2026-03-03
Version : 1.0
"""
from __future__ import annotations

import re
import json
import argparse
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════
# STRATO 1 – CONTESTO DI CONVERSIONE
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class ConversionContext:
    """
    Propagato dal padre al figlio durante la visita dell'albero.
    Contiene tutto ciò che il figlio deve sapere del suo contesto.
    """
    parent_type: str = "root"           # "root" | "macro" | "if" | "else"
    indent: int = 0                     # livello (1 livello = 4 spazi)
    macro_params: List[str] = field(default_factory=list)   # ex: ["flusso"]
    macro_vars: Dict[str, str] = field(default_factory=dict)   # "&var" → py_expr
    available_dfs: Dict[str, str] = field(default_factory=dict) # sas_name → py_var
    # Traccia le liste collezionate da blocchi INTO :var1-
    # chiave = nome base (es. "id_prodotto"), valore = nome variabile Python (es. "id_prodotto_list")
    into_lists: Dict[str, str] = field(default_factory=dict)
    function_name: Optional[str] = None  # nome della funzione Python corrente
    spark_session_var: str = "spark"     # nome della variabile SparkSession
    llm_backend: object = None           # OllamaConverter | CodestralConverter | None

    # ── helpers ──────────────────────────────────────────────────────

    def child(self, parent_type: str = None) -> "ConversionContext":
        """Crea un sotto-contesto con indentazione +1 (per i blocchi annidati)."""
        return ConversionContext(
            parent_type=parent_type or self.parent_type,
            indent=self.indent + 1,
            macro_params=list(self.macro_params),
            macro_vars=dict(self.macro_vars),
            available_dfs=dict(self.available_dfs),
            into_lists=dict(self.into_lists),
            function_name=self.function_name,
            spark_session_var=self.spark_session_var,
            llm_backend=self.llm_backend,
        )

    @property
    def pad(self) -> str:
        """Indentazione corrente come stringa."""
        return "    " * self.indent

    def register_df(self, sas_name: str, py_var: str) -> None:
        """Registra un dataset SAS → variabile Python nel contesto."""
        self.available_dfs[_sas_to_py_var(sas_name)] = py_var

    def register_into_list(self, base_name: str, list_var: str) -> None:
        """Registra una lista Python generata da INTO :base1- nel contesto.

        Permette ai blocchi successivi di riutilizzarla nelle clausole IN.
        Esempio: INTO :id_prodotto1-  →  base="id_prodotto", list_var="id_prodotto_list"
        """
        self.into_lists[base_name.lower()] = list_var

    def find_into_list_for_macro_block(self, macro_block: str) -> Optional[str]:
        """Cerca una lista Python che corrisponde ai pattern &&base&i nel blocco macro.

        Esempio: macro_block contiene '&&id_prodotto&i' → cerca 'id_prodotto' in into_lists
        Restituisce il nome della variabile lista se trovata, altrimenti None.
        """
        # Estrae i nomi base dai pattern &&nome&i  (doppio-ampersand con indice variabile)
        for m in re.finditer(r'&&(\w+?)(?:\d+)?&\w+', macro_block, re.I):
            base = m.group(1).lower().rstrip('_')
            if base in self.into_lists:
                return self.into_lists[base]
        # Cerca anche pattern &&nome1 (con numero letterale)
        for m in re.finditer(r'&&(\w+?)(\d+)', macro_block, re.I):
            base = m.group(1).lower().rstrip('_')
            if base in self.into_lists:
                return self.into_lists[base]
        return None

    def resolve_df(self, sas_name: str) -> str:
        """Risolve un nome di dataset SAS → variabile Python disponibile."""
        key = _sas_to_py_var(sas_name)
        return self.available_dfs.get(key, key)

    def resolve_macro_var(self, text: str) -> str:
        """Sostituisce i riferimenti macro &var. con il loro equivalente Python.

        Gestisce due casi:
          - singolo ampersand  : &var   → valore diretto
          - doppio  ampersand  : &&var&i → risoluzione in due passate (come SAS)
              Passata 1: &&name → &name  (rimozione di un &)
              Passata 2: &name  → valore Python

        Esempio SAS:  &&id_prodotto&i  (con i=2)
          Passata 1 → &id_prodotto&i
          Passata 2 → &id_prodotto2  → id_prodotto_list[1]
        """
        def _replace_single(m: re.Match) -> str:
            var = m.group(1).lower()
            if var in self.macro_vars:
                return self.macro_vars[var]
            if var in [p.lower() for p in self.macro_params]:
                return var  # parametro di funzione Python → stesso nome
            return f"TODO_MACRO_VAR_{var.upper()}"

        # Passata 1: &&name → &name  (il doppio-& SAS richiede due risoluzioni)
        # Converte &&word in un placeholder temporaneo per evitare doppia sostituzione
        text = re.sub(r'&&(\w+)', r'__DBLAMP__\1', text)
        # Passata 2: sostituisce &var singolo
        text = re.sub(r'&(\w+)\.?', _replace_single, text)
        # Ripristina i placeholder __DBLAMP__x → vengono ora risolti come &x singolo
        text = re.sub(r'__DBLAMP__(\w+)', r'&\1', text)
        # Terza passata: risolve i &x rimasti dal doppio-ampersand
        text = re.sub(r'&(\w+)\.?', _replace_single, text)
        return text


# ═══════════════════════════════════════════════════════════════════════
# STRATO 2 – STRUTTURE INTERMEDIE (risultato degli analizzatori)
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class DataStepInfo:
    """Struttura intermedia dopo l'analisi interna di un DATA step."""
    output_names: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)           # SET source(s)
    merge_sources: List[str] = field(default_factory=list)     # MERGE sources
    by_keys: List[str] = field(default_factory=list)           # BY keys
    where_clause: Optional[str] = None
    keep_cols: List[str] = field(default_factory=list)         # dataset option keep=
    drop_cols: List[str] = field(default_factory=list)         # dataset option drop=
    calc_columns: List[Tuple[str, str]] = field(default_factory=list)  # (name, sas_expr)
    schema_fields: List[Tuple[str, str]] = field(default_factory=list) # (name, sas_type)
    has_retain: bool = False
    has_first_last: bool = False
    has_hash: bool = False
    has_array: bool = False
    has_stop: bool = False
    has_eg_macro: bool = False                                 # %_eg_WhereParam etc.
    multi_output: List[Tuple[str, str]] = field(default_factory=list)  # [(condition, ds_name)]
    merge_in_flags: Dict[str, str] = field(default_factory=dict)       # {ds: "a"|"b"}


@dataclass
class ProcSqlInfo:
    """Struttura intermedia dopo l'analisi interna di un PROC SQL."""
    select_clause: Optional[str] = None
    from_clause: Optional[str] = None
    where_clause: Optional[str] = None
    group_by: Optional[str] = None
    having: Optional[str] = None
    order_by: Optional[str] = None
    into_var: Optional[str] = None          # INTO :macrovar  (scalare)
    into_var_is_list: bool = False          # True se INTO :var1-  (lista SAS, es. :id_prodotto1-)
    create_table: Optional[str] = None      # CREATE TABLE name AS
    joins: List[Tuple[str, str, str]] = field(default_factory=list)  # (type, table, condition)
    has_dynamic_sql: bool = False           # %str %sysfunc etc.
    macro_in_lists: List[Tuple[str, str]] = field(default_factory=list)  # (col, raw_macro_block)
    raw_sql: str = ""


@dataclass
class ProcSortInfo:
    """Struttura intermedia dopo l'analisi interna di un PROC SORT."""
    data_in: Optional[str] = None
    data_out: Optional[str] = None
    by_keys: List[str] = field(default_factory=list)
    nodupkey: bool = False
    descending_keys: List[str] = field(default_factory=list)


@dataclass
class MacroDefInfo:
    """Struttura intermedia di una definizione di macro."""
    name: str = ""
    params: List[str] = field(default_factory=list)


@dataclass
class IfBlockInfo:
    """Struttura intermedia di un blocco %IF/%THEN %DO."""
    condition_sas: str = ""
    condition_py: str = ""


# ═══════════════════════════════════════════════════════════════════════
# HELPER GENERALI
# ═══════════════════════════════════════════════════════════════════════

def _sas_to_py_var(sas_name: str) -> str:
    """Converte un nome di dataset SAS in nome di variabile Python valido."""
    s = sas_name.lower().strip()
    s = re.sub(r'[.\-]', '_', s)   # work.myds → work_myds
    s = re.sub(r'[^\w]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    if s and s[0].isdigit():
        s = "ds_" + s
    return s or "unknown_ds"


def _sas_type_to_spark(sas_type: str) -> str:
    """Converte un tipo SAS in tipo PySpark."""
    t = sas_type.strip().upper()
    if t.startswith("$"):
        return "StringType()"
    if t in ("8", "BEST", "BEST32", "BEST32."):
        return "DoubleType()"
    if re.match(r'^\d+$', t):
        return "LongType()"
    if "DATE" in t:
        return "DateType()"
    if "DATETIME" in t or "DT" in t:
        return "TimestampType()"
    return "StringType()"  # fallback


def _sas_expr_to_py(expr: str, ctx: ConversionContext) -> str:
    """
    Conversione di base di un'espressione SAS → espressione Python.
    Casi semplici: operatori, funzioni comuni, variabili macro.
    """
    e = ctx.resolve_macro_var(expr.strip())
    # operatori logici
    e = re.sub(r'\bEQ\b', '==', e, flags=re.I)
    e = re.sub(r'\bNE\b', '!=', e, flags=re.I)
    e = re.sub(r'\bGT\b', '>',  e, flags=re.I)
    e = re.sub(r'\bLT\b', '<',  e, flags=re.I)
    e = re.sub(r'\bGE\b', '>=', e, flags=re.I)
    e = re.sub(r'\bLE\b', '<=', e, flags=re.I)
    e = re.sub(r'\bAND\b', 'and', e, flags=re.I)
    e = re.sub(r'\bOR\b',  'or',  e, flags=re.I)
    e = re.sub(r'\bNOT\b', 'not', e, flags=re.I)
    # funzioni SAS comuni
    e = re.sub(r'\bsubstr\s*\(',  'substr(', e, flags=re.I)
    e = re.sub(r'\btrim\s*\(',    'trim(',   e, flags=re.I)
    e = re.sub(r'\bupcase\s*\(',  'upper(',  e, flags=re.I)
    e = re.sub(r'\blowcase\s*\(', 'lower(',  e, flags=re.I)
    e = re.sub(r'\bstrip\s*\(',   'strip(',  e, flags=re.I)
    e = re.sub(r'\bint\s*\(',     'int(',    e, flags=re.I)
    # SAS date/missing
    e = re.sub(r'\bNULL\b',    'None',  e, flags=re.I)
    e = re.sub(r'\b\.\b',      'None',  e)         # missing value SAS

    # SAS input(col, fmt) → cast
    def _input_to_cast(m: re.Match) -> str:
        col  = m.group(1).strip()
        fmt  = m.group(2).strip().upper()
        if re.match(r'^\d+\.?\d*$', fmt):          # 8. 4. 2. → double
            spark_t = "double"
        elif fmt.startswith('$'):                   # $20. → string
            spark_t = "string"
        elif 'DATE' in fmt:
            spark_t = "date"
        elif 'DATETIME' in fmt or 'DT' in fmt:
            spark_t = "timestamp"
        else:
            spark_t = "double"
        return f'F.col("{col}").cast("{spark_t}")'
    e = re.sub(r'\binput\s*\(\s*(\w+)\s*,\s*([^)]+)\)', _input_to_cast, e, flags=re.I)

    # SAS put(col, fmt) → cast string
    e = re.sub(r'\bput\s*\(\s*(\w+)\s*,[^)]+\)',
               lambda m: f'F.col("{m.group(1).strip()}").cast("string")', e, flags=re.I)
    return e


def _sas_macro_condition_to_py(cond_sas: str, ctx: ConversionContext) -> str:
    """
    Converte una condizione macro SAS in condizione Python.
    Es: %sysfunc(exist(result.register)) = 0  →  not spark.catalog.tableExists(...)
    """
    c = cond_sas.strip()

    # %sysfunc(exist(ds)) = 0  →  not spark.catalog.tableExists("ds")
    m = re.search(
        r'%sysfunc\s*\(\s*exist\s*\(\s*([^)]+)\s*\)\s*\)\s*=\s*0',
        c, re.I
    )
    if m:
        ds = m.group(1).strip()
        return f'not {ctx.spark_session_var}.catalog.tableExists("{ds}")'

    m = re.search(
        r'%sysfunc\s*\(\s*exist\s*\(\s*([^)]+)\s*\)\s*\)\s*(?:=\s*1|(?!\s*=\s*0))',
        c, re.I
    )
    if m:
        ds = m.group(1).strip()
        return f'{ctx.spark_session_var}.catalog.tableExists("{ds}")'

    # &syscc > 4  →  sys_cc > 4
    c = re.sub(r'&syscc\b', 'sys_cc', c, flags=re.I)

    # &var operatore valore
    c = ctx.resolve_macro_var(c)
    c = _sas_expr_to_py(c, ctx)

    if not c or c == cond_sas:
        return f"# TODO: condizione SAS: {cond_sas}\nTrue"
    return c


# ═══════════════════════════════════════════════════════════════════════
# STRATO 2 – ANALIZZATORI INTERNI
# ═══════════════════════════════════════════════════════════════════════

def _analyse_data_step(testo: str) -> DataStepInfo:
    """
    Analizza il contenuto interno di un DATA step e restituisce DataStepInfo.
    """
    info = DataStepInfo()
    txt = testo
    txt_upper = txt.upper()

    # ── Nomi di output (1ª riga: data name1 name2; ) ────────────────
    m = re.match(r'^\s*data\s+([^;]+);', txt, re.I)
    if m:
        raw_names = m.group(1).strip()
        info.output_names = [n.strip() for n in raw_names.split() if n.strip()]

    # ── Flag di complessità (ordine importante) ─────────────────────
    info.has_hash      = bool(re.search(r'\bdeclare\s+hash\b', txt, re.I))
    info.has_array     = bool(re.search(r'\barray\b', txt, re.I))
    info.has_retain    = bool(re.search(r'\bretain\b', txt, re.I))
    info.has_first_last= bool(re.search(r'\b(first|last)\.\w+', txt, re.I))
    info.has_stop      = bool(re.search(r'\bstop\s*;', txt, re.I))

    # ── Helper: estrae il contenuto tra parentesi bilanciate ────────
    def _balanced_parens(s: str, open_pos: int) -> str:
        depth, i = 0, open_pos
        start = None
        while i < len(s):
            if s[i] == '(':
                if depth == 0: start = i + 1
                depth += 1
            elif s[i] == ')':
                depth -= 1
                if depth == 0:
                    return s[start:i]
            i += 1
        return s[start:] if start is not None else ""

    # ── MERGE ───────────────────────────────────────────────────────
    m_merge = re.search(r'\bmerge\b\s+(.*?);', txt, re.I | re.S)
    if m_merge:
        # Prende solo la parte prima del primo spazio/newline dopo il dsname
        for token in m_merge.group(1).split():
            clean = re.sub(r'\(.*', '', token).strip()
            if clean:
                info.merge_sources.append(clean)
            flag = re.search(r'\(in\s*=\s*(\w+)\)', token, re.I)
            if flag and clean:
                info.merge_in_flags[clean] = flag.group(1)

    # ── SET (con gestione opzioni dataset: where= keep= drop=) ──────
    if not info.merge_sources:
        m_set = re.search(r'\bset\b\s+(.*?);', txt, re.I | re.S)
        if m_set:
            raw = m_set.group(1).strip()
            # Nome dataset = tutto prima del primo '(' o whitespace
            dsname = re.split(r'[\s(]', raw)[0].strip()
            if dsname:
                info.sources.append(dsname)

            # Opzioni dentro parentesi: where=(...) keep=... drop=...
            paren_open = raw.find('(')
            if paren_open != -1:
                opts = _balanced_parens(raw, paren_open)

                # where=(...) — supporta parentesi annidate
                m_wh = re.search(r'\bwhere\s*=\s*\(', opts, re.I)
                if m_wh:
                    where_start = opts.find('(', m_wh.start())
                    info.where_clause = _balanced_parens(opts, where_start)
                    # Macros EG specifiche → flag TODO
                    if re.search(r'%_eg_\w+', info.where_clause, re.I):
                        info.has_eg_macro = True

                # keep=col1 col2 ... (fino a prossima opzione o fine)
                m_keep = re.search(
                    r'\bkeep\s*=\s*([\w\s]+?)(?:\b(?:drop|where|rename|in)\s*=|$)',
                    opts, re.I | re.S
                )
                if m_keep:
                    info.keep_cols = [
                        c.strip() for c in m_keep.group(1).split()
                        if c.strip() and re.match(r'^[a-zA-Z_]\w*$', c.strip())
                    ]

                # drop=col1 col2 ...
                m_drop = re.search(
                    r'\bdrop\s*=\s*([\w\s]+?)(?:\b(?:keep|where|rename|in)\s*=|$)',
                    opts, re.I | re.S
                )
                if m_drop:
                    info.drop_cols = [
                        c.strip() for c in m_drop.group(1).split()
                        if c.strip() and re.match(r'^[a-zA-Z_]\w*$', c.strip())
                    ]

    # ── BY ──────────────────────────────────────────────────────────
    m_by = re.search(r'\bby\b\s+([^;]+);', txt, re.I)
    if m_by:
        info.by_keys = [k.strip() for k in m_by.group(1).split() if k.strip()]

    # ── WHERE ───────────────────────────────────────────────────────
    m_where = re.search(r'\bwhere\b\s+([^;]+);', txt, re.I)
    if m_where:
        info.where_clause = m_where.group(1).strip()

    # ── ATTRIB statements (una variabile per riga) ──────────────────
    # Sintassi: attrib varname length=$50 format=... label="...";
    # Ogni ATTRIB descrive UNA variabile → regex diretta su var + length
    _seen_attrib = set()
    for m_att in re.finditer(r'\battrib\b\s+(\w+)\b([^;]+);', txt, re.I):
        var_name = m_att.group(1).strip()
        opts     = m_att.group(2)
        m_type   = re.search(r'\blength\s*=\s*(\$?\d+[\w.]*)', opts, re.I)
        sas_type = m_type.group(1).strip() if m_type else "8"
        if var_name not in _seen_attrib:
            info.schema_fields.append((var_name, sas_type))
            _seen_attrib.add(var_name)

    # ── LENGTH statements (coppie posizionali nome/tipo) ─────────────
    # Sintassi: length var1 $50 var2 8 var3 $200;
    # Alterna: identificatore  tipo  identificatore  tipo ...
    # Tipo = $N  oppure  N (solo cifre) oppure formato SAS (datetime26.)
    _RE_SAS_TYPE = re.compile(r'^\$?\d+\.?$|^datetime\d*\.$|^date\d*\.$', re.I)
    _RE_SAS_IDENT = re.compile(r'^[a-zA-Z_]\w*$')
    for m_len in re.finditer(r'\blength\b\s+([^;]+);', txt, re.I):
        tokens = m_len.group(1).split()
        i = 0
        while i < len(tokens) - 1:
            name_tok = tokens[i]
            type_tok = tokens[i + 1]
            if _RE_SAS_IDENT.match(name_tok) and _RE_SAS_TYPE.match(type_tok):
                if name_tok not in _seen_attrib:   # non duplicare con ATTRIB
                    info.schema_fields.append((name_tok, type_tok))
                i += 2
            else:
                i += 1

    # ── Colonne calcolate: col = expr; ──────────────────────────────
    # Esclude le parole chiave SAS E le variabili già nello schema
    _skip_kw = {
        'data','set','merge','by','where','retain','array',
        'length','attrib','format','informat','label','output','stop',
        'run','if','else','then','do','end','call','put','input','cards',
        'datalines','keep','drop','rename','in','obs','firstobs','options',
    }
    _schema_vars = {v.lower() for v, _ in info.schema_fields}
    for m_calc in re.finditer(r'^\s*(\w+)\s*=\s*([^;]+);', txt, re.I | re.M):
        var  = m_calc.group(1).strip()
        expr = m_calc.group(2).strip()
        # Ignora le parole chiave e le assegnazioni di schema (format=, length=)
        if var.lower() in _skip_kw:
            continue
        # Ignora se la riga fa parte di un ATTRIB (preceduta da attrib)
        line_start = m_calc.start()
        preceding = txt[max(0, line_start - 10):line_start]
        if re.search(r'\battrib\b', preceding, re.I):
            continue
        # Ignora se è un valore di schema già analizzato
        if var.lower() in _schema_vars:
            continue
        info.calc_columns.append((var, expr))

    # ── OUTPUT multiples: if cond then output ds; ───────────────────
    for m_out in re.finditer(
        r'\bif\b\s+([^;]+)\s+\bthen\b\s+\boutput\b\s+(\w[\w.]*)\s*;',
        txt, re.I
    ):
        info.multi_output.append((m_out.group(1).strip(), m_out.group(2).strip()))

    return info


def _detect_macro_in_list_cols(sql_body: str) -> List[Tuple[str, str]]:
    """
    Rileva il pattern SAS: col IN ( %if ... %then %do; ... %end; )
    comunemente usato per generare liste dinamiche di valori in una WHERE.

    Restituisce lista di (nome_colonna, raw_macro_block).
    """
    results = []
    for m in re.finditer(r'\b(\w+)\s+IN\s*\(', sql_body, re.I):
        col = m.group(1)
        # Salta keyword SQL che non sono nomi di colonne
        if col.upper() in ('SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'NOT'):
            continue
        start = m.end() - 1  # posizione della '('
        depth = 0
        content_start = start + 1
        i = start
        while i < len(sql_body):
            if sql_body[i] == '(':
                depth += 1
            elif sql_body[i] == ')':
                depth -= 1
                if depth == 0:
                    content = sql_body[content_start:i]
                    if re.search(r'%if\b|%do\b', content, re.I):
                        results.append((col, content.strip()))
                    break
            i += 1
    return results


def _analyse_proc_sql(testo: str) -> ProcSqlInfo:
    """Analisi interna di un PROC SQL."""
    info = ProcSqlInfo()
    txt = testo

    # Estrae il corpo tra PROC SQL; ... QUIT;
    m_body = re.search(r'proc\s+sql[^;]*;(.*?)(?:quit|run)\s*;', txt, re.I | re.S)
    body = m_body.group(1).strip() if m_body else txt
    info.raw_sql = body

    # ── Rilevamento pattern IN (macro_loop) ─────────────────────────
    # Pattern comune: WHERE col IN ( %if &n=1 %then %do; val %end; %else %do; ... %end; )
    # Questi blocchi sono convertibili in PySpark con .isin(lista) + TODO per la risoluzione
    # delle variabili macro, e NON devono bloccare l'intera conversione.
    info.macro_in_lists = _detect_macro_in_list_cols(body)

    # Testo SQL senza i blocchi IN-macro (per valutare il dinamismo residuo)
    body_without_in_lists = body
    for _col, _block in info.macro_in_lists:
        # Rimuove il blocco macro dall'analisi del dinamismo residuo
        body_without_in_lists = body_without_in_lists.replace(_block, "_MACRO_IN_LIST_PLACEHOLDER_")

    # SQL davvero dinamico: %str %nrstr %sysfunc %eval call execute nel corpo SQL
    # oppure macro complesse (non solo IN-list) che impediscono la conversione automatica
    _truly_dynamic_patterns = (
        r'%str\b|%nrstr\b|%sysfunc\b|%eval\b|call\s+execute'
        r'|%if\b.*?%then\b|%do\b|%end\b'
    )
    info.has_dynamic_sql = bool(
        re.search(_truly_dynamic_patterns, body_without_in_lists, re.I | re.S)
    )

    # CREATE TABLE name AS
    m_ct = re.search(r'create\s+table\s+([\w.]+)\s+as', body, re.I)
    if m_ct:
        info.create_table = m_ct.group(1).strip()

    # SELECT
    m_sel = re.search(r'\bselect\b\s+(.*?)\s+\bfrom\b', body, re.I | re.S)
    if m_sel:
        info.select_clause = re.sub(r'\s+', ' ', m_sel.group(1)).strip()

    # FROM
    m_from = re.search(r'\bfrom\b\s+([\w.,\s]+?)(?:\bwhere\b|\bgroup\b|\border\b|\bhaving\b|$)', body, re.I | re.S)
    if m_from:
        info.from_clause = m_from.group(1).strip().rstrip(',')

    # WHERE
    m_where = re.search(r'\bwhere\b\s+(.*?)(?:\bgroup\b|\border\b|\bhaving\b|;|$)', body, re.I | re.S)
    if m_where:
        info.where_clause = re.sub(r'\s+', ' ', m_where.group(1)).strip().rstrip(';')

    # GROUP BY
    m_gb = re.search(r'\bgroup\s+by\b\s+(.*?)(?:\bhaving\b|\border\b|;|$)', body, re.I | re.S)
    if m_gb:
        info.group_by = re.sub(r'\s+', ' ', m_gb.group(1)).strip().rstrip(';')

    # ORDER BY
    m_ob = re.search(r'\border\s+by\b\s+(.*?)(?:;|$)', body, re.I | re.S)
    if m_ob:
        info.order_by = re.sub(r'\s+', ' ', m_ob.group(1)).strip().rstrip(';')

    # HAVING
    m_hav = re.search(r'\bhaving\b\s+(.*?)(?:\border\b|;|$)', body, re.I | re.S)
    if m_hav:
        info.having = re.sub(r'\s+', ' ', m_hav.group(1)).strip().rstrip(';')

    # INTO :var  (scalare)  oppure  INTO :var1-  (lista di variabili macro)
    # Il trattino finale nel pattern SAS indica una serie: :var1- crea :var1, :var2, ... :varN
    m_into = re.search(r'\binto\s+:(\w+?)(\d*)\s*(-)', body, re.I)
    if m_into:
        # Caso lista: into :id_prodotto1-  → base="id_prodotto", numerico=True
        base = re.sub(r'\d+$', '', m_into.group(1) + m_into.group(2))
        info.into_var = base
        info.into_var_is_list = True
    else:
        m_into_scalar = re.search(r'\binto\s+:(\w+)', body, re.I)
        if m_into_scalar:
            info.into_var = m_into_scalar.group(1).strip()
            info.into_var_is_list = False

    return info


def _analyse_proc_sort(testo: str) -> ProcSortInfo:
    """Analisi interna di un PROC SORT."""
    info = ProcSortInfo()
    txt = testo

    m_data = re.search(r'\bdata\s*=\s*([\w.]+)', txt, re.I)
    if m_data:
        info.data_in = m_data.group(1).strip()

    m_out = re.search(r'\bout\s*=\s*([\w.]+)', txt, re.I)
    if m_out:
        info.data_out = m_out.group(1).strip()
    else:
        info.data_out = info.data_in  # in-place sort

    info.nodupkey = bool(re.search(r'\bnodupkey\b', txt, re.I))

    m_by = re.search(r'\bby\b\s+([^;]+);', txt, re.I)
    if m_by:
        for tok in m_by.group(1).split():
            if tok.upper() == 'DESCENDING':
                continue
            info.by_keys.append(tok.strip())
        # descending
        for m_desc in re.finditer(r'\bDESCENDING\s+(\w+)', m_by.group(1), re.I):
            info.descending_keys.append(m_desc.group(1))

    return info


def _analyse_macro_def(testo: str) -> MacroDefInfo:
    """Analizza la firma di una definizione di macro."""
    info = MacroDefInfo()
    m = re.match(r'^\s*%macro\s+(\w+)\s*(?:\(([^)]*)\))?\s*;', testo, re.I)
    if m:
        info.name = m.group(1).strip()
        raw_params = m.group(2) or ""
        info.params = [
            p.split('=')[0].strip()
            for p in raw_params.split(',')
            if p.strip()
        ]
    return info


def _analyse_if_block(testo: str) -> IfBlockInfo:
    """Analizza la condizione di un blocco %IF/%THEN %DO."""
    info = IfBlockInfo()
    m = re.match(r'^\s*%if\b\s*(.*?)\s*%then\b', testo, re.I | re.S)
    if m:
        info.condition_sas = m.group(1).strip()
    return info


# ═══════════════════════════════════════════════════════════════════════
# STRATO 3 – GENERATORI DI CODICE PYSPARK
# ═══════════════════════════════════════════════════════════════════════

_PYSPARK_HEADER = """\
# =============================================================
# Script generato automaticamente da convert_engine.py
# SAS → PySpark  |  2026-03-03
# Richiede : PySpark, delta (opzionale)
# =============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, DateType, TimestampType
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sas_converted").getOrCreate()
"""


def _todo_block(blk: dict, ctx: ConversionContext, reason: str) -> str:
    """
    Genera un commento TODO per i blocchi non convertibili dalle regole.
    Se ctx.llm_backend e' disponibile, tenta una conversione via LLM prima
    di emettere il TODO — il blocco LLM viene marcato con un commento
    '[LLM-generated]' per distinguerlo dal codice deterministico.
    """
    pad = ctx.pad
    sas_code = blk.get("testo", "")

    # ── Tentativo LLM ──────────────────────────────────────────────
    if ctx.llm_backend is not None and sas_code.strip():
        category = blk.get("macro_categoria", "")
        context_hint = ctx.function_name or ctx.parent_type or ""
        try:
            llm_result = ctx.llm_backend.convert(
                sas_code=sas_code,
                parent_context=context_hint,
                block_category=category,
            )
            if llm_result and llm_result.strip():
                # Indenta il codice LLM al livello corrente
                indented = "\n".join(
                    f"{pad}{line}" if line.strip() else line
                    for line in llm_result.splitlines()
                )
                header = (
                    f"{pad}# [LLM-generated] righe "
                    f"{blk.get('linea_start')}–{blk.get('linea_stop')} "
                    f"| {reason}"
                )
                return f"{header}\n{indented}"
        except Exception as exc:
            print(f"[LLM] Errore durante la conversione: {exc}")

    # ── Fallback: commento TODO deterministico ─────────────────────
    lines = [
        f"{pad}# {'=' * 60}",
        f"{pad}# TODO: REVISIONE MANUALE NECESSARIA",
        f"{pad}# Motivo    : {reason}",
        f"{pad}# Categoria : {blk.get('macro_categoria', '?')}",
        f"{pad}# Righe     : {blk.get('linea_start')} – {blk.get('linea_stop')}",
        f"{pad}# {'=' * 60}",
        f"{pad}# Codice SAS originale:",
    ]
    for line in sas_code.splitlines()[:30]:
        lines.append(f"{pad}#   {line.rstrip()}")
    if sas_code.count('\n') > 30:
        lines.append(f"{pad}#   ... (troncato)")
    lines.append(f"{pad}# {'=' * 60}")
    return "\n".join(lines)


# ─── HASH OBJECT ──────────────────────────────────────────────────────

def _convert_hash_object(blk: dict, ctx: ConversionContext) -> str:
    """
    Converte un DATA step con declare hash in PySpark.

    Tre sottocasi rilevati dal pattern del codice SAS:
      1. Lookup semplice  : rc = T.find(key:k)  senza find_next
         → broadcast join
      2. Multidata loop   : T.find_next() in do while
         → join normale (possibili righe duplicate)
      3. Aggregazione     : data _null_ + T.replace() + T.output()
         → groupBy().agg() + write

    Estrae automaticamente: nome hash, dataset sorgente, chiavi,
    colonne data, output dataset.
    """
    pad  = ctx.pad
    txt  = blk.get("testo", "")
    lines = [
        f"{pad}# ── HASH OBJECT"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}] ──"
    ]

    # ── Trova tutti i blocchi declare hash ──────────────────────────
    # declare hash T(hashexp:N, dataset:'lib.nome', multidata:'Y');
    hash_defs = {}
    for m in re.finditer(
        r'\bdeclare\s+hash\s+(\w+)\s*\(([^)]*)\)\s*;',
        txt, re.I | re.S
    ):
        h_name = m.group(1)
        h_opts = m.group(2)
        ds_m = re.search(r"dataset\s*:\s*['\"]([^'\"]+)['\"]", h_opts, re.I)
        multi_m = re.search(r"multidata\s*:\s*['\"]Y['\"]", h_opts, re.I)
        hash_defs[h_name] = {
            "dataset": ds_m.group(1) if ds_m else None,
            "multidata": bool(multi_m),
            "keys": [],
            "data": [],
        }

    # ── Raccoglie definekey / definedata ────────────────────────────
    for h_name in hash_defs:
        for m in re.finditer(
            r'\b' + re.escape(h_name) + r'\s*\.\s*definekey\s*\(([^)]+)\)\s*;',
            txt, re.I
        ):
            keys = [c.strip().strip("'\"") for c in m.group(1).split(',')]
            hash_defs[h_name]["keys"].extend(keys)
        for m in re.finditer(
            r'\b' + re.escape(h_name) + r'\s*\.\s*definedata\s*\(([^)]+)\)\s*;',
            txt, re.I
        ):
            cols = [c.strip().strip("'\"") for c in m.group(1).split(',')]
            hash_defs[h_name]["data"].extend(cols)

    # ── Determina il SET sorgente principale ────────────────────────
    set_m = re.search(r'\bset\s+([\w.]+)\s*;', txt, re.I)
    src_ds = set_m.group(1) if set_m else None
    src_py = _sas_to_py_var(src_ds) if src_ds else "source_df"

    # ── Riconosce il sottocaso ───────────────────────────────────────
    is_null_data  = bool(re.match(r'^\s*data\s+_null_\s*;', txt, re.I))
    has_find_next = bool(re.search(r'\.find_next\s*\(', txt, re.I))
    has_output    = bool(re.search(r'\.output\s*\(', txt, re.I))
    has_replace   = bool(re.search(r'\.replace\s*\(', txt, re.I))

    # ── Caso 3: aggregazione (data _null_ + replace + output) ───────
    if is_null_data and (has_replace or has_output):
        # Determina output dataset
        out_m = re.search(
            r'\.output\s*\(\s*dataset\s*:\s*[\'"]([^\'"]+)[\'"]',
            txt, re.I
        )
        out_ds   = out_m.group(1) if out_m else "hash_output"
        out_py   = _sas_to_py_var(out_ds)

        # Usa il primo hash definito come riferimento
        first_h  = next(iter(hash_defs.values()), {})
        grp_keys = first_h.get("keys", [])
        agg_cols = [c for c in first_h.get("data", []) if c not in grp_keys]

        if src_ds:
            lines.append(f"{pad}# Hash aggregation → groupBy/agg")
            grp_str = (
                ", ".join(f'"{k}"' for k in grp_keys)
                if grp_keys else '"TODO_GROUP_KEY"'
            )
            if agg_cols:
                agg_exprs = ", ".join(
                    f'F.sum("{c}").alias("SUM_of_{c}")' for c in agg_cols
                )
            else:
                agg_exprs = "F.count(\"*\").alias(\"count\")"
            lines.append(
                f"{pad}{out_py} = spark.table(\"{src_ds}\")\\\n"
                f"{pad}    .groupBy({grp_str})\\\n"
                f"{pad}    .agg({agg_exprs})"
            )
            lines.append(
                f"{pad}{out_py}.write.mode(\"overwrite\").saveAsTable(\"{out_ds}\")"
            )
        else:
            lines.append(f"{pad}# TODO: sorgente SET non trovata nel blocco hash aggregation")
        ctx.register_df(out_ds, out_py)
        return "\n".join(lines)

    # ── Genera codice per ogni hash definito ─────────────────────────
    for h_name, h_info in hash_defs.items():
        ds      = h_info["dataset"]
        keys    = h_info["keys"]
        data    = h_info["data"]
        multi   = h_info["multidata"]
        lkp_var = f"lookup_{h_name.lower()}"

        if not ds:
            lines.append(f"{pad}# {h_name}: dataset non rilevato → TODO")
            continue

        # Colonne da selezionare nel lookup
        all_cols = list(dict.fromkeys(keys + data))  # dedup, ordine mantenuto
        if all_cols:
            sel_str = ", ".join(f'"{c}"' for c in all_cols)
            lkp_expr = f'spark.table("{ds}").select({sel_str})'
        else:
            lkp_expr = f'spark.table("{ds}")'

        # Chiave join
        if len(keys) == 1:
            on_str = f'on="{keys[0]}"'
        elif keys:
            on_str = "on=[" + ", ".join(f'"{k}"' for k in keys) + "]"
        else:
            on_str = 'on="TODO_KEY"'

        if multi or has_find_next:
            # Caso 2: multidata — join normale (possibili duplicati)
            lines += [
                f"{pad}# Hash multidata '{h_name}' → join (possibili righe multiple)",
                f"{pad}{lkp_var} = {lkp_expr}",
            ]
            if src_ds:
                out_names = [n for n in re.findall(r'^\s*data\s+(.+?)\s*;', txt, re.I | re.M)]
                py_out = _sas_to_py_var(out_names[0]) if out_names else "join_result"
                lines.append(
                    f"{pad}{py_out} = spark.table(\"{src_ds}\")"
                    f".join({lkp_var}, {on_str}, how=\"left\")"
                )
                ctx.register_df(out_names[0] if out_names else py_out, py_out)
        else:
            # Caso 1: lookup semplice → broadcast join
            lines += [
                f"{pad}# Hash lookup '{h_name}' → broadcast join",
                f"{pad}{lkp_var} = {lkp_expr}",
            ]
            if src_ds:
                out_names = [n for n in re.findall(r'^\s*data\s+(.+?)\s*;', txt, re.I | re.M)]
                py_out = _sas_to_py_var(out_names[0]) if out_names else "join_result"
                lines.append(
                    f"{pad}{py_out} = spark.table(\"{src_ds}\")"
                    f".join(F.broadcast({lkp_var}), {on_str}, how=\"left\")"
                )
                ctx.register_df(out_names[0] if out_names else py_out, py_out)

    # ── call missing → commento ──────────────────────────────────────
    if re.search(r'\bcall\s+missing\s*\(', txt, re.I):
        lines.append(
            f"{pad}# call missing(of _ALL_) — reset variabili: non necessario in PySpark"
        )

    return "\n".join(lines)


# ─── DATA STEP ────────────────────────────────────────────────────────

def convert_data_step(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    info = _analyse_data_step(blk["testo"])
    aff  = blk.get("affidabilita_auto", "BASSA")

    # Nomi di output Python
    py_out_names = [_sas_to_py_var(n) for n in info.output_names] if info.output_names else ["df"]
    py_out = py_out_names[0]

    # Commento di intestazione
    header = (
        f"{pad}# ── DATA: {', '.join(info.output_names or ['?'])} "
        f"[riga {blk.get('linea_start')}–{blk.get('linea_stop')}] ──"
    )

    # ── HASH OBJECT → handler dedicato ─────────────────────────────
    if info.has_hash:
        return _convert_hash_object(blk, ctx)

    # ── ARRAY → TODO ─────────────────────────────────────────────────
    if info.has_array:
        return _todo_block(blk, ctx,
            "ARRAY SAS: riscrittura manuale necessaria")

    if info.has_retain or info.has_first_last:
        reason = "RETAIN / FIRST. LAST.: usare Window functions"
        todo = _todo_block(blk, ctx, reason)
        hint = (
            f"\n{pad}# Suggerimento PySpark:\n"
            f"{pad}# w = Window.partitionBy(...).orderBy(...)\n"
            f"{pad}# {py_out} = source_df.withColumn('col', F.sum('x').over(w))"
        )
        return todo + hint

    lines = [header]

    # ── SCHEMA VUOTO (STOP senza SET) ───────────────────────────────
    if info.has_stop and not info.sources and not info.merge_sources:
        schema_lines = []
        for fname, ftype in info.schema_fields:
            spark_t = _sas_type_to_spark(ftype)
            schema_lines.append(f"{pad}    StructField(\"{fname}\", {spark_t}, True),")
        schema_str = "\n".join(schema_lines) if schema_lines else f"{pad}    # TODO: definire schema"
        lines += [
            f"{pad}{py_out} = spark.createDataFrame([], schema=StructType([",
            schema_str,
            f"{pad}]))",
        ]
        if "result." in " ".join(info.output_names):
            lines.append(
                f"{pad}{py_out}.write.mode(\"overwrite\")"
                f".saveAsTable(\"{info.output_names[0]}\")"
            )
        for name in py_out_names:
            ctx.register_df(info.output_names[py_out_names.index(name)] if py_out_names.index(name) < len(info.output_names) else name, name)
        return "\n".join(lines)

    # ── MERGE (JOIN) ─────────────────────────────────────────────────
    if info.merge_sources:
        if len(info.merge_sources) == 2:
            ds_a = ctx.resolve_df(info.merge_sources[0])
            ds_b = ctx.resolve_df(info.merge_sources[1])
            flags = list(info.merge_in_flags.values())
            # determina tipo join
            if len(flags) == 2:
                join_how = "inner"    # if a and b
            elif len(flags) == 1 and flags[0] == "a":
                join_how = "left"
            elif len(flags) == 1 and flags[0] == "b":
                join_how = "right"
            else:
                join_how = "outer"
            by_str = (
                "[" + ", ".join(f'"{k}"' for k in info.by_keys) + "]"
                if info.by_keys else '"TODO_BY_KEY"'
            )
            lines += [
                f"{pad}# MERGE {info.merge_sources[0]} {info.merge_sources[1]} → {join_how} join",
                f"{pad}{py_out} = {ds_a}.join({ds_b}, on={by_str}, how=\"{join_how}\")",
            ]
            if aff == "MEDIA":
                lines.append(f"{pad}# TODO: verifica tipo join e chiavi BY")
        else:
            return _todo_block(blk, ctx,
                f"MERGE con {len(info.merge_sources)} sorgenti: join multiplo complesso")
        ctx.register_df(info.output_names[0] if info.output_names else py_out, py_out)
        return "\n".join(lines)

    # ── OUTPUT MULTIPLI ──────────────────────────────────────────────
    if info.multi_output and info.sources:
        source_py = ctx.resolve_df(info.sources[0])
        for cond, ds_out in info.multi_output:
            py_cond = _sas_expr_to_py(cond, ctx)
            py_var  = _sas_to_py_var(ds_out)
            lines.append(
                f"{pad}{py_var} = {source_py}.filter(F.expr(\"{py_cond}\"))"
            )
            ctx.register_df(ds_out, py_var)
        return "\n".join(lines)

    # ── SET SIMPLE : filter + select + withColumn ───────────────────
    if info.sources:
        src_sas   = info.sources[0]
        source_py = ctx.resolve_df(src_sas)
        chain     = [f'spark.table("{src_sas}")']

        # WHERE  (con rilevamento macro EG)
        if info.where_clause:
            if info.has_eg_macro:
                lines += [
                    f"{pad}# TODO: WHERE contiene macro EG (%_eg_WhereParam)",
                    f"{pad}# Condizione originale: {info.where_clause[:120].strip()}",
                    f"{pad}# Sostituisci con i valori effettivi del prompt:",
                ]
                # Tenta comunque di estrarre le colonne e l'operatore
                for m_egp in re.finditer(
                    r'%_eg_WhereParam\s*\(\s*(\w+)\s*,\s*\w+\s*,\s*(\w+)',
                    info.where_clause, re.I
                ):
                    col_eg  = m_egp.group(1)
                    op_eg   = m_egp.group(2).upper()
                    op_py   = {'LE': '<=', 'GE': '>=', 'LT': '<',
                               'GT': '>', 'EQ': '==', 'NE': '!='}.get(op_eg, op_eg)
                    lines.append(
                        f"{pad}#   .filter(F.col(\"{col_eg}\") {op_py} F.lit(prompt_value))"
                    )
            else:
                py_where = _sas_expr_to_py(
                    ctx.resolve_macro_var(info.where_clause), ctx
                )
                chain.append(f"{pad}    .filter(F.expr(\"{py_where}\"))")

        # KEEP → .select()  (formattato su più righe se > 6 colonne)
        if info.keep_cols:
            if len(info.keep_cols) <= 6:
                cols_str = ", ".join(f'"{c}"' for c in info.keep_cols)
                chain.append(f"{pad}    .select({cols_str})")
            else:
                col_lines = [f'{pad}        "{c}",' for c in info.keep_cols]
                chain.append(f"{pad}    .select(\n" + "\n".join(col_lines) + f"\n{pad}    )")

        # DROP → .drop()
        if info.drop_cols:
            drop_str = ", ".join(f'"{c}"' for c in info.drop_cols)
            chain.append(f"{pad}    .drop({drop_str})")

        # Colonnes calculées
        for col_name, col_expr in info.calc_columns:
            py_expr_raw = ctx.resolve_macro_var(col_expr)
            py_expr     = _sas_expr_to_py(py_expr_raw, ctx)
            # input() già tradotto da _sas_expr_to_py → usare direttamente
            if py_expr.startswith("F.col("):
                chain.append(f"{pad}    .withColumn(\"{col_name}\", {py_expr})")
            else:
                chain.append(
                    f"{pad}    .withColumn(\"{col_name}\", F.expr(\"{py_expr}\"))"
                )

        # Assembla la catena di metodi
        if len(chain) == 1:
            lines.append(f"{pad}{py_out} = {chain[0]}")
        else:
            # Racchiude in () per permettere il method chaining multiriga
            inner = f"\n{pad}    ".join(chain)
            lines.append(f"{pad}{py_out} = (\n{pad}    {inner}\n{pad})")
        ctx.register_df(info.output_names[0] if info.output_names else py_out, py_out)
        if aff == "MEDIA":
            lines.append(f"{pad}# TODO: verifica espressioni convertite")
        return "\n".join(lines)

    # ── CREAZIONE DA ZERO (valori fissi, senza SET) ──────────────────
    val_rows = []
    for col_name, col_expr in info.calc_columns:
        resolved = ctx.resolve_macro_var(col_expr)
        val_rows.append(f"{col_name}={repr(resolved)}")

    if val_rows:
        schema_names = [c[0] for c in info.calc_columns]
        row_values   = [ctx.resolve_macro_var(c[1]) for c in info.calc_columns]
        lines += [
            f"{pad}{py_out} = spark.createDataFrame(",
            f"{pad}    [({', '.join(repr(v) for v in row_values)},)],",
            f"{pad}    schema=[{', '.join(repr(n) for n in schema_names)}]",
            f"{pad})",
        ]
        ctx.register_df(info.output_names[0] if info.output_names else py_out, py_out)
        return "\n".join(lines)

    # ── FALLBACK ─────────────────────────────────────────────────────
    return _todo_block(blk, ctx, "Pattern DATA step non riconosciuto")


# ─── PROC SQL ─────────────────────────────────────────────────────────

def convert_proc_sql(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    info = _analyse_proc_sql(blk["testo"])

    header = (
        f"{pad}# ── PROC SQL"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}] ──"
    )

    # SQL con macro SAS incorporate → non convertibile automaticamente
    if info.has_dynamic_sql:
        pad = ctx.pad
        motivo = (
            "SQL dinamico: macro SAS nel corpo della query "
            "(%if/%then/%do, %str, %sysfunc, call execute).\n"
            f"{pad}# Il codice SAS genera SQL diverso a runtime in base a variabili macro.\n"
            f"{pad}# Strategia: risolvere le variabili macro prima di convertire,\n"
            f"{pad}# oppure usare una logica if/else Python che chiama spark.sql() distinte."
        )
        todo = _todo_block(blk, ctx, motivo)
        # Aggiunge anche il SQL grezzo come riferimento
        raw_lines = [f"{pad}#   {l}" for l in info.raw_sql.splitlines()[:25]]
        if raw_lines:
            todo += f"\n{pad}# SQL originale (con macro SAS):\n" + "\n".join(raw_lines)
        return todo

    lines = [header]

    # ── Genera variabili Python per le liste IN da macro SAS ─────────
    # Pattern: WHERE col IN (%if &n=1 %then %do; val1 %end; %else %do i=1 %to &n; ...)
    # Strategia 1 (preferita): se esiste già una lista raccolta da INTO :var1- nel contesto,
    #   riutilizzarla direttamente → .filter(F.col(col).isin(existing_list_var))
    # Strategia 2 (fallback): genera _col_values = []  con commento TODO
    isin_filters = []
    for col_name, macro_block in info.macro_in_lists:
        # Cerca se esiste una lista INTO compatibile nel contesto
        existing_list = ctx.find_into_list_for_macro_block(macro_block)
        if existing_list:
            # Lista già disponibile: uso diretto senza TODO
            lines += [
                f"{pad}# LISTA IN per '{col_name}': usa la lista raccolta dal blocco INTO precedente",
                f"{pad}# SAS: &&id_prodotto&i  →  Python: {existing_list}",
            ]
            isin_filters.append((col_name, existing_list))
        else:
            # Lista sconosciuta: genera placeholder con TODO e tutti gli hint possibili
            py_list_var = f"_{col_name.lower()}_values"
            macro_var_hints = sorted(set(re.findall(r'&{1,2}(\w+)', macro_block)))
            hint_str = ", ".join(f"&{v}" for v in macro_var_hints) if macro_var_hints else "variabili macro SAS"
            lines += [
                f"{pad}# LISTA IN per '{col_name}': generata da macro SAS a runtime.",
                f"{pad}# Variabili macro da risolvere: {hint_str}",
                f"{pad}# Popola questa lista con i valori corrispondenti ai parametri SAS.",
                f"{pad}{py_list_var} = []  # TODO: popola con i valori di {hint_str}",
            ]
            isin_filters.append((col_name, py_list_var))

    # Estrae il SQL grezzo e crea una variabile
    py_out = _sas_to_py_var(info.create_table) if info.create_table else "df_sql"
    if info.create_table:
        ctx.register_df(info.create_table, py_out)

    # Strategia: spark.sql() direttamente (il più affidabile)
    raw = info.raw_sql.strip()

    # Ripulisce il SQL dai blocchi macro IN: rimuove l'intera condizione "col IN (macro)"
    # dalla WHERE clause — il filtro sarà applicato via .filter().isin() sul DataFrame.
    for col_name, macro_block in info.macro_in_lists:
        # Rimuove: [AND|OR] col IN (\n  macro_block\n)  oppure col IN (...) [AND|OR]
        # Gestisce sia posizione iniziale che intermedia nella WHERE
        escaped_col = re.escape(col_name)
        escaped_block = re.escape(macro_block)
        # Pattern: (AND|OR)? col IN ( macro_block )
        raw = re.sub(
            r'(?:(?<=\s)|(?<=\())(?:AND\s+|OR\s+)?' + escaped_col
            + r'\s+IN\s*\(\s*' + escaped_block + r'\s*\)(?:\s+AND|\s+OR)?',
            '',
            raw,
            flags=re.I | re.S,
        )
        # Se rimane una WHERE vuota (solo spazi/newline/;), la rimuove
        raw = re.sub(r'\bwhere\s*(?:and\s+|or\s+)?(?=group\b|order\b|having\b|;|$)',
                     '', raw, flags=re.I | re.S)

    # Sostituisce nomi di dataset con i loro equivalenti Spark
    for sas_name, py_var in ctx.available_dfs.items():
        raw = re.sub(r'\b' + re.escape(sas_name) + r'\b', py_var, raw, flags=re.I)

    if isin_filters:
        lines.append(
            f"{pad}# NOTA: le condizioni IN con macro SAS sono applicate via .filter().isin() dopo la query"
        )

    if info.into_var and info.into_var_is_list:
        # SAS: INTO :var1-  →  crea una lista Python + variabile contatore (come &sqlobs)
        # Equivalente a: SELECT DISTINCT col INTO :var1- FROM ...
        # In PySpark: raccoglie tutti i valori distinti in una lista Python
        list_var = f"{info.into_var}_list"
        count_var = f"n_{info.into_var}"
        # Rimuove la sintassi INTO dal SQL grezzo (non valida in Spark SQL)
        raw_no_into = re.sub(
            r'\binto\s+:\w+\s*-\s*', '', raw, flags=re.I
        ).strip()
        lines += [
            f"{pad}# INTO :{info.into_var}1-  →  lista Python + contatore (equivalente a &sqlobs)",
            f"{pad}_df_into = {ctx.spark_session_var}.sql(\"\"\"{raw_no_into}\"\"\")",
            f"{pad}{list_var} = [row[0] for row in _df_into.collect()]",
            f"{pad}{count_var} = len({list_var})",
            f"{pad}# Accesso singolo elemento: {info.into_var}_list[0], {info.into_var}_list[1], ...",
        ]
        # Registra la lista nel contesto: i blocchi SQL successivi la useranno in .isin()
        ctx.register_into_list(info.into_var, list_var)
    elif info.into_var:
        # Scalare: INTO :var  →  primo valore della prima riga
        raw_no_into = re.sub(
            r'\binto\s+:\w+\s*', '', raw, flags=re.I
        ).strip()
        lines += [
            f"{pad}# INTO :{info.into_var}  →  variabile Python scalare",
            f"{pad}_df_into = {ctx.spark_session_var}.sql(\"\"\"{raw_no_into}\"\"\")",
            f"{pad}{info.into_var} = _df_into.collect()[0][0]",
        ]
    elif info.create_table:
        lines += [
            f"{pad}{py_out} = {ctx.spark_session_var}.sql(\"\"\"{raw}\"\"\")",
        ]
        # Applica i filtri .isin() per le liste IN generate da macro
        for col_name, py_list_var in isin_filters:
            lines.append(
                f"{pad}{py_out} = {py_out}.filter(F.col(\"{col_name}\").isin({py_list_var}))"
            )
        lines.append(
            f"{pad}{py_out}.write.mode(\"overwrite\").saveAsTable(\"{info.create_table}\")"
        )
    else:
        lines += [
            f"{pad}{py_out} = {ctx.spark_session_var}.sql(\"\"\"{raw}\"\"\")",
        ]
        for col_name, py_list_var in isin_filters:
            lines.append(
                f"{pad}{py_out} = {py_out}.filter(F.col(\"{col_name}\").isin({py_list_var}))"
            )

    lines.append(f"{pad}# TODO: verifica SQL e nomi tabelle/alias")
    return "\n".join(lines)


# ─── PROC SORT ────────────────────────────────────────────────────────

def convert_proc_sort(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    info = _analyse_proc_sort(blk["testo"])

    header = (
        f"{pad}# ── PROC SORT {info.data_in or '?'}"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}] ──"
    )

    src_py = ctx.resolve_df(info.data_in) if info.data_in else "df"
    out_py = _sas_to_py_var(info.data_out) if info.data_out else src_py

    # Costruisce le chiavi di ordinamento
    sort_cols = []
    for key in info.by_keys:
        if key in info.descending_keys:
            sort_cols.append(f"F.col(\"{key}\").desc()")
        else:
            sort_cols.append(f"F.col(\"{key}\")")
    sort_str = ", ".join(sort_cols) if sort_cols else '"TODO_sort_key"'

    lines = [
        header,
        f"{pad}{out_py} = {src_py}.orderBy({sort_str})",
    ]

    if info.nodupkey:
        by_cols = ", ".join(f'"{k}"' for k in info.by_keys)
        lines += [
            f"{pad}# NODUPKEY → dropDuplicates sulle chiavi BY",
            f"{pad}{out_py} = {out_py}.dropDuplicates([{by_cols}])",
        ]

    ctx.register_df(info.data_out or info.data_in or "", out_py)
    return "\n".join(lines)


# ─── PROC APPEND ──────────────────────────────────────────────────────

def convert_proc_append(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    txt = blk["testo"]

    m_base = re.search(r'\bbase\s*=\s*([\w.]+)', txt, re.I)
    m_data = re.search(r'\bdata\s*=\s*([\w.]+)', txt, re.I)

    base_sas = m_base.group(1).strip() if m_base else "base_table"
    data_sas = m_data.group(1).strip() if m_data else "data_table"

    base_py = ctx.resolve_df(base_sas)
    data_py = ctx.resolve_df(data_sas)
    out_py  = base_py

    return "\n".join([
        f"{pad}# ── PROC APPEND base={base_sas} data={data_sas}"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}] ──",
        f"{pad}{out_py} = {base_py}.union({data_py})",
        f"{pad}{out_py}.write.mode(\"overwrite\").saveAsTable(\"{base_sas}\")",
    ])


# ─── PROC DATASETS ────────────────────────────────────────────────────

def convert_proc_datasets(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    return _todo_block(blk, ctx,
        "PROC DATASETS: gestione catalogo (DELETE/COPY/RENAME) → revisione manuale")


# ─── MACRO CALL ───────────────────────────────────────────────────────

def convert_macro_call(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    txt = blk["testo"].strip()
    m = re.match(r'%(\w+)\s*\(([^)]*)\)', txt, re.I)
    if m:
        macro_name = m.group(1)
        raw_args   = m.group(2)
        py_name    = f"macro_{macro_name}"
        # Converti argomenti keyword: flusso=&x → flusso=x
        args_py = ctx.resolve_macro_var(raw_args)
        return (
            f"{pad}# ── MACRO CALL %{macro_name}"
            f" [riga {blk.get('linea_start')}] ──\n"
            f"{pad}{py_name}({args_py}, spark=spark)"
        )
    return _todo_block(blk, ctx, "MACRO CALL: sintassi non riconosciuta")


# ─── INCLUDE ──────────────────────────────────────────────────────────

def convert_include(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    txt = blk["testo"].strip()
    m = re.search(r'%inc(?:lude)?\s+"?([^";\s]+)"?\s*;', txt, re.I)
    path = m.group(1) if m else "???"
    return (
        f"{pad}# ── %INCLUDE \"{path}\""
        f" [riga {blk.get('linea_start')}] ──\n"
        f"{pad}# TODO: importa o converti il file esterno:\n"
        f"{pad}#   exec(open(\"{path}\").read())  # oppure: import modulo"
    )


# ─── IF BLOCK ─────────────────────────────────────────────────────────

def convert_if_block(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    info = _analyse_if_block(blk["testo"])
    cond_py = _sas_macro_condition_to_py(info.condition_sas, ctx)

    header = (
        f"{pad}# %IF {info.condition_sas[:60]}"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}]"
    )
    body   = "\n".join(children_code) if children_code else f"{pad}    pass"
    return f"{header}\n{pad}if {cond_py}:\n{body}"


# ─── ELSE BLOCK ───────────────────────────────────────────────────────

def convert_else_block(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    body = "\n".join(children_code) if children_code else f"{pad}    pass"
    return f"{pad}# %ELSE [riga {blk.get('linea_start')}–{blk.get('linea_stop')}]\n{pad}else:\n{body}"


# ─── MACRO DEF ────────────────────────────────────────────────────────

def convert_macro_def(
    blk: dict,
    children_code: List[str],
    ctx: ConversionContext,
) -> str:
    pad = ctx.pad
    info = _analyse_macro_def(blk["testo"])

    py_name = f"macro_{info.name}" if info.name else "macro_unknown"
    params  = list(info.params) + ["spark"]
    sig     = ", ".join(params)

    header = (
        f"{pad}# ══════════════════════════════════════════════════════\n"
        f"{pad}# %MACRO {info.name}({', '.join(info.params)})"
        f" [riga {blk.get('linea_start')}–{blk.get('linea_stop')}]\n"
        f"{pad}# ══════════════════════════════════════════════════════"
    )

    body = "\n".join(children_code) if children_code else f"    {pad}pass"

    return f"{header}\n{pad}def {py_name}({sig}):\n{body}"


# ═══════════════════════════════════════════════════════════════════════
# TABLE DE DISPATCH
# ═══════════════════════════════════════════════════════════════════════

CONVERTERS: Dict[str, Callable] = {
    "MACRO_DEF":    convert_macro_def,
    "IF_BLOCK":     convert_if_block,
    "ELSE_BLOCK":   convert_else_block,
    "DATA_STEP":    convert_data_step,
    "PROC_SQL":     convert_proc_sql,
    "PROC_SORT":    convert_proc_sort,
    "PROC_APPEND":  convert_proc_append,
    "PROC_DATASETS":convert_proc_datasets,
    "MACRO_CALL":   convert_macro_call,
    "INCLUDE":      convert_include,
}


# ═══════════════════════════════════════════════════════════════════════
# STRATO 1 – ORCHESTRATORE (post-order DFS)
# ═══════════════════════════════════════════════════════════════════════

def _build_children_map(
    blocks: Dict[str, dict],
) -> Dict[Optional[str], List[str]]:
    """
    Costruisce un indice uid → [children_uid] a partire dai parent_uid.
    I nodi senza padre hanno parent_uid = None → chiave None nel dict.
    """
    children_map: Dict[Optional[str], List[str]] = {}
    for uid, blk in blocks.items():
        p = blk.get("parent_uid")
        children_map.setdefault(p, []).append(uid)
    return children_map


def convert_node(
    uid: str,
    blocks: Dict[str, dict],
    children_map: Dict[Optional[str], List[str]],
    ctx: ConversionContext,
) -> str:
    """
    Converte ricorsivamente un nodo dell'albero (post-order).
    I figli vengono convertiti PRIMA del padre.
    """
    blk = blocks[uid]
    cat = blk.get("macro_categoria", "UNKNOWN")
    conv_auto = blk.get("convertibile_auto", "NO")
    aff_auto  = blk.get("affidabilita_auto", "BASSA")

    # ── Contesto figlio in base al tipo del nodo corrente ───────────
    if cat == "MACRO_DEF":
        info = _analyse_macro_def(blk["testo"])
        child_ctx = ctx.child("macro")
        child_ctx.macro_params = info.params
        child_ctx.function_name = f"macro_{info.name}"
    elif cat in ("IF_BLOCK", "ELSE_BLOCK"):
        child_ctx = ctx.child(cat.lower())
    else:
        child_ctx = ctx.child()

    # ── Converte i figli per primi (post-order) ──────────────────────
    child_uids = children_map.get(uid, [])
    children_code = [
        convert_node(child_uid, blocks, children_map, child_ctx)
        for child_uid in child_uids
    ]

    # ── Dispatch verso il convertitore appropriato ───────────────────
    converter = CONVERTERS.get(cat)
    if converter:
        return converter(blk, children_code, ctx)

    # ── Fallback ─────────────────────────────────────────────────────
    return _todo_block(blk, ctx, f"Categoria '{cat}' non gestita")


def convert_block_preview(uid: str, blocks: Dict[str, dict]) -> str:
    """
    Converte un singolo blocco in isolamento (senza contesto figlio).
    Usato per la colonna 'codice_pyspark' nel file Excel:
    ogni blocco mostra la conversione che lo riguarda direttamente.

    Restituisce una stringa compatta (max ~30 righe).
    """
    blk = blocks.get(uid)
    if not blk:
        return ""
    cat  = blk.get("macro_categoria", "")
    conv = blk.get("convertibile_auto", "NO")

    if conv == "NO":
        reason = f"Blocco {cat} non convertibile automaticamente"
        return _todo_block(blk, ConversionContext(), reason)

    converter = CONVERTERS.get(cat)
    if not converter:
        return f"# Categoria '{cat}' non gestita"

    try:
        code = converter(blk, [], ConversionContext())
        # Troncamento a 40 righe per leggibilità Excel
        lines = code.splitlines()
        if len(lines) > 40:
            code = "\n".join(lines[:40]) + f"\n# ... ({len(lines)-40} righe aggiuntive)"
        return code
    except Exception as e:
        return f"# Errore conversione: {e}"


def convert_blocks_to_map(
    blocks: Dict[str, dict],
    llm_backend: object = None,
) -> Dict[str, str]:
    """
    Converte tutti i blocchi in isolamento e restituisce
    un dizionario {uid: codice_pyspark} per popolare l'Excel.
    Il llm_backend opzionale viene passato al ConversionContext.
    """
    return {uid: convert_block_preview(uid, blocks) for uid in blocks}


def convert_tree(
    blocks: Dict[str, dict],
    llm_backend: object = None,
) -> str:
    """
    Punto di ingresso principale del pipeline completo.
    Riceve il dizionario di blocchi (da trova_sas3_tracker.py) e restituisce
    lo script PySpark completo come stringa.

    llm_backend : istanza di OllamaConverter, CodestralConverter o None.
                  Se fornito, i blocchi non convertibili dalle regole vengono
                  inviati al LLM prima di emettere un commento TODO.
    """
    if not blocks:
        return "# Nessun blocco da convertire."

    children_map = _build_children_map(blocks)

    # Nodi radice (parent_uid = None)
    root_uids = children_map.get(None, [])
    # Ordinamento per riga di partenza per preservare l'ordine del file SAS
    root_uids.sort(key=lambda u: blocks[u].get("linea_start", 0))

    global_ctx = ConversionContext(llm_backend=llm_backend)
    converted_parts = []

    for uid in root_uids:
        code = convert_node(uid, blocks, children_map, global_ctx)
        converted_parts.append(code)

    body = "\n\n".join(converted_parts)
    return _PYSPARK_HEADER + "\n\n" + body + "\n"


# ═══════════════════════════════════════════════════════════════════════
# HOOK LLM – PUNTO DI INTEGRAZIONE FUTURO
# ═══════════════════════════════════════════════════════════════════════

class LLMFallback:
    """
    Segnaposto per l'integrazione futura di un motore LLM.

    Utilizzo previsto:
        llm = LLMFallback(api_key="...", model="claude-opus-4-6")
        code = llm.convert_block(blk["testo"], context_hint="DATA step con RETAIN")

    Protocollo:
        - Il motore rule-based fallisce (blocco TODO generato)
        - Il blocco SAS + contesto vengono inviati al LLM
        - La risposta LLM viene restituita come codice PySpark
        - La coppia (SAS_bloc, PySpark_code) viene memorizzata nella base
          di apprendimento per fine-tuning o few-shot futuro
    """

    def __init__(self, api_key: str = "", model: str = "claude-opus-4-6"):
        self.api_key = api_key
        self.model   = model
        self.enabled = bool(api_key)
        self._learning_db: List[dict] = []

    def convert_block(self, sas_code: str, context_hint: str = "") -> str:
        """
        Invia il blocco SAS al LLM e restituisce il codice PySpark.
        Restituisce None se il LLM non è attivato.
        """
        if not self.enabled:
            return None  # il motore rule-based mantiene il controllo

        # TODO: chiamare l'API LLM qui
        # prompt = self._build_prompt(sas_code, context_hint)
        # response = anthropic.Anthropic(api_key=self.api_key).messages.create(...)
        # return response.choices[0].message.content
        raise NotImplementedError("LLM integration: vedere TODO in llm_integration.py")

    def record_correction(
        self,
        sas_code: str,
        generated_py: str,
        corrected_py: str,
    ) -> None:
        """
        Registra una correzione manuale dello sviluppatore.
        Queste coppie alimenteranno il fine-tuning o i few-shot examples.
        """
        self._learning_db.append({
            "sas_input":    sas_code,
            "generated":    generated_py,
            "corrected":    corrected_py,
        })

    def export_learning_db(self, path: str) -> None:
        """Esporta la base di apprendimento in JSONL (formato fine-tuning)."""
        with open(path, "w", encoding="utf-8") as f:
            for entry in self._learning_db:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(f"Learning DB esportata: {path} ({len(self._learning_db)} voci)")


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Converte un file JSON di blocchi SAS (trova_sas3_tracker) in PySpark"
    )
    parser.add_argument(
        "json_file",
        help="File JSON prodotto da trova_sas3_tracker.py (--no-json disabilitato)"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="File Python di output (default: stesso nome .py)"
    )
    args = parser.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"ERRORE: file non trovato: {json_path}")
        return

    # ── Carica il JSON ───────────────────────────────────────────────
    # Il JSON di trova_sas3_tracker è un albero → bisogna «appiattirlo»
    with open(json_path, encoding="utf-8") as f:
        tree_json = json.load(f)

    def _flatten(nodes: list, result: dict, parent_uid=None):
        for node in nodes:
            uid = node["uid"]
            result[uid] = {**node, "parent_uid": parent_uid}
            if "children" in node:
                _flatten(node["children"], result, uid)

    blocks: Dict[str, dict] = {}
    _flatten(tree_json, blocks)
    print(f"Blocchi caricati: {len(blocks)}")

    # ── Conversion ───────────────────────────────────────────────────
    pyspark_code = convert_tree(blocks)

    # ── Output ───────────────────────────────────────────────────────
    out_path = Path(args.output) if args.output else json_path.with_suffix(".py")
    out_path.write_text(pyspark_code, encoding="utf-8")
    print(f"Script PySpark generato: {out_path}")

    # Anteprima
    preview_lines = pyspark_code.splitlines()[:40]
    print("\n── Anteprima (prime 40 righe) ──")
    for line in preview_lines:
        print(line)


if __name__ == "__main__":
    main()
