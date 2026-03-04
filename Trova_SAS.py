"""
sas_block_parser.py  –  Parser generico a dizionario
----------------------------------------------------

Estrae blocchi DATA, PROC, %INCLUDE e %MACRO sfruttando
un dizionario che mappa:
    • nome blocco  →  { 'start': regex, 'end': regex, 'key_fn': callable }
Questo evita lunghi if/elif.
"""

from __future__ import annotations

import os
import re,uuid,hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Callable
from collections import deque

# ───────────────────────── REGEX comuni ─────────────────────────
RE_COMMENT_LINE  = re.compile(r'^\s*\*.*?;\s*$', re.S)
RE_BLOCK_COMMENT = re.compile(r'/\*.*?\*/', re.S)

# ─────────── macro IF/ELSE/END (versione con DO/END a blocchi) ───────────
RE_IFDO_START    = re.compile(r'^\s*%if\b.*?\b%then\b\s*%do\s*;?\s*$', re.I)
RE_ELSEDO_START  = re.compile(r'^\s*%else\b\s*%do\s*;?\s*$', re.I)
RE_MACRO_END     = re.compile(r'^\s*%end\s*;?\s*$', re.I)  # chiusura blocchi macro

def _key_data(header: str, n: int) -> str:
    name = header.split()[1].rstrip(';') if len(header.split()) > 1 else f"step{n}"
    return f"data_{name}_{n}"

def _key_proc(header: str, n: int) -> str:
    name = header.split()[1] if len(header.split()) > 1 else f"proc{n}"
    return f"proc_{name}_{n}"

def _key_include(_: str, n: int) -> str:
    return f"include_{n}"

def _key_macro(header: str, n: int) -> str:
    name = header.split()[1] if len(header.split()) > 1 else f"macro{n}"
    return f"macro_{name}_{n}"

def _key_if(header: str, n: int) -> str:
    return f"ifdo_{n}"

def _key_else(header: str, n: int) -> str:
    return f"elsedo_{n}"


BLOCKS: Dict[str, Dict[str, re.Pattern | Callable[[str, int], str]]] = {
    "data": {
        "start": re.compile(r'^\s*data\b', re.I),
        "end":   re.compile(r'^\s*run\s*;\s*$', re.I),
        "key_fn": _key_data,
    },
    "proc": {
        "start": re.compile(r'^\s*proc\b', re.I),
        "end":   re.compile(r'^\s*(run|quit)\s*;\s*$', re.I),
        "key_fn": _key_proc,
    },
    "macro": {
        "start": re.compile(r'^\s*%macro\b', re.I),
        "end":   re.compile(r'^\s*%mend\b.*;\s*$', re.I),
        "key_fn": _key_macro,
        # opzionale: puoi trattarlo come balanced se vuoi %macro annidate (raro)
        # "balanced": True,
        # "nest_starts": [re.compile(r'^\s*%macro\b', re.I)],
        # "end_token": RE_MACRO_END,  # oppure mantieni l'end originale
    },
    # --- NUOVI BLOCCHI: %if ... %then %do; ... %end; ---
    "ifdo": {
        "start": RE_IFDO_START,
        "end":   RE_MACRO_END,
        "key_fn": _key_if,
        # Abilita gestione annidamenti (vedi patch parser sotto)
        "balanced": True,
        "nest_starts": [RE_IFDO_START, RE_ELSEDO_START],
        "end_token": RE_MACRO_END,
    },
    "elsedo": {
        "start": RE_ELSEDO_START,
        "end":   RE_MACRO_END,
        "key_fn": _key_else,
        "balanced": True,
        "nest_starts": [RE_IFDO_START, RE_ELSEDO_START],
        "end_token": RE_MACRO_END,
    },
    "include": {
        "start": re.compile(r'^\s*%include\b', re.I),
        "end":   re.compile(r'^\s*;\s*$', re.I),     # ";" su propria riga ⇢ fine include
        "key_fn": _key_include,
    },
}


# ──────────────────── comment handling (immutato) ───────────────
def preprocess_lines_keep_comments(code: str) -> List[Tuple[str, bool]]:
    """
    Ritorna [(linea, is_comment)] lasciando i commenti nel testo,
    marcati per ignorarli nei match delle regex di blocco.
    """
    out, inside = [], False
    for line in code.splitlines(keepends=True):
        stripped = line.lstrip()

        if not inside and '/*' in stripped:
            inside = True
            out.append((line, True))
            if '*/' in stripped:
                inside = False
            continue
        if inside:
            out.append((line, True))
            if '*/' in stripped:
                inside = False
            continue
        if RE_COMMENT_LINE.match(stripped):
            out.append((line, True))
            continue
        out.append((line, False))
    return out

def _stable_uid(file_id: str, blk_name: str, start_line_no: int, block_text: str) -> str:
    """UID stabile/deterministico: file_id + tipo + riga_start + hash testo blocco."""
    blk_hash = hashlib.blake2b(block_text.encode("utf-8", "ignore"), digest_size=8).hexdigest()
    return f"{file_id}:{blk_name}:{start_line_no}:{blk_hash}"

# ────────────────────────── PARSER ───────────────────────────────
def parse_sas_blocks(
    path: str | Path,
    *,
    deep: bool = True,
    max_depth: int = 50,
    start_from: int = 1
) -> Dict[str, Dict[str, object]]:
    """
    Parser iterativo (deep-first) basato su deque.

    Parametri:
      - deep:      se True re-inserisce in testa alla coda il contenuto interno dei blocchi per analizzarlo subito.
      - max_depth: profondità massima (sicurezza).
      - start_from: riga 1-based da cui iniziare la lettura.

    Ritorna un dict:
      {
        "<uid>": {
           "tipo":        "<label es. data_3>",
           "linea_start": <int 1-based>,
           "linea_stop":  <int|None>,
           "testo":       "<blocco completo (start+corpo+end se presente)>",
           "chiuso":      <bool>  # True se end trovato
        }, ...
      }
    """
    p = Path(path)
    # ID file stabile basato sul path risolto; se preferisci usa i bytes del file
    file_id = hashlib.blake2b(str(p.resolve()).encode("utf-8"), digest_size=8).hexdigest()

    text = p.read_text(encoding="utf-8", errors="ignore")

    # Taglio opzionale (mantenendo offset per numerazione riga corretta)
    if start_from and start_from > 1:
        all_lines = text.splitlines(keepends=True)
        text = "".join(all_lines[start_from-1:])
        base_offset = start_from - 1
    else:
        base_offset = 0

    initial_raw: List[Tuple[str, bool]] = preprocess_lines_keep_comments(text)

    # Coda di lavoro: ogni item = (raw_lines_segment, offset_line, depth)
    # - offset_line è 0-based: prima riga del segmento nel file originale = offset_line + 1
    work = deque([(initial_raw, base_offset, 0)])

    blocks: Dict[str, Dict[str, object]] = {}
    counter = 1  # contatore globale per key_fn (mantiene formati tipo "data_3")

    while work:
        # prendiamo SEMPRE dalla testa (left) e inseriamo i nuovi segmenti in TESTA (appendleft)
        # → profondità prima della larghezza (deep-first)
        raw_lines, offset, depth = work.popleft()
        i = 0
        n = len(raw_lines)

        while i < n:
            line, is_comment = raw_lines[i]
            if is_comment:
                i += 1
                continue

            matched = False

            for blk_name, spec in BLOCKS.items():  # ordine: specifici → generici
                if spec["start"].match(line):
                    matched = True
                    tipo_label = spec["key_fn"](line.strip(), counter)
                    end_re: re.Pattern = spec["end"]  # type: ignore

                    start_idx = i
                    start_line_no = offset + start_idx + 1  # 1-based

                    buf: List[str] = [line]
                    i += 1

                    # Caso speciale: %include su riga singola terminata da ';'
                    if blk_name == "include" and line.rstrip().endswith(";"):
                        stop_line_no = start_line_no
                        block_text = "".join(buf)  # policy: blocco completo
                        uid = _stable_uid(file_id, blk_name, start_line_no, block_text)
                        blocks[uid] = {
                            "tipo": tipo_label,
                            "linea_start": start_line_no,
                            "linea_stop": stop_line_no,
                            "testo": block_text,
                            "chiuso": True,
                        }
                        counter += 1
                        break

                    end_found = False
                    stop_line_no = None

                    balanced = bool(spec.get("balanced"))
                    nest_starts: List[re.Pattern] = spec.get("nest_starts", []) if balanced else []
                    end_token: re.Pattern = spec.get("end_token", end_re) if balanced else end_re

                    if balanced:
                        depth_if = 1
                        while i < n:
                            l, c = raw_lines[i]
                            buf.append(l)
                            if not c:  # ignora commenti
                                # incremento nesting se incontro un nuovo start
                                if any(r.match(l) for r in nest_starts):
                                    depth_if += 1
                                # decremento su %end;
                                elif end_token.match(l):
                                    depth_if -= 1
                                    if depth_if == 0:
                                        end_found = True
                                        stop_line_no = offset + i + 1
                                        break
                            i += 1
                    else:
                        # comportamento originale: chiude al primo end
                        while i < n:
                            l, c = raw_lines[i]
                            buf.append(l)
                            if not c and end_re.match(l):
                                end_found = True
                                stop_line_no = offset + i + 1
                                break
                            i += 1

                    # Policy coerente: "testo" = blocco completo
                    block_text = "".join(buf)

                    uid = _stable_uid(file_id, blk_name, start_line_no, block_text)
                    blocks[uid] = {
                        "tipo": tipo_label,
                        "linea_start": start_line_no,
                        "linea_stop": stop_line_no,
                        "testo": block_text,
                        "chiuso": bool(end_found),
                    }
                    counter += 1

                    # Discesa: accodiamo SUBITO (in testa) il contenuto interno
                    # escludendo la riga di start e la riga di end (se presente)
                    if deep and depth < max_depth:
                        if end_found and len(buf) >= 2:
                            inner_text = "".join(buf[0:])
                            # la prima inner-line corrisponde a start_line_no + 1 ⇒ offset = start_line_no
                            inner_offset = start_line_no
                        else:
                            inner_text = "".join(buf[0:])
                            inner_offset = start_line_no

                        if inner_text.strip():
                            inner_raw = preprocess_lines_keep_comments(inner_text)
                            # Inseriamo in TESTA per depth-first
                            work.appendleft((inner_raw, inner_offset, depth + 1))

                    break  # match su un blocco: non testare altri start su questa riga

            if not matched:
                i += 1  # nessun blocco trovato: passa alla riga successiva

    return blocks
# ─────────────────────────── CLI test ────────────────────────────
if __name__ == "__main__":
    print("ciao")
    sas_file = "import.sas"
    f = open('import.txt', 'w+', encoding="utf-8")

    blocks=parse_sas_blocks(sas_file)
    for k, v in blocks.items():
        print(f"\n=== {k} ===\n{v['testo']}")
        #os.remove("import.txt",'w+')
        f.write(f"\n=== {k} ===\n{v['testo']}")
