from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple, Callable
from collections import deque
import re, hashlib
import pandas as pd

# ───────────────────────── REGEX comuni ─────────────────────────
RE_COMMENT_LINE  = re.compile(r'^\s*\*.*?;\s*$', re.S)   # '*' ... ';' su una riga
RE_BLOCK_COMMENT = re.compile(r'/\*.*?\*/', re.S)        # /* ... */ multilina

# ───────────────────── key_fn per etichette "tipo" ──────────────
def _key_data(header: str, n: int) -> str:
    parts = header.split()
    name = parts[1].rstrip(';') if len(parts) > 1 else f"step{n}"
    return f"data_{name}_{n}"

def _key_proc(header: str, n: int) -> str:
    parts = header.split()
    name = parts[1] if len(parts) > 1 else f"proc{n}"
    return f"proc_{name}_{n}"

def _key_include(_: str, n: int) -> str:
    return f"include_{n}"

def _key_macro(header: str, n: int) -> str:
    parts = header.split()
    name = parts[1] if len(parts) > 1 else f"macro{n}"
    return f"macro_{name}_{n}"

def _key_if(header: str, n: int) -> str:
    return f"ifdo_{n}"

def _key_else(header: str, n: int) -> str:
    return f"elsedo_{n}"

# ─────────── macro IF/ELSE/END (versione con DO/END a blocchi) ───────────
RE_IFDO_START    = re.compile(r'^\s*%if\b.*?\b%then\b\s*%do\s*;?\s*$', re.I)
RE_ELSEDO_START  = re.compile(r'^\s*%else\b\s*%do\s*;?\s*$', re.I)
RE_MACRO_ENDTOK  = re.compile(r'^\s*%end\s*;?\s*$', re.I)  # chiusura blocchi macro
# --- aggiungi/rafforza queste regex vicino alle altre ---
RE_MACRO_START = re.compile(r'^\s*%macro\b', re.I)
RE_MEND_TOKEN  = re.compile(r'^\s*%mend\b(?:\s+\w+)?\s*;\s*$', re.I)

# ────────────────── definizione blocchi (ordine = priorità) ─────
BLOCKS: Dict[str, Dict[str, object]] = {
    # Più specifici prima se necessario
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
    # Blocchi IF/ELSE con DO/END bilanciati
    "macro": {
        "start": RE_MACRO_START,
        "end": RE_MEND_TOKEN,  # usato se balanced=False (fallback)
        "key_fn": _key_macro,
        "balanced": True,  # ← attiva il contatore di nesting
        "nest_starts": [RE_MACRO_START],  # ← nuove %macro incrementano la profondità
        "end_token": RE_MEND_TOKEN,  # ← %mend; decrementa; chiudi a depth==0
    },

    # IF/ELSE bilanciati (come già fatto)
    "ifdo": {
        "start": RE_IFDO_START,
        "end": RE_MACRO_ENDTOK,
        "key_fn": _key_if,
        "balanced": True,
        "nest_starts": [RE_IFDO_START, RE_ELSEDO_START],
        "end_token": RE_MACRO_ENDTOK,
    },
    "elsedo": {
        "start": RE_ELSEDO_START,
        "end": RE_MACRO_ENDTOK,
        "key_fn": _key_else,
        "balanced": True,
        "nest_starts": [RE_IFDO_START, RE_ELSEDO_START],
        "end_token": RE_MACRO_ENDTOK,
    },
    "include": {
        "start": re.compile(r'^\s*%include\b', re.I),
        "end":   re.compile(r'^\s*;\s*$', re.I),  # ";" su propria riga
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

# ────────────────────────── UID stabile ─────────────────────────
def _stable_uid(file_id: str, blk_name: str, start_line_no: int, block_text: str) -> str:
    blk_hash = hashlib.blake2b(block_text.encode("utf-8", "ignore"), digest_size=8).hexdigest()
    return f"{file_id}:{blk_name}:{start_line_no}:{blk_hash}"

# ────────────────────────── PARSER 0 ────────────────────────────
def parse_sas_blocks(
    path: str | Path,
    *,
    deep: bool = True,
    max_depth: int = 50,
    start_from: int = 1
) -> Dict[str, Dict[str, object]]:
    """
    Parser iterativo (deep-first) basato su deque.

    Ritorna:
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
    file_id = hashlib.blake2b(str(p.resolve()).encode("utf-8"), digest_size=8).hexdigest()

    text = p.read_text(encoding="utf-8", errors="ignore")

    # Taglio opzionale mantenendo offset corretto
    if start_from and start_from > 1:
        all_lines = text.splitlines(keepends=True)
        text = "".join(all_lines[start_from-1:])
        base_offset = start_from - 1
    else:
        base_offset = 0

    initial_raw: List[Tuple[str, bool]] = preprocess_lines_keep_comments(text)

    # work item = (raw_lines_segment, offset_line, depth)
    work = deque([(initial_raw, base_offset, 0)])

    blocks: Dict[str, Dict[str, object]] = {}
    counter = 1

    while work:
        raw_lines, offset, depth = work.popleft()
        i = 0
        n = len(raw_lines)

        while i < n:
            line, is_comment = raw_lines[i]
            if is_comment:
                i += 1
                continue

            matched = False

            for blk_name, spec in BLOCKS.items():  # ordine di BLOCKS preservato
                start_re: re.Pattern = spec["start"]  # type: ignore
                if start_re.match(line):
                    matched = True
                    tipo_label = spec["key_fn"](line.strip(), counter)  # type: ignore
                    end_re: re.Pattern = spec["end"]  # type: ignore

                    start_idx = i
                    start_line_no = offset + start_idx + 1  # 1-based

                    buf: List[str] = [line]
                    i += 1

                    # %include chiuso su riga singola con ';'
                    if blk_name == "include" and line.rstrip().endswith(";"):
                        stop_line_no = start_line_no
                        block_text = "".join(buf)
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

                    # ricerca end (bilanciata opzionale)
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
                            if not c:
                                if any(r.match(l) for r in nest_starts):
                                    depth_if += 1
                                elif end_token.match(l):
                                    depth_if -= 1
                                    if depth_if == 0:
                                        end_found = True
                                        stop_line_no = offset + i + 1
                                        break
                            i += 1
                    else:
                        while i < n:
                            l, c = raw_lines[i]
                            buf.append(l)
                            if not c and end_re.match(l):
                                end_found = True
                                stop_line_no = offset + i + 1
                                break
                            i += 1

                    # salva blocco (blocco completo)
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

                    # profondità: re-inserisci SOLO l'interno (evita loop su %macro/%mend)
                    if deep and depth < max_depth:
                        if end_found and len(buf) >= 2:
                            # escludi la riga di start (buf[0]) e la riga di end (buf[-1])
                            inner_text = "".join(buf[1:-1])
                            inner_offset = start_line_no  # la prima inner è start+1
                        else:
                            # fino a EOF: escludi solo la riga di start
                            inner_text = "".join(buf[1:])
                            inner_offset = start_line_no

                        if inner_text.strip():
                            inner_raw = preprocess_lines_keep_comments(inner_text)
                            work.appendleft((inner_raw, inner_offset, depth + 1))

                    break  # blocco trovato → non testare altri start su questa riga

            if not matched:
                i += 1

    return blocks

# ─────────────────────────── CLI test ────────────────────────────
if __name__ == "__main__":
    sas_file = "import.sas"
    f = open('import.txt', 'w+', encoding="utf-8")

    # Esegui il parser
    blocks = parse_sas_blocks(sas_file)

    # Trasforma in DataFrame
    df = pd.DataFrame.from_dict(blocks, orient="index")

    # Aggiungi la colonna con l'uid come prima colonna
    df.insert(0, "uid", df.index)

    # Mostra la tabella a schermo
    print(df.head(200))  # stampa le prime 20 righe
    for k, v in blocks.items():
        print(f"\n=== {k} ===\n{v['testo']}")
        #os.remove("import.txt",'w+')
        f.write(f"\n=== {k} ===\n{v['testo']}")
