"""
process_input.py
================
Processa tutti i file .sas trovati nella cartella INPUT/ e produce gli
output nella cartella output/<YYYYMMDD_HHMMSS>_<nome>/ contenente:

    <nome>.sas     -> copia del file SAS originale
    <nome>.py      -> script PySpark convertito
    <nome>.json    -> albero dei blocchi SAS (intermedio)
    <nome>.xlsx    -> report Excel di analisi

Utilizzo:
    # Processa tutti i file nuovi (non ancora convertiti)
    python process_input.py

    # Forza la riconversione anche di file già processati
    python process_input.py --force

    # Specifica una cartella INPUT diversa
    python process_input.py --input-dir ./miei_sas

Logica di "file già processato":
    Viene considerato già processato un file <nome>.sas per cui esiste
    almeno una cartella output/<data>_<nome>/ contenente un <nome>.py.
    Usare --force per rielaborare comunque.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

# ── import dal pipeline esistente ────────────────────────────────────
from run_converter import convert_sas_file


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

ROOT = Path(__file__).parent
DEFAULT_INPUT  = ROOT / "INPUT"
DEFAULT_OUTPUT = ROOT / "output"


def already_processed(sas_name: str, output_root: Path) -> bool:
    """True se esiste già una cartella output/*_<sas_name>/ con il .py."""
    stem = Path(sas_name).stem
    for folder in output_root.glob(f"*_{stem}"):
        if folder.is_dir() and (folder / f"{stem}.py").exists():
            return True
    return False


def run(
    input_dir: Path,
    output_root: Path,
    force: bool = False,
    verbose: bool = True,
) -> list[Path]:
    """
    Processa tutti i .sas in input_dir.
    Ritorna la lista delle cartelle di output create.
    """
    sas_files = sorted(input_dir.rglob("*.sas"))

    if not sas_files:
        print(f"[INFO] Nessun file .sas trovato in: {input_dir}")
        return []

    print(f"[INFO] File .sas trovati: {len(sas_files)}")
    created: list[Path] = []

    for sas_path in sas_files:
        stem = sas_path.stem

        if not force and already_processed(stem, output_root):
            print(f"[SKIP] {sas_path.name}  (già convertito, usa --force per rielaborare)")
            continue

        # Cartella di output: output/YYYYMMDD_HHMMSS_<nome>/
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = output_root / f"{ts}_{stem}"
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'='*60}")
        print(f"  SAS    : {sas_path.relative_to(ROOT)}")
        print(f"  OUTPUT : {out_dir.relative_to(ROOT)}/")
        print(f"{'='*60}")

        # 1. Copia il file SAS originale nella cartella di output
        shutil.copy2(sas_path, out_dir / sas_path.name)

        # 2. Esegui la conversione (py + json + xlsx)
        try:
            convert_sas_file(
                sas_path=sas_path,
                output_dir=out_dir,
                excel=True,
                json_intermediate=True,
                verbose=verbose,
            )
            created.append(out_dir)
        except Exception as exc:
            print(f"[ERRORE] Conversione fallita per {sas_path.name}: {exc}", file=sys.stderr)
            # Lascia la cartella così com'è per debug; rimuovi il .py mancante
            # così il file non risulta "già processato"

    return created


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Processa file SAS da INPUT/ e produce output strutturati.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input-dir", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        metavar="DIR",
        help=f"Cartella sorgente dei file .sas (default: {DEFAULT_INPUT.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        metavar="DIR",
        help=f"Cartella radice degli output (default: {DEFAULT_OUTPUT.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Rielabora anche i file già convertiti",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Output minimale",
    )
    args = parser.parse_args()

    if not args.input_dir.exists():
        print(f"[ERRORE] Cartella INPUT non trovata: {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    created = run(
        input_dir=args.input_dir,
        output_root=args.output_dir,
        force=args.force,
        verbose=not args.quiet,
    )

    print(f"\n[DONE] Cartelle create: {len(created)}")
    for d in created:
        print(f"       -> {d.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
