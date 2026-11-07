"""Entrypoint: `python app/backfill_ga4_dataapi ...` (Cloud Run) ou `python -m backfill_ga4_dataapi ...` a partir de app/."""
import sys
from pathlib import Path

# Ao executar a pasta diretamente, o Python põe a própria pasta no path; isso deixaria config.py/api.py/bq.py
# importáveis como módulos de topo. Trocamos por app/, para os imports absolutos do pacote.
_PKG_DIR = Path(__file__).resolve().parent
sys.path = [p for p in sys.path if not p or Path(p).resolve() != _PKG_DIR]
_APP_DIR = str(_PKG_DIR.parent)
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from backfill_ga4_dataapi.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
