import os
import sys
from pathlib import Path

# Valores do ambiente do bq-mirror-job; os testes nunca acessam BigQuery, GA4 ou Cloud Run.
os.environ.setdefault("PROJECT_ID", "jota-dados-integracao-ga4")
os.environ.setdefault("DATASET_RAW", "analytics_420210992")
os.environ.setdefault("DATASET_SILVER", "ga4_metrics_us")
os.environ.setdefault("GA4_PROPERTY_ID", "420210992")

_APP = Path(__file__).resolve().parents[2]
for p in (str(_APP), str(_APP / "src")):
    if p not in sys.path:
        sys.path.insert(0, p)
