import os
import sys

# Valores fictícios: os testes nunca acessam BigQuery, GA4 ou Cloud Run.
os.environ.setdefault("PROJECT_ID", "proj-teste")
os.environ.setdefault("DATASET_RAW", "raw_teste")
os.environ.setdefault("DATASET_SILVER", "silver_teste")
os.environ.setdefault("GA4_PROPERTY_ID", "123")
os.environ.pop("RUN_DATE", None)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
