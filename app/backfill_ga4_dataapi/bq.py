"""Helpers finos sobre o cliente BigQuery."""
import datetime
import json

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from backfill_ga4_dataapi.config import BQ_LOCATION, T_LOG

SCHEMA_LOG = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("modo", "STRING"),
    bigquery.SchemaField("data", "DATE"),
    bigquery.SchemaField("tabela", "STRING"),
    bigquery.SchemaField("linhas", "INT64"),
    bigquery.SchemaField("totais", "STRING"),
    bigquery.SchemaField("executado_em", "TIMESTAMP"),
    bigquery.SchemaField("observacoes", "STRING"),
]


def run(bq: bigquery.Client, sql: str):
    return bq.query(sql, location=BQ_LOCATION).result()


def rows(bq: bigquery.Client, sql: str) -> list:
    return [dict(r) for r in run(bq, sql)]


def table_exists(bq: bigquery.Client, table_id: str) -> bool:
    try:
        bq.get_table(table_id)
        return True
    except NotFound:
        return False


def load_json(bq: bigquery.Client, table_id: str, schema: list, data: list) -> None:
    if not data:
        return
    job = bq.load_table_from_json(
        data,
        table_id,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
        location=BQ_LOCATION,
    )
    job.result()


def dates_sql(dates) -> str:
    return ", ".join(f"DATE '{d}'" for d in dates)


class RunLog:
    """Acumula as linhas de _backfill_dataapi_log de uma execução e grava tudo de uma vez."""

    def __init__(self, run_id: str, modo: str):
        self.run_id = run_id
        self.modo = modo
        self.entries = []

    def add(self, tabela: str, data=None, linhas=None, totais=None, observacoes=None) -> None:
        self.entries.append({
            "run_id": self.run_id,
            "modo": self.modo,
            "data": data,
            "tabela": tabela,
            "linhas": linhas,
            "totais": json.dumps(totais, ensure_ascii=False, default=str) if totais is not None else None,
            "executado_em": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "observacoes": "; ".join(observacoes) if isinstance(observacoes, (list, tuple, set)) else observacoes,
        })

    def flush(self, bq: bigquery.Client) -> None:
        load_json(bq, T_LOG, SCHEMA_LOG, self.entries)
        self.entries = []
