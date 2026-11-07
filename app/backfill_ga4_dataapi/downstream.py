"""Reuso dos SQLs do paid-media-job (cópias idênticas em sql_midiaspagas/) para as datas do backfill."""
import datetime
import re
from pathlib import Path

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

from backfill_ga4_dataapi import bq as bqh
from backfill_ga4_dataapi.config import (
    DATASET_GA4,
    DATASET_MP_GOLD,
    DATASET_MP_SILVER,
    PROJECT_ID,
    T_FATO_SESSOES,
)

SQL_DIR = Path(__file__).parent / "sql_midiaspagas"

# Valores padrão das variáveis do paid-media-job (insert_fato_*.py e cloudbuild.yaml)
_PLACEHOLDERS = {
    "PROJECT_ID": PROJECT_ID,
    "DATASET_GA4": DATASET_GA4,
    "DATASET_SILVER": DATASET_MP_SILVER,
    "DATASET_GOLD": DATASET_MP_GOLD,
    "TABLE_DIM_TRAFEGO_GA4": "dim_trafego_ga4",
    "TABLE_FATO_SESSOES_GA4": "fato_sessoes_ga4",
    "TABLE_DIM_CAMPANHA": "dim_campanha",
    "TABLE_DIM_PLATAFORMA": "dim_plataforma",
    "TABLE_FATO_PERFORMANCE_AD": "fato_performance_ad",
    "TABLE_FATO_PERFORMANCE_MIDIA": "fato_performance_midia",
}

# Janela fixa D-3 do fato_sessoes_ga4.sql, trocada pelas datas do backfill (2 ocorrências)
SESSOES_WINDOW = ">= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY)"
SESSOES_WINDOW_COUNT = 2


def render(template: str, extra: dict = None) -> str:
    text = template
    for key, value in {**_PLACEHOLDERS, **(extra or {})}.items():
        text = text.replace("{" + key + "}", value)
    leftover = re.findall(r"\{[A-Z_]+\}", text)
    if leftover:
        raise RuntimeError(f"Placeholders sem valor no SQL: {sorted(set(leftover))}")
    return text


def sessoes_template() -> str:
    return (SQL_DIR / "fato_sessoes_ga4.sql").read_text(encoding="utf-8")


def gold_template() -> str:
    return (SQL_DIR / "fato_performance_midia.sql").read_text(encoding="utf-8")


def sessoes_script(dates) -> str:
    merge = render(sessoes_template())
    count = merge.count(SESSOES_WINDOW)
    if count != SESSOES_WINDOW_COUNT:
        raise RuntimeError(
            f"fato_sessoes_ga4.sql tem {count} ocorrências da janela D-3 (esperado {SESSOES_WINDOW_COUNT}); "
            "o SQL mudou e o reuso não é seguro."
        )
    merge = merge.replace(SESSOES_WINDOW, f"IN ({bqh.dates_sql(dates)})").strip()
    if not merge.endswith(";"):
        merge += ";"
    # O MERGE original não remove linhas; o DELETE prévio torna a carga idempotente por data
    return f"""
    BEGIN TRANSACTION;

    DELETE FROM `{T_FATO_SESSOES}` WHERE data IN ({bqh.dates_sql(dates)});

    {merge}

    COMMIT TRANSACTION;
    """


def contiguous_ranges(dates) -> list:
    """Agrupa datas em faixas contínuas (início, fim)."""
    days = sorted(datetime.date.fromisoformat(d) for d in dates)
    ranges = []
    for day in days:
        if ranges and day - ranges[-1][1] == datetime.timedelta(days=1):
            ranges[-1][1] = day
        else:
            ranges.append([day, day])
    return [(a.isoformat(), b.isoformat()) for a, b in ranges]


def gold_scripts(dates) -> list:
    """Um script por faixa contínua; o SQL original já faz DELETE + INSERT da faixa numa transação."""
    return [render(gold_template(), {"DATA_INICIO": a, "DATA_FIM": b}) for a, b in contiguous_ranges(dates)]


def _normalize(sql: str) -> str:
    sql = re.sub(r"'\d{4}-\d{2}-\d{2}'", "'<DATA>'", sql)
    return re.sub(r"\s+", " ", sql).strip()


def _last_production_query(bq: bigquery.Client, starts_with: str, table: str):
    found = bqh.rows(bq, f"""
    SELECT query
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
      AND job_type = 'QUERY'
      AND state = 'DONE'
      AND error_result IS NULL
      AND parent_job_id IS NULL
      AND STARTS_WITH(TRIM(query), '{starts_with}')
      AND query LIKE '%{table}%'
      AND query NOT LIKE '%_backfill_dataapi%'
    ORDER BY creation_time DESC
    LIMIT 1
    """)
    return found[0]["query"] if found else None


def verify_against_production(bq: bigquery.Client, tables) -> list:
    """Compara as cópias com o último SQL executado pelo paid-media-job. Diferença aborta; sem acesso, só avisa."""
    checks = []
    if "sessoes" in tables:
        checks.append(("fato_sessoes_ga4.sql", "MERGE INTO", "fato_sessoes_ga4", render(sessoes_template())))
    if "gold" in tables:
        checks.append(("fato_performance_midia.sql", "BEGIN TRANSACTION", "fato_performance_midia",
                       render(gold_template(), {"DATA_INICIO": "2000-01-01", "DATA_FIM": "2000-01-01"})))

    notes = []
    for name, starts_with, table, expected in checks:
        try:
            production = _last_production_query(bq, starts_with, table)
        except Forbidden as exc:
            notes.append(f"{name}: não verificado contra produção (sem permissão em JOBS_BY_PROJECT: {exc.message})")
            continue
        if production is None:
            notes.append(f"{name}: não verificado (nenhuma execução do paid-media-job nos últimos 14 dias)")
            continue
        if _normalize(production) != _normalize(expected):
            raise RuntimeError(
                f"{name} difere do último SQL executado em produção. Atualize a cópia em "
                f"{SQL_DIR.name}/ a partir do repositório midias-pagas antes de continuar."
            )
        notes.append(f"{name}: idêntico ao último SQL executado em produção")
    return notes
