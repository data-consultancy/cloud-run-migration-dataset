"""Tabelas _backfill_dataapi_*: linhas cruas da API e linhas já no formato final (hashes no BigQuery)."""
import re

from google.cloud import bigquery

from backfill_ga4_dataapi import bq as bqh
from backfill_ga4_dataapi.config import (
    DATASET_GA4,
    DEVICE_SK_DESCONHECIDO_SQL,
    EVENT_SK_SQL,
    PAGE_SK_SQL,
    PROJECT_ID,
    SK_GEO_DESCONHECIDO_SQL,
    T_CONTEUDO,
    T_DIM_TRAFEGO,
    T_DUSER_DIARIO,
    T_FEVENTS,
    T_LOG,
    T_MAIN,
    T_RAW_CONTEUDO,
    T_RAW_DUSER,
    T_RAW_MAIN,
    T_STG_CONTEUDO,
    T_STG_DUSER_DIARIO,
    T_STG_MAIN,
    T_STG_TRAFEGO,
    T_STG_TRAFEGO_FEVENTS,
    TRAFFIC_SK_SQL,
    VIEW_DTRAFFIC,
)

_STR = "STRING"
SCHEMA_RAW = {
    "main": [("data", "DATE"), ("event_name", _STR), ("source", _STR), ("medium", _STR), ("campaign", _STR),
             ("user_company", _STR), ("user_plan", _STR), ("is_pro_user_flag", _STR),
             ("event_count", "INT64"), ("total_users", "INT64")],
    "conteudo": [("data", "DATE"), ("page_location", _STR), ("source", _STR), ("medium", _STR), ("campaign", _STR),
                 ("user_company", _STR), ("is_pro_user_flag", _STR),
                 ("event_count", "INT64"), ("total_users", "INT64")],
    "duser": [("data", "DATE"), ("user_company", _STR), ("user_plan", _STR), ("event_count", "INT64")],
}
RAW_TABLES = {"main": T_RAW_MAIN, "conteudo": T_RAW_CONTEUDO, "duser": T_RAW_DUSER}


def bq_schema(report: str) -> list:
    return [bigquery.SchemaField(name, typ) for name, typ in SCHEMA_RAW[report]]


def ensure_tables(bq: bigquery.Client) -> None:
    ddl = []
    for report, table in RAW_TABLES.items():
        cols = ",\n      ".join(f"{name} {typ}" for name, typ in SCHEMA_RAW[report])
        ddl.append(f"CREATE TABLE IF NOT EXISTS `{table}` (\n      {cols}\n    ) PARTITION BY data;")
    # Mesmo schema das tabelas finais, para o --execute copiar sem transformação
    ddl += [
        f"CREATE TABLE IF NOT EXISTS `{T_STG_MAIN}` LIKE `{T_MAIN}`;",
        f"CREATE TABLE IF NOT EXISTS `{T_STG_CONTEUDO}` LIKE `{T_CONTEUDO}`;",
        f"CREATE TABLE IF NOT EXISTS `{T_STG_DUSER_DIARIO}` LIKE `{T_DUSER_DIARIO}`;",
        f"CREATE TABLE IF NOT EXISTS `{T_STG_TRAFEGO_FEVENTS}` LIKE `{T_FEVENTS}`;",
        f"CREATE TABLE IF NOT EXISTS `{T_STG_TRAFEGO}` LIKE `{T_DIM_TRAFEGO}`;",
        f"""CREATE TABLE IF NOT EXISTS `{T_LOG}` (
      run_id STRING, modo STRING, data DATE, tabela STRING, linhas INT64,
      totais STRING, executado_em TIMESTAMP, observacoes STRING
    );""",
    ]
    bqh.run(bq, "\n".join(ddl))


def replace_raw(bq: bigquery.Client, report: str, dates, data: list) -> None:
    table = RAW_TABLES[report]
    bqh.run(bq, f"DELETE FROM `{table}` WHERE data IN ({bqh.dates_sql(dates)});")
    bqh.load_json(bq, table, bq_schema(report), data)


def prepare_main_sql(dates) -> str:
    d = bqh.dates_sql(dates)
    return f"""
    DELETE FROM `{T_STG_MAIN}` WHERE data_evento IN ({d});

    INSERT INTO `{T_STG_MAIN}` (
      data_evento, event_sk, sk_geo, device_sk, traffic_sk,
      user_company, user_plan, is_pro_user_flag,
      total_eventos, usuarios_unicos_aprox, total_sessoes
    )
    SELECT
      data AS data_evento,
      {EVENT_SK_SQL} AS event_sk,
      {SK_GEO_DESCONHECIDO_SQL} AS sk_geo,
      {DEVICE_SK_DESCONHECIDO_SQL} AS device_sk,
      {TRAFFIC_SK_SQL} AS traffic_sk,
      user_company, user_plan, is_pro_user_flag,
      SUM(event_count) AS total_eventos,
      SUM(total_users) AS usuarios_unicos_aprox,
      SUM(IF(event_name = 'session_start', event_count, 0)) AS total_sessoes
    FROM `{T_RAW_MAIN}`
    WHERE data IN ({d})
    GROUP BY 1, 2, 5, 6, 7, 8;
    """


def prepare_conteudo_sql(dates) -> str:
    d = bqh.dates_sql(dates)
    return f"""
    DELETE FROM `{T_STG_CONTEUDO}` WHERE data_evento IN ({d});

    INSERT INTO `{T_STG_CONTEUDO}` (
      data_evento, page_sk, traffic_sk, sk_geo, user_company, is_pro_user_flag,
      pageviews, leitores_unicos_aprox
    )
    SELECT
      data AS data_evento,
      {PAGE_SK_SQL} AS page_sk,
      {TRAFFIC_SK_SQL} AS traffic_sk,
      {SK_GEO_DESCONHECIDO_SQL} AS sk_geo,
      COALESCE(user_company, 'N/A') AS user_company,
      COALESCE(is_pro_user_flag, 'false') AS is_pro_user_flag,
      SUM(event_count) AS pageviews,
      SUM(total_users) AS leitores_unicos_aprox
    FROM `{T_RAW_CONTEUDO}`
    WHERE data IN ({d})
    GROUP BY 1, 2, 3, 5, 6;
    """


def prepare_duser_sql(dates) -> str:
    d = bqh.dates_sql(dates)
    return f"""
    DELETE FROM `{T_STG_DUSER_DIARIO}` WHERE data IN ({d});

    INSERT INTO `{T_STG_DUSER_DIARIO}` (user_company, data, total_eventos, ultimo_plano, ts_ultimo_plano)
    SELECT
      user_company,
      data,
      SUM(event_count) AS total_eventos,
      -- Aproximação: plano com mais eventos no dia (a API não dá o timestamp do evento)
      ARRAY_AGG(user_plan IGNORE NULLS ORDER BY event_count DESC, user_plan DESC LIMIT 1)[SAFE_OFFSET(0)] AS ultimo_plano,
      CAST(NULL AS INT64) AS ts_ultimo_plano
    FROM `{T_RAW_DUSER}`
    WHERE data IN ({d})
      AND user_company IS NOT NULL
    GROUP BY 1, 2;
    """


def prepare_trafego_fevents_sql(dates) -> str:
    """Linhas com o schema de fEvents_v2 só com as colunas de tráfego, para alimentar a lógica de vw_dTraffic."""
    d = bqh.dates_sql(dates)
    return f"""
    DELETE FROM `{T_STG_TRAFEGO_FEVENTS}` WHERE event_date_parsed IN ({d});

    INSERT INTO `{T_STG_TRAFEGO_FEVENTS}` (
      event_date_parsed, event_name, traffic_sk,
      traffic_source_source, traffic_source_medium, traffic_source_name,
      user_company, user_plan, is_pro_user_flag, page_url_clean, hostname_calculado
    )
    SELECT DISTINCT
      data, event_name, {TRAFFIC_SK_SQL},
      source, medium, campaign,
      user_company, user_plan, is_pro_user_flag, CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `{T_RAW_MAIN}`
    WHERE data IN ({d})
    UNION DISTINCT
    SELECT DISTINCT
      data, 'page_view', {TRAFFIC_SK_SQL},
      source, medium, campaign,
      user_company, CAST(NULL AS STRING), is_pro_user_flag,
      SPLIT(page_location, '?')[SAFE_OFFSET(0)], REGEXP_EXTRACT(page_location, r'^https?://([^/]+)')
    FROM `{T_RAW_CONTEUDO}`
    WHERE data IN ({d});
    """


# Referências a fEvents_v2 na definição da view, com ou sem projeto e crases
_FEVENTS_REF = re.compile(
    r"`?(?:" + re.escape(PROJECT_ID) + r"`?\.`?)?" + re.escape(DATASET_GA4) + r"`?\.`?fEvents_v2`?",
    re.IGNORECASE,
)


def read_view_definition(bq: bigquery.Client) -> str:
    found = bqh.rows(bq, f"""
    SELECT view_definition
    FROM `{PROJECT_ID}.{DATASET_GA4}.INFORMATION_SCHEMA.VIEWS`
    WHERE table_name = '{VIEW_DTRAFFIC}'
    """)
    if not found:
        raise RuntimeError(f"View {DATASET_GA4}.{VIEW_DTRAFFIC} não encontrada em INFORMATION_SCHEMA.VIEWS")
    return found[0]["view_definition"]


def rewrite_view(view_sql: str, source_table: str) -> str:
    """Troca fEvents_v2 pela staging na definição de vw_dTraffic, reaproveitando a lógica da view sem copiá-la."""
    rewritten, count = _FEVENTS_REF.subn(f"`{source_table}`", view_sql)
    if count == 0 or re.search(r"fEvents_v2", rewritten, re.IGNORECASE):
        raise RuntimeError(
            "Não foi possível reapontar vw_dTraffic para a staging: a definição não referencia "
            f"{DATASET_GA4}.fEvents_v2 de forma reconhecível. Definição:\n{view_sql}"
        )
    return rewritten.strip().rstrip(";")


def prepare_trafego_sql(view_sql: str, backfill_dates) -> str:
    """traffic_sk das datas do backfill que ainda não existem em dim_trafego_ga4, derivados pela vw_dTraffic."""
    view_on_staging = rewrite_view(view_sql, T_STG_TRAFEGO_FEVENTS)
    return f"""
    TRUNCATE TABLE `{T_STG_TRAFEGO}`;

    INSERT INTO `{T_STG_TRAFEGO}` (traffic_sk, source, medium, campaign, traffic_group, origem_ga4, source_medium)
    SELECT traffic_sk, source, medium, campaign, traffic_group, origem_ga4, source_medium
    FROM (
    {view_on_staging}
    ) v
    WHERE v.traffic_sk IN (
        SELECT traffic_sk FROM `{T_STG_TRAFEGO_FEVENTS}`
        WHERE event_date_parsed IN ({bqh.dates_sql(backfill_dates)})
      )
      AND v.traffic_sk NOT IN (SELECT traffic_sk FROM `{T_DIM_TRAFEGO}` WHERE traffic_sk IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY traffic_sk ORDER BY source, medium, campaign) = 1;
    """


def missing_traffic_sk_sql(backfill_dates) -> str:
    """traffic_sk usados nas agregadas do backfill que ficariam sem linha na dimensão."""
    d = bqh.dates_sql(backfill_dates)
    return f"""
    SELECT COUNT(DISTINCT traffic_sk) AS sem_dimensao
    FROM (
      SELECT traffic_sk FROM `{T_STG_MAIN}` WHERE data_evento IN ({d})
      UNION ALL
      SELECT traffic_sk FROM `{T_STG_CONTEUDO}` WHERE data_evento IN ({d})
    )
    WHERE traffic_sk NOT IN (SELECT traffic_sk FROM `{T_DIM_TRAFEGO}` WHERE traffic_sk IS NOT NULL)
      AND traffic_sk NOT IN (SELECT traffic_sk FROM `{T_STG_TRAFEGO}` WHERE traffic_sk IS NOT NULL)
    """
