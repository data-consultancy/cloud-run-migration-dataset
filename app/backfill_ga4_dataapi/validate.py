"""Validação do dry-run: dias de controle (API x tabelas atuais) e datas do backfill x mediana dos vizinhos."""
import datetime
import statistics

from google.cloud import bigquery

from backfill_ga4_dataapi import bq as bqh
from backfill_ga4_dataapi.config import (
    BACKFILL_DATES,
    T_CONTEUDO,
    T_DIM_TRAFEGO,
    T_DUSER_DIARIO,
    T_MAIN,
    T_STG_CONTEUDO,
    T_STG_DUSER_DIARIO,
    T_STG_MAIN,
    T_STG_TRAFEGO,
)

NEIGHBORS = 7
NEIGHBOR_SEARCH_DAYS = 30

# métrica -> (tabela atual, tabela de staging, coluna de data, expressão)
METRICS = {
    "main.total_eventos": (T_MAIN, T_STG_MAIN, "data_evento", "SUM(total_eventos)"),
    "main.total_sessoes": (T_MAIN, T_STG_MAIN, "data_evento", "SUM(total_sessoes)"),
    "conteudo.pageviews": (T_CONTEUDO, T_STG_CONTEUDO, "data_evento", "SUM(pageviews)"),
    "duser.total_eventos_empresas": (T_DUSER_DIARIO, T_STG_DUSER_DIARIO, "data", "SUM(total_eventos)"),
    "duser.empresas": (T_DUSER_DIARIO, T_STG_DUSER_DIARIO, "data", "COUNT(DISTINCT user_company)"),
}
METRIC_TABLE = {"main": "main", "conteudo": "conteudo", "duser": "duser"}


def _totals(bq: bigquery.Client, table: str, date_col: str, expr: str, where: str) -> dict:
    found = bqh.rows(bq, f"""
    SELECT FORMAT_DATE('%Y-%m-%d', {date_col}) AS d, {expr} AS v
    FROM `{table}`
    WHERE {where}
    GROUP BY 1
    """)
    return {r["d"]: r["v"] for r in found}


def _paid_sessions(bq: bigquery.Client, main_table: str, dates, with_backfill_dim: bool) -> dict:
    dim = f"SELECT traffic_sk, traffic_group FROM `{T_DIM_TRAFEGO}`"
    if with_backfill_dim:
        dim += f" UNION ALL SELECT traffic_sk, traffic_group FROM `{T_STG_TRAFEGO}`"
    found = bqh.rows(bq, f"""
    SELECT FORMAT_DATE('%Y-%m-%d', m.data_evento) AS d, SUM(m.total_sessoes) AS v
    FROM `{main_table}` m
    JOIN ({dim}) t USING (traffic_sk)
    WHERE t.traffic_group = 'Paid'
      AND m.data_evento IN ({bqh.dates_sql(dates)})
    GROUP BY 1
    """)
    return {r["d"]: r["v"] for r in found}


def _pct(api, real):
    if not real:
        return None
    return round(((api or 0) - real) / real * 100, 2)


def _active_metrics(tables) -> dict:
    return {k: v for k, v in METRICS.items() if METRIC_TABLE[k.split(".")[0]] in tables}


def compare_control(bq: bigquery.Client, control_dates, tables, log: bqh.RunLog) -> list:
    """API (staging) x tabelas atuais nos dias de controle, com diferença percentual."""
    lines = []
    where = lambda col: f"{col} IN ({bqh.dates_sql(control_dates)})"
    comparisons = {}
    for metric, (real_table, stg_table, col, expr) in _active_metrics(tables).items():
        comparisons[metric] = (_totals(bq, stg_table, col, expr, where(col)),
                               _totals(bq, real_table, col, expr, where(col)))
    if "main" in tables:
        comparisons["sessoes_pagas"] = (_paid_sessions(bq, T_STG_MAIN, control_dates, True),
                                        _paid_sessions(bq, T_MAIN, control_dates, False))

    for d in control_dates:
        for metric, (api, real) in comparisons.items():
            row = {"api": api.get(d), "atual": real.get(d), "diferenca_pct": _pct(api.get(d), real.get(d))}
            lines.append((d, metric, row))
            log.add(f"validacao_controle:{metric}", data=d, totais=row)

    if "duser" in tables:
        for d in control_dates:
            top = bqh.rows(bq, f"""
            SELECT user_company, a.total_eventos AS api, r.total_eventos AS atual,
                   COALESCE(a.total_eventos, 0) - COALESCE(r.total_eventos, 0) AS diferenca
            FROM (SELECT * FROM `{T_STG_DUSER_DIARIO}` WHERE data = DATE '{d}') a
            FULL OUTER JOIN (SELECT * FROM `{T_DUSER_DIARIO}` WHERE data = DATE '{d}') r USING (user_company)
            ORDER BY ABS(COALESCE(a.total_eventos, 0) - COALESCE(r.total_eventos, 0)) DESC
            LIMIT 5
            """)
            log.add("validacao_controle:duser.maiores_diferencas", data=d, totais=top)
            lines.append((d, "duser.maiores_diferencas", top))
    return lines


def nearest_neighbors(target: str, candidates) -> list:
    day = datetime.date.fromisoformat(target)
    ordered = sorted(candidates, key=lambda c: (abs((datetime.date.fromisoformat(c) - day).days), c))
    return ordered[:NEIGHBORS]


def compare_neighbors(bq: bigquery.Client, backfill_dates, tables, log: bqh.RunLog) -> list:
    """Totais de cada data do backfill ao lado da mediana dos 7 dias vizinhos com dados reais."""
    lines = []
    first = min(datetime.date.fromisoformat(d) for d in backfill_dates) - datetime.timedelta(days=NEIGHBOR_SEARCH_DAYS)
    last = max(datetime.date.fromisoformat(d) for d in backfill_dates) + datetime.timedelta(days=NEIGHBOR_SEARCH_DAYS)
    excluded = bqh.dates_sql(BACKFILL_DATES)

    for metric, (real_table, stg_table, col, expr) in _active_metrics(tables).items():
        real = _totals(bq, real_table, col, expr,
                       f"{col} BETWEEN DATE '{first}' AND DATE '{last}' AND {col} NOT IN ({excluded})")
        api = _totals(bq, stg_table, col, expr, f"{col} IN ({bqh.dates_sql(backfill_dates)})")
        for d in backfill_dates:
            neighbors = nearest_neighbors(d, real.keys())
            median = statistics.median([real[n] for n in neighbors]) if neighbors else None
            row = {"backfill": api.get(d), "mediana_vizinhos": median,
                   "diferenca_pct": _pct(api.get(d), median), "vizinhos": neighbors}
            lines.append((d, metric, row))
            log.add(f"validacao_vizinhos:{metric}", data=d, totais=row)
    return lines


def print_lines(title: str, lines) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)
    for d, metric, row in lines:
        if isinstance(row, dict):
            shown = {k: v for k, v in row.items() if k != "vizinhos"}
            print(f"{d}  {metric:<32} {shown}")
        else:
            print(f"{d}  {metric}")
            for item in row:
                print(f"      {item}")
