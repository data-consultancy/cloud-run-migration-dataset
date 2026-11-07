"""Orquestração do backfill: travas, dry-run (padrão) e --execute."""
import argparse
import datetime
import sys
import uuid
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.cloud import bigquery

from backfill_ga4_dataapi import api, downstream, staging, validate
from backfill_ga4_dataapi import bq as bqh
from backfill_ga4_dataapi.config import (
    BACKFILL_DATES,
    BQ_LOCATION,
    CONTROL_DATES,
    DATASET_GA4,
    DATASET_RAW,
    PROJECT_ID,
    T_CONTEUDO,
    T_DIM_TRAFEGO,
    T_DUSER_DIARIO,
    T_FATO_PERFORMANCE_MIDIA,
    T_FATO_SESSOES,
    T_GA4_EVENTS,
    T_LOG,
    T_MAIN,
    T_STG_CONTEUDO,
    T_STG_DUSER_DIARIO,
    T_STG_MAIN,
    T_STG_TRAFEGO,
    TABLES,
)

# Relatórios da API necessários para cada destino
REPORTS_BY_TABLE = {
    "main": {"main"},
    "conteudo": {"conteudo"},
    "duser": {"duser"},
    "trafego": {"main", "conteudo"},
    "sessoes": {"main"},
    "gold": set(),
}

MAIN_COLS = ("data_evento, event_sk, sk_geo, device_sk, traffic_sk, user_company, user_plan, "
             "is_pro_user_flag, total_eventos, usuarios_unicos_aprox, total_sessoes")
CONTEUDO_COLS = ("data_evento, page_sk, traffic_sk, sk_geo, user_company, is_pro_user_flag, "
                 "pageviews, leitores_unicos_aprox")
DUSER_DIARIO_COLS = "user_company, data, total_eventos, ultimo_plano, ts_ultimo_plano"
TRAFEGO_COLS = "traffic_sk, source, medium, campaign, traffic_group, origem_ga4, source_medium"

# (nome, tabela atual, coluna de data) para o relatório de linhas existentes
CURRENT_TABLES = (
    ("main", T_MAIN, "data_evento"),
    ("conteudo", T_CONTEUDO, "data_evento"),
    ("duser", T_DUSER_DIARIO, "data"),
    ("sessoes", T_FATO_SESSOES, "data"),
    ("gold", T_FATO_PERFORMANCE_MIDIA, "`Date`"),
)


def _split(values) -> list:
    return sorted({v.strip() for value in values for v in value.split(",") if v.strip()})


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_ga4_dataapi",
        description="Backfill descartável das tabelas agregadas do GA4 via Data API (dry-run por padrão).",
    )
    parser.add_argument("--dates", nargs="+", default=list(BACKFILL_DATES),
                        help="Subconjunto da lista fechada de datas (AAAA-MM-DD). Padrão: todas as 15.")
    parser.add_argument("--tables", nargs="+", default=list(TABLES),
                        help=f"Destinos a processar: {', '.join(TABLES)}. Padrão: todos.")
    parser.add_argument("--control-dates", nargs="+", default=list(CONTROL_DATES),
                        help="Dias com dados reais para comparar com a API (só no dry-run).")
    parser.add_argument("--execute", action="store_true",
                        help="Grava nas tabelas finais a partir da staging do último dry-run.")
    parser.add_argument("--conteudo-sem-trafego", action="store_true",
                        help="Busca o relatório de conteúdo sem source/medium/campaign (usar se houver (other)).")
    parser.add_argument("--allow-other", action="store_true",
                        help="Permite --execute mesmo com linhas (other) no último dry-run.")
    parser.add_argument("--allow-sampled", action="store_true",
                        help="Permite --execute mesmo com respostas amostradas da API no último dry-run.")
    args = parser.parse_args(argv)

    args.dates = _split(args.dates)
    requested_tables = _split(args.tables)
    args.tables = [t for t in TABLES if t in requested_tables]
    args.control_dates = _split(args.control_dates)

    unknown_tables = set(requested_tables) - set(TABLES)
    if unknown_tables:
        parser.error(f"Tabelas desconhecidas: {sorted(unknown_tables)}. Use: {', '.join(TABLES)}")
    rejected = [d for d in args.dates if d not in BACKFILL_DATES]
    if rejected:
        parser.error(f"Datas fora da lista fechada do backfill: {rejected}")
    overlap = [d for d in args.control_dates if d in BACKFILL_DATES]
    if overlap:
        parser.error(f"Dias de controle precisam ter dados reais, não podem ser datas do backfill: {overlap}")
    return args


def assert_dates_writable(bq: bigquery.Client, dates) -> None:
    """Aborta se alguma data existir na raw ou em ga4_events_v2: nesses casos o fluxo normal trata a data."""
    problems = []
    for d in dates:
        raw_table = f"{PROJECT_ID}.{DATASET_RAW}.events_{d.replace('-', '')}"
        if bqh.table_exists(bq, raw_table):
            problems.append(f"{d}: existe {raw_table}")
    for r in bqh.rows(bq, f"""
    SELECT FORMAT_DATE('%Y-%m-%d', event_date_parsed) AS d, COUNT(*) AS n
    FROM `{T_GA4_EVENTS}`
    WHERE event_date_parsed IN ({bqh.dates_sql(dates)})
    GROUP BY 1
    """):
        problems.append(f"{r['d']}: ga4_events_v2 tem {r['n']} linhas")
    if problems:
        raise RuntimeError("Abortado: datas que devem ser tratadas pelo fluxo normal:\n  " + "\n  ".join(problems))


def _mirror_module():
    """main.py do bq-mirror-job, importado só para reutilizar o MERGE de dUser_Company_v2."""
    src = str(Path(__file__).resolve().parents[1] / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    import main as mirror

    if (mirror.PROJECT_ID, mirror.DATASET_SILVER) != (PROJECT_ID, DATASET_GA4):
        raise RuntimeError(
            f"PROJECT_ID/DATASET_SILVER do ambiente ({mirror.PROJECT_ID}/{mirror.DATASET_SILVER}) "
            f"diferem de {PROJECT_ID}/{DATASET_GA4}"
        )
    return mirror


def _counts(bq: bigquery.Client, table: str, col: str, dates) -> dict:
    found = bqh.rows(bq, f"""
    SELECT FORMAT_DATE('%Y-%m-%d', {col}) AS d, COUNT(*) AS n
    FROM `{table}`
    WHERE {col} IN ({bqh.dates_sql(dates)})
    GROUP BY 1
    """)
    return {r["d"]: r["n"] for r in found}


# --- dry-run ------------------------------------------------------------------

def fetch_reports(bq, ga4, reports, dates, sem_trafego: bool, log: bqh.RunLog) -> None:
    for report in sorted(reports):
        spec = api.REPORTS["conteudo_sem_trafego" if report == "conteudo" and sem_trafego else report]
        # Carrega um dia por vez: acumular todas as datas em memória estourava o container.
        staging.clear_raw(bq, report, dates)
        for d in dates:
            result = api.fetch_report(ga4, spec, d)
            staging.append_raw(bq, report, result.rows)
            totais = {
                "eventos": result.eventos,
                "linhas_other": result.linhas_other,
                "eventos_other": result.eventos_other,
                "requisicoes": result.requisicoes,
                "tokens_dia_restantes": result.tokens_dia_restantes,
                "tokens_hora_restantes": result.tokens_hora_restantes,
            }
            log.add(f"api:{report}", data=d, linhas=len(result.rows), totais=totais,
                    observacoes=sorted(result.observacoes) + ([f"relatorio={spec.name}"] if spec.name != report else []))
            print(f"API {spec.name:<22} {d}: {len(result.rows):>7} linhas, {result.eventos:>10} eventos"
                  f"{'  ' + ', '.join(sorted(result.observacoes)) if result.observacoes else ''}")
            del result


def dry_run(bq, ga4, args, log: bqh.RunLog) -> None:
    for note in downstream.verify_against_production(bq, args.tables):
        print(note)
        log.add("verificacao_sql", observacoes=note)

    reports = set().union(*(REPORTS_BY_TABLE[t] for t in args.tables))
    fetch_dates = sorted(set(args.dates) | set(args.control_dates))
    fetch_reports(bq, ga4, reports, fetch_dates, args.conteudo_sem_trafego, log)

    if "main" in reports:
        bqh.run(bq, staging.prepare_main_sql(fetch_dates))
    if "conteudo" in reports:
        bqh.run(bq, staging.prepare_conteudo_sql(fetch_dates))
    if "duser" in reports:
        bqh.run(bq, staging.prepare_duser_sql(fetch_dates))
    if "trafego" in args.tables:
        bqh.run(bq, staging.prepare_trafego_fevents_sql(fetch_dates))
        bqh.run(bq, staging.prepare_trafego_sql(staging.read_view_definition(bq), args.dates))
        novos = bqh.rows(bq, f"SELECT COUNT(*) AS n FROM `{T_STG_TRAFEGO}`")[0]["n"]
        sem_dim = bqh.rows(bq, staging.missing_traffic_sk_sql(args.dates))[0]["sem_dimensao"]
        log.add("staging:trafego", linhas=novos, totais={"novos_traffic_sk": novos, "sem_dimensao": sem_dim})
        print(f"dim_trafego_ga4: {novos} traffic_sk novos a inserir; {sem_dim} continuariam sem dimensão")

    print("=" * 100)
    print("Linhas atuais nas tabelas finais (serão substituídas nas datas do backfill)")
    for name, table, col in CURRENT_TABLES:
        if name in args.tables:
            current = _counts(bq, table, col, args.dates)
            for d in args.dates:
                log.add(f"atual:{name}", data=d, linhas=current.get(d, 0))
            print(f"  {name:<10} {sum(current.values()):>9} linhas em {len(current)} das {len(args.dates)} datas")

    for name, table, col in (("main", T_STG_MAIN, "data_evento"), ("conteudo", T_STG_CONTEUDO, "data_evento"),
                             ("duser", T_STG_DUSER_DIARIO, "data")):
        if name in args.tables:
            counts = _counts(bq, table, col, args.dates)
            for d in args.dates:
                log.add(f"staging:{name}", data=d, linhas=counts.get(d, 0))

    validate.print_lines("Dias de controle: API x tabelas atuais",
                         validate.compare_control(bq, args.control_dates, args.tables, log))
    validate.print_lines("Datas do backfill x mediana dos vizinhos (mesmo dia da semana, sem feriados e eleições)",
                         validate.compare_neighbors(bq, args.dates, args.tables, log))

    if "sessoes" in args.tables:
        paid = validate._paid_sessions(bq, T_STG_MAIN, args.dates, True)
        for d in args.dates:
            log.add("previa:fato_sessoes_ga4", data=d, totais={"sessoes_pagas": paid.get(d)})
        print(f"Prévia fato_sessoes_ga4: {sum(v or 0 for v in paid.values())} sessões pagas nas datas do backfill")
    if "gold" in args.tables:
        ranges = downstream.contiguous_ranges(args.dates)
        log.add("previa:fato_performance_midia", totais={"faixas": ranges})
        print(f"Prévia fato_performance_midia: recalcula as faixas {ranges}")

    print("=" * 100)
    print(f"DRY-RUN concluído. Nada foi gravado fora de {DATASET_GA4}._backfill_dataapi_*.")
    print(f"Resumo gravado em {T_LOG} (run_id={log.run_id}).")


# --- execute ------------------------------------------------------------------

def _require_staging(bq, table: str, col: str, dates, name: str) -> None:
    counts = _counts(bq, table, col, dates)
    missing = [d for d in dates if not counts.get(d)]
    if missing:
        raise RuntimeError(f"Staging de {name} sem linhas para {missing}. Rode o dry-run antes do --execute.")


def _check_other(bq, dates, allow_other: bool, allow_sampled: bool = False) -> str:
    latest = bqh.rows(bq, f"""
    SELECT run_id, FORMAT_DATE('%Y-%m-%d', data) AS d, tabela,
           CAST(JSON_VALUE(totais, '$.linhas_other') AS INT64) AS linhas_other,
           observacoes
    FROM `{T_LOG}`
    WHERE modo = 'dry-run' AND STARTS_WITH(tabela, 'api:') AND data IN ({bqh.dates_sql(dates)})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data, tabela ORDER BY executado_em DESC) = 1
    """)
    if not latest:
        raise RuntimeError("Nenhum dry-run encontrado em _backfill_dataapi_log para essas datas.")
    with_other = [f"{r['d']} {r['tabela']}: {r['linhas_other']}" for r in latest if r["linhas_other"]]
    if with_other and not allow_other:
        raise RuntimeError("O último dry-run teve linhas (other):\n  " + "\n  ".join(with_other)
                           + "\nUse --conteudo-sem-trafego no dry-run ou --allow-other para aceitar.")
    sampled = [f"{r['d']} {r['tabela']}" for r in latest if "amostrado" in (r.get("observacoes") or "")]
    if sampled and not allow_sampled:
        raise RuntimeError("O último dry-run teve respostas amostradas da API (distribuição por dimensão estimada):\n  "
                           + "\n  ".join(sampled)
                           + "\nTire essas datas de --dates ou use --allow-sampled para aceitar.")
    return ", ".join(sorted({r["run_id"] for r in latest}))


def _per_date_replace(bq, target: str, source: str, col: str, cols: str, dates, name: str, log) -> None:
    for d in dates:
        bqh.run(bq, f"""
        BEGIN TRANSACTION;
        DELETE FROM `{target}` WHERE {col} = DATE '{d}';
        INSERT INTO `{target}` ({cols})
        SELECT {cols} FROM `{source}` WHERE {col} = DATE '{d}';
        COMMIT TRANSACTION;
        """)
    for d, n in sorted(_counts(bq, target, col, dates).items()):
        log.add(name, data=d, linhas=n)
        print(f"  {name:<28} {d}: {n} linhas")


def duser_script(mirror, dates) -> str:
    d = bqh.dates_sql(dates)
    per_date = "\n".join(f"""
    DELETE FROM `{T_DUSER_DIARIO}` WHERE data = DATE '{day}';
    INSERT INTO `{T_DUSER_DIARIO}` ({DUSER_DIARIO_COLS})
    SELECT {DUSER_DIARIO_COLS} FROM `{T_STG_DUSER_DIARIO}` WHERE data = DATE '{day}';""" for day in dates)
    # Mesma lógica do bq-mirror-job: recalcula valores absolutos só para as empresas afetadas
    return f"""
    DECLARE empresas ARRAY<STRING> DEFAULT ARRAY(
      SELECT DISTINCT user_company FROM `{T_DUSER_DIARIO}` WHERE data IN ({d})
    );

    BEGIN TRANSACTION;
    {per_date}

    SET empresas = ARRAY_CONCAT(empresas, ARRAY(
      SELECT DISTINCT user_company FROM `{T_DUSER_DIARIO}` WHERE data IN ({d})
    ));
    {mirror.duser_merge_sql("user_company IN UNNEST(empresas)")}
    COMMIT TRANSACTION;
    """


def execute(bq, args, log: bqh.RunLog) -> None:
    dates, tables = args.dates, args.tables
    base = _check_other(bq, dates, args.allow_other, args.allow_sampled)
    print(f"Gravando a partir da staging do dry-run {base}")

    if "main" in tables:
        _require_staging(bq, T_STG_MAIN, "data_evento", dates, "main")
    if "conteudo" in tables:
        _require_staging(bq, T_STG_CONTEUDO, "data_evento", dates, "conteudo")
    if "duser" in tables:
        _require_staging(bq, T_STG_DUSER_DIARIO, "data", dates, "duser")
        mirror = _mirror_module()

    # Revalida imediatamente antes de gravar
    assert_dates_writable(bq, dates)

    if "main" in tables:
        _per_date_replace(bq, T_MAIN, T_STG_MAIN, "data_evento", MAIN_COLS, dates, "fEventos_Agregada_Main_v2", log)
    if "conteudo" in tables:
        _per_date_replace(bq, T_CONTEUDO, T_STG_CONTEUDO, "data_evento", CONTEUDO_COLS, dates,
                          "fEventos_Agregada_Conteudo_v2", log)
    if "duser" in tables:
        bqh.run(bq, duser_script(mirror, dates))
        for d, n in sorted(_counts(bq, T_DUSER_DIARIO, "data", dates).items()):
            log.add("dUser_Company_diario_v2", data=d, linhas=n)
        log.add("dUser_Company_v2", observacoes="recalculada para as empresas das datas do backfill")
        print("  dUser_Company_diario_v2 e dUser_Company_v2 atualizadas")
    if "trafego" in tables:
        novos = bqh.rows(bq, f"""
        SELECT COUNT(*) AS n FROM `{T_STG_TRAFEGO}`
        WHERE traffic_sk NOT IN (SELECT traffic_sk FROM `{T_DIM_TRAFEGO}` WHERE traffic_sk IS NOT NULL)
        """)[0]["n"]
        bqh.run(bq, f"""
        BEGIN TRANSACTION;
        INSERT INTO `{T_DIM_TRAFEGO}` ({TRAFEGO_COLS})
        SELECT {TRAFEGO_COLS} FROM `{T_STG_TRAFEGO}`
        WHERE traffic_sk NOT IN (SELECT traffic_sk FROM `{T_DIM_TRAFEGO}` WHERE traffic_sk IS NOT NULL);
        COMMIT TRANSACTION;
        """)
        log.add("dim_trafego_ga4", linhas=novos, observacoes="traffic_sk ausentes inseridos")
        print(f"  dim_trafego_ga4: {novos} traffic_sk inseridos")
    if "sessoes" in tables:
        main_counts = _counts(bq, T_MAIN, "data_evento", dates)
        missing = [d for d in dates if not main_counts.get(d)]
        if missing:
            raise RuntimeError(f"fEventos_Agregada_Main_v2 sem linhas para {missing}; grave 'main' antes de 'sessoes'.")
        bqh.run(bq, downstream.sessoes_script(dates))
        for d, n in sorted(_counts(bq, T_FATO_SESSOES, "data", dates).items()):
            log.add("fato_sessoes_ga4", data=d, linhas=n)
            print(f"  {'fato_sessoes_ga4':<28} {d}: {n} linhas")
    if "gold" in tables:
        for script in downstream.gold_scripts(dates):
            bqh.run(bq, script)
        for d, n in sorted(_counts(bq, T_FATO_PERFORMANCE_MIDIA, "`Date`", dates).items()):
            log.add("fato_performance_midia", data=d, linhas=n)
            print(f"  {'fato_performance_midia':<28} {d}: {n} linhas")

    print(f"EXECUTE concluído. Log em {T_LOG} (run_id={log.run_id}).")


def main(argv=None, bq: bigquery.Client = None, ga4: BetaAnalyticsDataClient = None) -> None:
    args = parse_args(argv)
    modo = "execute" if args.execute else "dry-run"
    run_id = f"{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    bq = bq or bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    log = bqh.RunLog(run_id, modo)

    print("=" * 100)
    print(f"backfill_ga4_dataapi | modo={modo} | run_id={run_id}")
    print(f"datas={args.dates}")
    print(f"tabelas={args.tables}")
    print("=" * 100)

    assert_dates_writable(bq, args.dates)
    staging.ensure_tables(bq)
    try:
        if args.execute:
            execute(bq, args, log)
        else:
            dry_run(bq, ga4 or BetaAnalyticsDataClient(), args, log)
    except Exception as exc:
        log.add("erro", observacoes=repr(exc))
        raise
    finally:
        log.flush(bq)
