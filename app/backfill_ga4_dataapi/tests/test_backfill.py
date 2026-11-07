import re
import sys
from unittest.mock import MagicMock

import pytest
from google.analytics.data_v1beta.types import (
    DimensionValue,
    MetricValue,
    PropertyQuota,
    QuotaStatus,
    ResponseMetaData,
    Row,
    RunReportResponse,
)
from google.api_core.exceptions import Forbidden, NotFound

from backfill_ga4_dataapi import api, cli, config, downstream, staging

FINAL_TABLES = (config.T_MAIN, config.T_CONTEUDO, config.T_DUSER_DIARIO, config.T_DUSER,
                config.T_DIM_TRAFEGO, config.T_FATO_SESSOES, config.T_FATO_PERFORMANCE_MIDIA)

VIEW_SQL = """SELECT DISTINCT traffic_sk, traffic_source_source AS source, traffic_source_medium AS medium,
  traffic_source_name AS campaign, IF(traffic_source_medium = 'cpc', 'Paid', 'Organic') AS traffic_group,
  'GA4' AS origem_ga4, CONCAT(traffic_source_source, ' / ', traffic_source_medium) AS source_medium
FROM `jota-dados-integracao-ga4.ga4_metrics_us.fEvents_v2`"""


class FakeBQ:
    """Registra o SQL executado e responde consultas por trecho do texto."""

    def __init__(self, existing_tables=(), responses=None):
        self.sql = []
        self.existing = set(existing_tables)
        self.responses = responses or {}
        self.loads = []

    def query(self, sql, location=None):
        self.sql.append(sql)
        job = MagicMock()
        result = []
        for marker, value in self.responses.items():
            if marker in sql:
                result = value(sql) if callable(value) else value
                break
        job.result.return_value = result
        return job

    def get_table(self, table_id):
        if table_id not in self.existing:
            raise NotFound(table_id)
        return MagicMock()

    def load_table_from_json(self, rows, table_id, job_config=None, location=None):
        self.loads.append((table_id, list(rows)))
        return MagicMock()

    def writes_to(self, table):
        pattern = re.compile(rf"(DELETE FROM|INSERT INTO|MERGE( INTO)?|TRUNCATE TABLE)\s+`{re.escape(table)}`")
        return [s for s in self.sql if pattern.search(s)]


def api_response(rows, row_count=None, other=False, thresholding=False):
    return RunReportResponse(
        rows=[Row(dimension_values=[DimensionValue(value=v) for v in dims],
                  metric_values=[MetricValue(value=str(m)) for m in mets]) for dims, mets in rows],
        row_count=len(rows) if row_count is None else row_count,
        metadata=ResponseMetaData(data_loss_from_other_row=other, subject_to_thresholding=thresholding),
        property_quota=PropertyQuota(tokens_per_day=QuotaStatus(consumed=10, remaining=1990),
                                     tokens_per_hour=QuotaStatus(consumed=10, remaining=390)),
    )


def fake_ga4():
    """Uma linha por requisição, com o número certo de dimensões e métricas."""
    client = MagicMock()

    def run_report(request):
        d = request.date_ranges[0].start_date.replace("-", "")
        dims = [d if dim.name == "date" else f"v_{dim.name}" for dim in request.dimensions]
        mets = [5 for _ in request.metrics]
        return api_response([(dims, mets)])

    client.run_report.side_effect = run_report
    return client


# --- argumentos e travas -------------------------------------------------------

def test_lista_fechada_tem_as_15_datas():
    assert len(config.BACKFILL_DATES) == 15
    assert config.BACKFILL_DATES[0] == "2026-10-04" and config.BACKFILL_DATES[6] == "2026-10-10"
    assert config.BACKFILL_DATES[7] == "2026-10-26" and config.BACKFILL_DATES[-1] == "2026-11-02"
    assert "2026-11-05" not in config.BACKFILL_DATES


def test_padrao_e_dry_run_com_todas_as_datas_e_tabelas():
    args = cli.parse_args([])
    assert args.execute is False
    assert args.dates == list(config.BACKFILL_DATES)
    assert args.tables == list(config.TABLES)


@pytest.mark.parametrize("argv", [
    ["--dates", "2026-11-05"],
    ["--dates", "2026-10-04", "2026-10-11"],
    ["--control-dates", "2026-10-05"],
    ["--tables", "main", "usuarios_ativos_v2"],
])
def test_argumentos_invalidos_sao_rejeitados(argv):
    with pytest.raises(SystemExit):
        cli.parse_args(argv)


def test_subconjunto_de_datas_e_tabelas():
    args = cli.parse_args(["--dates", "2026-10-26", "2026-10-04", "--tables", "conteudo,main"])
    assert args.dates == ["2026-10-04", "2026-10-26"]
    assert args.tables == ["main", "conteudo"]


def test_aborta_se_existe_tabela_raw():
    bq = FakeBQ(existing_tables={"jota-dados-integracao-ga4.analytics_420210992.events_20261005"})
    with pytest.raises(RuntimeError, match="events_20261005"):
        cli.assert_dates_writable(bq, ["2026-10-04", "2026-10-05"])


def test_aborta_se_ga4_events_v2_tem_linhas():
    bq = FakeBQ(responses={"ga4_events_v2": [{"d": "2026-10-30", "n": 12}]})
    with pytest.raises(RuntimeError, match="2026-10-30: ga4_events_v2 tem 12 linhas"):
        cli.assert_dates_writable(bq, ["2026-10-30"])


def test_datas_livres_passam_na_trava():
    cli.assert_dates_writable(FakeBQ(), list(config.BACKFILL_DATES))


# --- Data API -------------------------------------------------------------------

def test_fetch_report_pagina_converte_not_set_e_registra_other(monkeypatch):
    monkeypatch.setattr(api, "PAGE_SIZE", 2)
    spec = api.REPORTS["duser"]
    pages = [
        api_response([(["20261004", "ACME", "(not set)"], [10]), (["20261004", "(other)", "Pro"], [3])],
                     row_count=3, other=True),
        api_response([(["20261004", "(not set)", "Basic"], [7])], row_count=3, thresholding=True),
    ]
    client = MagicMock()
    client.run_report.side_effect = pages

    result = api.fetch_report(client, spec, "2026-10-04")

    assert result.requisicoes == 2
    offsets = [c.args[0].offset for c in client.run_report.call_args_list]
    assert offsets == [0, 2]
    assert all(c.args[0].return_property_quota for c in client.run_report.call_args_list)
    assert result.rows[0] == {"data": "2026-10-04", "user_company": "ACME", "user_plan": None, "event_count": 10}
    assert result.rows[2]["user_company"] is None
    assert result.linhas_other == 1 and result.eventos_other == 3
    assert {"data_loss_from_other_row", "subject_to_thresholding"} <= result.observacoes
    assert result.tokens_dia_restantes == 1990


def test_clean_converte_jota_vazio_em_null_e_preserva_outras_dimensoes():
    assert api._clean("customEvent:JOTA_COMPANY", "") is None
    assert api._clean("customEvent:JOTA_Planos", "") is None
    assert api._clean("customEvent:JOTA_COMPANY", "ACME") == "ACME"
    # fora das dimensões JOTA_*, '' segue como veio (source/medium têm COALESCE próprio)
    assert api._clean("customEvent:source", "") == ""


def test_relatorio_de_conteudo_filtra_page_view_e_respeita_9_dimensoes():
    request = api._request(api.REPORTS["conteudo"], "2026-10-04", 0)
    assert request.dimension_filter.filter.field_name == "eventName"
    assert request.dimension_filter.filter.string_filter.value == "page_view"
    for spec in api.REPORTS.values():
        assert len(spec.dimensions) <= 9


# --- SQL de staging -------------------------------------------------------------

def test_chaves_de_desconhecido_seguem_a_formula():
    assert config.SK_GEO_DESCONHECIDO_SQL.count("'N/A'") == 6
    assert config.DEVICE_SK_DESCONHECIDO_SQL.count("'N/A'") == 9


def test_hashes_sao_calculados_no_bigquery():
    sql = staging.prepare_main_sql(["2026-10-04"])
    assert "ABS(FARM_FINGERPRINT(COALESCE(event_name,'Unknown')))" in sql
    assert "COALESCE(source,'(direct)')" in sql and "COALESCE(campaign,'(not set)')" in sql
    assert "SUM(IF(event_name = 'session_start', event_count, 0)) AS total_sessoes" in sql
    conteudo = staging.prepare_conteudo_sql(["2026-10-04"])
    assert "COALESCE(user_company, 'N/A')" in conteudo and "COALESCE(is_pro_user_flag, 'false')" in conteudo
    duser = staging.prepare_duser_sql(["2026-10-04"])
    assert "user_company IS NOT NULL" in duser and "CAST(NULL AS INT64) AS ts_ultimo_plano" in duser


@pytest.mark.parametrize("ref", [
    "`jota-dados-integracao-ga4.ga4_metrics_us.fEvents_v2`",
    "`jota-dados-integracao-ga4`.`ga4_metrics_us`.`fEvents_v2`",
    "ga4_metrics_us.fEvents_v2",
    "`ga4_metrics_us.fEvents_v2`",
])
def test_view_reapontada_para_staging(ref):
    rewritten = staging.rewrite_view(f"SELECT traffic_sk FROM {ref} f;", config.T_STG_TRAFEGO_FEVENTS)
    assert rewritten == f"SELECT traffic_sk FROM `{config.T_STG_TRAFEGO_FEVENTS}` f"


def test_view_sem_fevents_aborta():
    with pytest.raises(RuntimeError, match="Não foi possível reapontar"):
        staging.rewrite_view("SELECT * FROM `x.y.outra_tabela`", config.T_STG_TRAFEGO_FEVENTS)


# --- downstream -----------------------------------------------------------------

def test_sessoes_troca_janela_d3_pelas_datas_com_delete_previo():
    script = downstream.sessoes_script(["2026-10-04", "2026-10-05"])
    assert downstream.SESSOES_WINDOW not in script
    assert script.count("IN (DATE '2026-10-04', DATE '2026-10-05')") == 3  # DELETE + 2 janelas
    assert script.index("BEGIN TRANSACTION") < script.index("DELETE FROM") < script.index("MERGE INTO") \
        < script.index("COMMIT TRANSACTION")
    assert "{" not in script


def test_sessoes_aborta_se_sql_original_mudou(monkeypatch):
    monkeypatch.setattr(downstream, "sessoes_template", lambda: "MERGE INTO x USING y ON TRUE")
    with pytest.raises(RuntimeError, match="0 ocorrências"):
        downstream.sessoes_script(["2026-10-04"])


def test_gold_uma_execucao_por_faixa_continua():
    scripts = downstream.gold_scripts(config.BACKFILL_DATES)
    assert len(scripts) == 2
    assert "BETWEEN '2026-10-04' AND '2026-10-10'" in scripts[0]
    assert "BETWEEN '2026-10-26' AND '2026-11-02'" in scripts[1]
    assert downstream.contiguous_ranges(["2026-10-04", "2026-10-06", "2026-10-07"]) == [
        ("2026-10-04", "2026-10-04"), ("2026-10-06", "2026-10-07")]


def test_copias_sql_iguais_a_producao_passam():
    production = downstream.render(downstream.sessoes_template()).replace("\n", "\n\n")
    bq = FakeBQ(responses={"JOBS_BY_PROJECT": [{"query": production}]})
    notes = downstream.verify_against_production(bq, ["sessoes"])
    assert "idêntico" in notes[0]


def test_copia_sql_diferente_de_producao_aborta():
    bq = FakeBQ(responses={"JOBS_BY_PROJECT": [{"query": "MERGE INTO outra coisa"}]})
    with pytest.raises(RuntimeError, match="difere"):
        downstream.verify_against_production(bq, ["gold"])


def test_sem_permissao_em_jobs_so_avisa():
    bq = FakeBQ()
    bq.query = MagicMock(side_effect=Forbidden("sem acesso"))
    notes = downstream.verify_against_production(bq, ["sessoes"])
    assert "não verificado" in notes[0]


# --- dry-run e execute ------------------------------------------------------------

def dry_run_bq():
    return FakeBQ(responses={
        "INFORMATION_SCHEMA.VIEWS": [{"view_definition": VIEW_SQL}],
        "sem_dimensao": [{"sem_dimensao": 0}],
        f"SELECT COUNT(*) AS n FROM `{config.T_STG_TRAFEGO}`": [{"n": 3}],
    })


def test_dry_run_nao_grava_nas_tabelas_finais():
    bq = dry_run_bq()
    cli.main(["--dates", "2026-10-04", "2026-10-26"], bq=bq, ga4=fake_ga4())

    for table in FINAL_TABLES:
        assert bq.writes_to(table) == [], table
    loaded = {t for t, _ in bq.loads}
    assert {config.T_RAW_MAIN, config.T_RAW_CONTEUDO, config.T_RAW_DUSER, config.T_LOG} <= loaded
    # Dias de controle também buscados, apenas para staging
    # Uma carga por dia (não acumula todas as datas em memória)
    raw_main_loads = [rows for t, rows in bq.loads if t == config.T_RAW_MAIN]
    assert [{r["data"] for r in rows} for rows in raw_main_loads] == [
        {"2026-10-04"}, {"2026-10-12"}, {"2026-10-20"}, {"2026-10-26"}]
    assert bq.writes_to(config.T_STG_MAIN) and bq.writes_to(config.T_STG_TRAFEGO)
    log_rows = next(rows for t, rows in bq.loads if t == config.T_LOG)
    assert {r["modo"] for r in log_rows} == {"dry-run"}
    assert any(r["tabela"].startswith("validacao_controle:") for r in log_rows)
    assert any(r["tabela"].startswith("validacao_vizinhos:") for r in log_rows)


def test_dry_run_aborta_antes_da_api_se_data_existe_na_raw():
    bq = FakeBQ(existing_tables={"jota-dados-integracao-ga4.analytics_420210992.events_20261004"})
    ga4 = fake_ga4()
    with pytest.raises(RuntimeError, match="Abortado"):
        cli.main(["--dates", "2026-10-04"], bq=bq, ga4=ga4)
    ga4.run_report.assert_not_called()


def execute_bq(other=0, staging_rows=True):
    def counts(sql):
        if config.T_GA4_EVENTS in sql:  # a trava continua vendo ga4_events_v2 vazia
            return []
        return [{"d": d, "n": 10} for d in config.BACKFILL_DATES if f"DATE '{d}'" in sql]

    responses = {
        "STARTS_WITH(tabela, 'api:')": [{"run_id": "dry1", "d": "2026-10-04", "tabela": "api:main", "linhas_other": other}],
        "JOBS_BY_PROJECT": [],
        "SELECT COUNT(*) AS n FROM": [{"n": 2}],
    }
    if staging_rows:
        responses["COUNT(*) AS n\n    FROM"] = counts
    return FakeBQ(responses=responses)


def test_execute_grava_por_data_em_transacao_sem_chamar_api(monkeypatch):
    # main.py pode já ter sido importado por outra suíte com outro PROJECT_ID
    mirror = sys.modules.get("main")
    if mirror is not None:
        monkeypatch.setattr(mirror, "PROJECT_ID", config.PROJECT_ID)
        monkeypatch.setattr(mirror, "DATASET_SILVER", config.DATASET_GA4)
    bq = execute_bq()
    ga4 = fake_ga4()
    cli.main(["--execute", "--dates", "2026-10-04", "2026-10-05"], bq=bq, ga4=ga4)

    ga4.run_report.assert_not_called()
    main_writes = bq.writes_to(config.T_MAIN)
    assert len(main_writes) == 2
    for sql, d in zip(main_writes, ["2026-10-04", "2026-10-05"]):
        assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql
        assert f"DELETE FROM `{config.T_MAIN}` WHERE data_evento = DATE '{d}'" in sql

    duser = bq.writes_to(config.T_DUSER)[0]
    assert "user_company IN UNNEST(empresas)" in duser
    assert not re.search(r"T\.total_eventos_historicos\s*\+", duser)

    assert bq.writes_to(config.T_DIM_TRAFEGO)
    assert "NOT IN (SELECT traffic_sk FROM" in bq.writes_to(config.T_DIM_TRAFEGO)[0]
    assert bq.writes_to(config.T_FATO_SESSOES) and bq.writes_to(config.T_FATO_PERFORMANCE_MIDIA)


def test_execute_aborta_com_other_sem_allow_other():
    with pytest.raises(RuntimeError, match=r"\(other\)"):
        cli.main(["--execute", "--dates", "2026-10-04"], bq=execute_bq(other=4))


def test_execute_aborta_sem_staging():
    bq = execute_bq(staging_rows=False)
    with pytest.raises(RuntimeError, match="Rode o dry-run"):
        cli.main(["--execute", "--dates", "2026-10-04", "--tables", "main"], bq=bq)
    assert bq.writes_to(config.T_MAIN) == []


def test_execute_sem_dry_run_aborta():
    bq = FakeBQ()
    with pytest.raises(RuntimeError, match="Nenhum dry-run"):
        cli.main(["--execute", "--dates", "2026-10-04"], bq=bq)
