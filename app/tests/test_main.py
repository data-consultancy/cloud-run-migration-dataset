import datetime
import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound

import main


def fake_bq(events_suffixes=(), loaded=(), other_tables=(), daily_rows=1):
    """Cliente BigQuery falso: lista tabelas RAW, devolve dias carregados e registra o SQL executado."""
    bq = MagicMock()
    tables = [f"events_{s}" for s in events_suffixes] + list(other_tables)
    bq.list_tables.side_effect = lambda dataset: [SimpleNamespace(table_id=t) for t in tables]

    def get_table(table_id):
        if table_id.split(".")[-1] not in tables:
            raise NotFound(table_id)
        return MagicMock()

    bq.get_table.side_effect = get_table

    def query(sql, location=None):
        job = MagicMock()
        if "INTERSECT DISTINCT" in sql:
            job.result.return_value = [{"suffix": s} for s in loaded]
        elif "COUNT(*) AS n" in sql:
            job.result.return_value = [{"n": daily_rows}]
        else:
            job.result.return_value = []
        return job

    bq.query.side_effect = query
    return bq


def executed_sql(bq):
    return [c.args[0] for c in bq.query.call_args_list]


def duser_merge_sql(bq):
    stmts = [s for s in executed_sql(bq) if f"MERGE `proj-teste.silver_teste.{main.T_DUSER_V2}`" in s]
    assert len(stmts) == 1
    return stmts[0]


@pytest.fixture
def frozen_now(monkeypatch):
    def freeze(*args):
        moment = datetime.datetime(*args, tzinfo=main.TZ_SP)
        monkeypatch.setattr(main, "now_sp", lambda: moment)
    return freeze


@pytest.fixture
def data_api(monkeypatch):
    calls = []
    for name in ("upsert_active_users_api", "upsert_total_users_by_page_api", "upsert_active_users_monthly_api"):
        monkeypatch.setattr(main, name, lambda bq, ga4, suffix, _n=name: calls.append((_n, suffix)))
    return calls


@pytest.fixture
def processed(monkeypatch):
    days = []
    monkeypatch.setattr(main, "process_day", lambda bq, suffix: days.append(suffix))
    return days


# --- dUser_Company_v2 ---------------------------------------------------------

def test_duser_merge_mesmo_sql_em_duas_execucoes_sem_soma_incremental():
    bq1, bq2 = fake_bq(), fake_bq()
    main.process_day(bq1, "20261101")
    main.process_day(bq2, "20261101")

    sql1, sql2 = duser_merge_sql(bq1), duser_merge_sql(bq2)
    assert sql1 == sql2

    assert not re.search(r"T\.total_eventos_historicos\s*\+", sql1)
    assert "T.total_eventos_historicos = S.total_eventos_historicos" in sql1
    assert "T.data_primeira_aparicao = S.data_primeira_aparicao" in sql1
    assert "T.data_ultima_aparicao = S.data_ultima_aparicao" in sql1
    assert "LEAST(" not in sql1 and "GREATEST(" not in sql1
    # Plano do evento mais recente com plano preenchido: por dia e depois entre os dias
    assert "ARRAY_AGG(user_plan IGNORE NULLS ORDER BY event_timestamp DESC" in sql1
    assert "ARRAY_AGG(ultimo_plano IGNORE NULLS ORDER BY ts_ultimo_plano DESC" in sql1
    assert "[SAFE_OFFSET(0)] AS plano_atual" in sql1
    assert "T.plano_atual = COALESCE(S.plano_atual, T.plano_atual)" in sql1
    # Classificações manuais não são tocadas em empresas existentes
    update_clause = sql1.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    for col in ("tier_cliente", "segmento_mercado", "account_manager"):
        assert col not in update_clause


def test_duser_le_apenas_o_dia_de_fevents():
    bq = fake_bq()
    main.process_day(bq, "20261101")
    sql = duser_merge_sql(bq)
    fevents = f"`proj-teste.silver_teste.{main.T_FEVENTS_V2}`"
    daily = f"`proj-teste.silver_teste.{main.T_DUSER_DIARIO_V2}`"

    # fEvents_v2 aparece uma única vez no script, filtrada pelo dia
    assert sql.count(fevents) == 1
    after_fevents = sql.split(fevents)[1]
    assert after_fevents.lstrip().startswith("WHERE event_date_parsed = PARSE_DATE('%Y%m%d', '20261101')")

    # O resumo diário é regravado só no dia, numa transação junto com o MERGE
    assert f"DELETE FROM {daily}\n    WHERE data = PARSE_DATE('%Y%m%d', '20261101')" in sql
    assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM") < sql.index("MERGE") < sql.index("COMMIT TRANSACTION")

    # dUser é recalculada a partir do resumo diário, só para as empresas afetadas
    source = sql.split("USING (")[1].split(") S\n")[0]
    assert daily in source
    assert "SUM(total_eventos) AS total_eventos_historicos" in source
    assert "WHERE user_company IN UNNEST(empresas)" in source


def test_duser_roda_antes_das_agregadas():
    bq = fake_bq()
    main.process_day(bq, "20261101")
    stmts = executed_sql(bq)
    idx_duser = next(i for i, s in enumerate(stmts) if f"MERGE `proj-teste.silver_teste.{main.T_DUSER_V2}`" in s)
    idx_main = next(i for i, s in enumerate(stmts) if f"INSERT INTO `proj-teste.silver_teste.{main.T_AG_MAIN_V2}`" in s)
    idx_cont = next(i for i, s in enumerate(stmts) if f"INSERT INTO `proj-teste.silver_teste.{main.T_AG_CONT_V2}`" in s)
    assert idx_duser < idx_main < idx_cont
    assert idx_cont == len(stmts) - 1


def test_carga_inicial_quando_resumo_diario_vazio():
    bq = fake_bq(daily_rows=0)
    main.ensure_duser_daily_backfill(bq)
    stmts = executed_sql(bq)
    assert len(stmts) == 2
    backfill = stmts[1]
    assert "BEGIN TRANSACTION" in backfill and "COMMIT TRANSACTION" in backfill
    assert "WHERE TRUE\n      AND user_company IS NOT NULL" in backfill
    assert f"MERGE `proj-teste.silver_teste.{main.T_DUSER_V2}`" in backfill


def test_sem_carga_inicial_quando_resumo_diario_preenchido():
    bq = fake_bq(daily_rows=1234)
    main.ensure_duser_daily_backfill(bq)
    stmts = executed_sql(bq)
    assert len(stmts) == 1
    assert "COUNT(*) AS n" in stmts[0]


# --- Staging da Data API ------------------------------------------------------

@pytest.mark.parametrize("func, getter, rows", [
    ("upsert_active_users_api", "get_active_users_per_day",
     [{"data": "2026-11-01", "usuarios_ativos": 10}]),
    ("upsert_total_users_by_page_api", "get_active_users_per_page",
     [{"data": "2026-11-01", "page_location": "https://x/a", "usuarios_ativos": 3, "page_time": "5s"}]),
    ("upsert_active_users_monthly_api", "get_active_users_per_month",
     [{"data": "202611", "usuarios_ativos": 99}]),
])
def test_staging_exclusiva_por_execucao(monkeypatch, func, getter, rows):
    monkeypatch.setattr(main, "GA4_PROPERTY_ID", "123")
    monkeypatch.setattr(main, getter, lambda *a, **k: rows)

    names = []
    for _ in range(2):
        bq = fake_bq()
        getattr(main, func)(bq, MagicMock(), "20261101")

        created = bq.create_table.call_args.args[0]
        assert created.expires is not None
        assert created.expires > datetime.datetime.now(datetime.timezone.utc)
        assert "_stg_" not in created.table_id
        assert re.search(r"_tmp_\w+_[0-9a-f]{32}$", created.table_id)

        assert bq.load_table_from_json.call_args.args[1].endswith(created.table_id)
        bq.delete_table.assert_called_once()
        assert bq.delete_table.call_args.args[0].endswith(created.table_id)

        merge = [s for s in executed_sql(bq) if "MERGE" in s]
        assert len(merge) == 1
        assert "QUALIFY ROW_NUMBER() OVER" in merge[0]
        assert created.table_id in merge[0]
        assert not any("_stg_" in s or s.lstrip().startswith("DELETE") for s in executed_sql(bq))

        names.append(created.table_id)

    assert names[0] != names[1]


@pytest.mark.parametrize("func, getter, rows", [
    ("upsert_active_users_api", "get_active_users_per_day",
     [{"data": "2026-11-01", "usuarios_ativos": 10}]),
    ("upsert_total_users_by_page_api", "get_active_users_per_page",
     [{"data": "2026-11-01", "page_location": "https://x/a", "usuarios_ativos": 3, "page_time": "5s"}]),
])
def test_merge_diario_le_so_a_particao_do_dia_no_destino(monkeypatch, func, getter, rows):
    monkeypatch.setattr(main, "GA4_PROPERTY_ID", "123")
    monkeypatch.setattr(main, getter, lambda *a, **k: rows)
    bq = fake_bq()
    getattr(main, func)(bq, MagicMock(), "20261101")
    merge = next(s for s in executed_sql(bq) if "MERGE" in s)
    on_clause = merge.split(") S")[1].split("WHEN MATCHED")[0]
    assert "AND T.data = DATE('2026-11-01')" in on_clause


def test_staging_removida_mesmo_se_merge_falhar(monkeypatch):
    monkeypatch.setattr(main, "GA4_PROPERTY_ID", "123")
    monkeypatch.setattr(main, "get_active_users_per_day", lambda *a, **k: [{"data": "2026-11-01", "usuarios_ativos": 1}])
    bq = fake_bq()
    bq.query.side_effect = RuntimeError("falha no MERGE")
    with pytest.raises(RuntimeError):
        main.upsert_active_users_api(bq, MagicMock(), "20261101")
    bq.delete_table.assert_called_once()


def test_ensure_tables_nao_cria_mais_staging_fixa():
    bq = fake_bq()
    main.ensure_tables_v2(bq)
    sql = executed_sql(bq)[0]
    assert "_stg_" not in sql
    assert f"CREATE TABLE IF NOT EXISTS `proj-teste.silver_teste.{main.T_DUSER_DIARIO_V2}`" in sql


# --- pending_days -------------------------------------------------------------

def test_pending_days_ignora_intraday_e_dias_carregados(frozen_now):
    frozen_now(2026, 11, 3, 9, 0)
    bq = fake_bq(
        events_suffixes=["20261001", "20261028", "20261030", "20261031", "20261101", "20261102"],
        loaded=["20261028", "20261101"],
        other_tables=["events_intraday_20261103", "events_intraday_20261102", "pseudonymous_users_20261102"],
    )
    # 20261001 está fora da janela de 14 dias
    assert main.pending_days(bq, lookback_days=14) == ["20261030", "20261031", "20261102"]


def test_pending_days_sem_tabelas_na_janela_nao_consulta_bq(frozen_now):
    frozen_now(2026, 11, 3, 9, 0)
    bq = fake_bq(other_tables=["events_intraday_20261102"])
    assert main.pending_days(bq) == []
    bq.query.assert_not_called()


# --- run(): Data API e tabela ausente -----------------------------------------

def test_data_api_roda_quando_events_nao_existe(monkeypatch, frozen_now, data_api, processed):
    monkeypatch.setattr(main, "RUN_DATE", None)
    frozen_now(2026, 11, 3, 9, 0)
    bq = fake_bq(events_suffixes=["20261101"], loaded=["20261101"])

    main.run(bq, MagicMock())

    assert [s for _, s in data_api] == ["20261102"] * 3
    assert processed == []


def test_run_date_processa_so_aquele_dia(monkeypatch, frozen_now, data_api, processed):
    monkeypatch.setattr(main, "RUN_DATE", "20261020")
    frozen_now(2026, 11, 3, 9, 0)
    bq = fake_bq(events_suffixes=["20261020", "20261030", "20261102"])

    main.run(bq, MagicMock())

    assert processed == ["20261020"]
    assert [s for _, s in data_api] == ["20261020"] * 3


def test_sem_run_date_processa_pendentes_em_ordem(monkeypatch, frozen_now, data_api, processed):
    monkeypatch.setattr(main, "RUN_DATE", None)
    frozen_now(2026, 11, 3, 9, 0)
    bq = fake_bq(events_suffixes=["20261031", "20261029", "20261102", "20261030", "20261101"], loaded=["20261030"])

    main.run(bq, MagicMock())

    assert processed == ["20261029", "20261031", "20261101", "20261102"]


def test_tabela_ausente_dentro_da_tolerancia_termina_com_sucesso(monkeypatch, frozen_now, data_api, processed, capsys):
    monkeypatch.setattr(main, "RUN_DATE", None)
    monkeypatch.setattr(main, "MISSING_TABLE_ALERT_HOURS", 48)
    frozen_now(2026, 11, 3, 9, 0)  # events_20261102 está 9h atrasada
    bq = fake_bq(events_suffixes=["20261031", "20261101"], loaded=["20261031"])

    main.run(bq, MagicMock())

    assert processed == ["20261101"]
    assert len(data_api) == 3
    assert "AVISO" in capsys.readouterr().out


def test_tabela_ausente_fora_da_tolerancia_falha_depois_das_cargas(monkeypatch, frozen_now, data_api, processed):
    monkeypatch.setattr(main, "RUN_DATE", None)
    monkeypatch.setattr(main, "MISSING_TABLE_ALERT_HOURS", 48)
    # events_20261102 nunca chegou: em 05/11 09:00 está 57h atrasada; 20261103 está 33h (aviso)
    frozen_now(2026, 11, 5, 9, 0)
    bq = fake_bq(events_suffixes=["20261031", "20261101", "20261104"], loaded=["20261031"])

    with pytest.raises(RuntimeError) as err:
        main.run(bq, MagicMock())

    assert "events_20261102" in str(err.value)
    assert "events_20261103" not in str(err.value)
    # Data API e dias pendentes foram concluídos antes do erro
    assert [s for _, s in data_api] == ["20261104"] * 3
    assert processed == ["20261101", "20261104"]


def test_run_date_com_tabela_ausente_falha_imediatamente(monkeypatch, frozen_now, data_api, processed):
    monkeypatch.setattr(main, "RUN_DATE", "20261102")
    frozen_now(2026, 11, 3, 9, 0)  # dentro da tolerância, mas RUN_DATE não tem tolerância
    bq = fake_bq(events_suffixes=["20261101"])

    with pytest.raises(RuntimeError, match="events_20261102"):
        main.run(bq, MagicMock())

    assert processed == []
    assert len(data_api) == 3


def test_falha_da_data_api_nao_impede_eventos(monkeypatch, frozen_now, processed):
    monkeypatch.setattr(main, "RUN_DATE", None)
    frozen_now(2026, 11, 3, 9, 0)

    def boom(*a):
        raise RuntimeError("quota")

    monkeypatch.setattr(main, "load_data_api", boom)
    bq = fake_bq(events_suffixes=["20261102"])

    with pytest.raises(RuntimeError, match="Data API"):
        main.run(bq, MagicMock())
    assert processed == ["20261102"]
