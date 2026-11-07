import os
import re
import uuid
import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension


PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_RAW = os.environ.get("DATASET_RAW")
DATASET_SILVER = os.environ.get("DATASET_SILVER")
RUN_DATE = os.environ.get("RUN_DATE")
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID")
GA4_HOST = os.environ.get("GA4_HOST", "https://www.jota.info")

BQ_LOCATION = "US"
TZ_SP = ZoneInfo("America/Sao_Paulo")

T_GA4_EVENTS_V2 = "ga4_events_v2"
T_FEVENTS_V2 = "fEvents_v2"
T_AG_MAIN_V2 = "fEventos_Agregada_Main_v2"
T_AG_CONT_V2 = "fEventos_Agregada_Conteudo_v2"
T_DUSER_V2 = "dUser_Company_v2"
T_DUSER_DIARIO_V2 = "dUser_Company_diario_v2"
T_USERS_ACTIVE = "usuarios_ativos_v2"
T_USERS_BY_PAGE = "usuarios_paginas_v2"
T_USERS_ACTIVE_MONTH = "usuarios_ativos_mensal_v2"
# Prefixos das tabelas temporárias de staging (uma por execução, com sufixo uuid e expiração automática)
T_TMP_USERS_ACTIVE = "_tmp_usuarios_ativos_v2"
T_TMP_USERS_BY_PAGE = "_tmp_usuarios_paginas_v2"
T_TMP_USERS_ACTIVE_MONTH = "_tmp_usuarios_ativos_mensal_v2"
STAGING_EXPIRATION_HOURS = 6

MISSING_TABLE_ALERT_HOURS = int(os.environ.get("MISSING_TABLE_ALERT_HOURS", "48"))
PENDING_LOOKBACK_DAYS = int(os.environ.get("PENDING_LOOKBACK_DAYS", "5"))

# Casa apenas events_AAAAMMDD; events_intraday_AAAAMMDD fica de fora
EVENTS_TABLE_RE = re.compile(r"^events_(\d{8})$")

SCHEMA_USERS_ACTIVE = [
    bigquery.SchemaField("data", "DATE"),
    bigquery.SchemaField("usuarios_ativos", "INT64"),
]
SCHEMA_USERS_BY_PAGE = [
    bigquery.SchemaField("data", "DATE"),
    bigquery.SchemaField("page_location", "STRING"),
    bigquery.SchemaField("usuarios_ativos", "INT64"),
    bigquery.SchemaField("page_time", "STRING"),
]
SCHEMA_USERS_ACTIVE_MONTH = [
    bigquery.SchemaField("data", "STRING"),
    bigquery.SchemaField("usuarios_ativos", "INT64"),
]


def suffix_to_date(suffix: str) -> str:
    return f"{suffix[:4]}-{suffix[4:6]}-{suffix[6:8]}"


def now_sp() -> datetime.datetime:
    return datetime.datetime.now(TZ_SP)


def run_query(bq: bigquery.Client, sql: str) -> None:
    bq.query(sql, location=BQ_LOCATION).result()


def query_rows(bq: bigquery.Client, sql: str) -> list:
    return list(bq.query(sql, location=BQ_LOCATION).result())


def create_temp_table(bq: bigquery.Client, prefix: str, schema: list) -> str:
    table_id = f"{PROJECT_ID}.{DATASET_SILVER}.{prefix}_{uuid.uuid4().hex}"
    table = bigquery.Table(table_id, schema=schema)
    table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=STAGING_EXPIRATION_HOURS)
    bq.create_table(table)
    return table_id


def load_rows(bq: bigquery.Client, table_id: str, schema: list, rows: list) -> None:
    load_job = bq.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
        location=BQ_LOCATION,
    )
    load_job.result()


def get_active_users_per_day(ga4: BetaAnalyticsDataClient, property_id: str, start_date: str, end_date: str):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=100000,
    )
    response = ga4.run_report(request)
    result = []
    for row in response.rows:
        raw_date = row.dimension_values[0].value
        result.append({
            "data": f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}",
            "usuarios_ativos": int(row.metric_values[0].value),
        })
    return result


def format_engagement_time(seconds: float) -> str:
    total_seconds = int(seconds)
    minutes = total_seconds // 60
    secs = total_seconds % 60
    if minutes > 0:
        return f"{minutes}min {secs:02d}s"
    return f"{total_seconds}s"


USERS_BY_PAGE_PAGE_SIZE = 50000


def iter_active_users_per_page(ga4: BetaAnalyticsDataClient, property_id: str, start_date: str, end_date: str):
    """Gera um lote de linhas por página da Data API.

    Não acumula o dia inteiro em memória: um dia pode ter centenas de milhares de
    páginas distintas e a lista completa (+ serialização JSON no load) estourava a
    memória do container.
    """
    limit = USERS_BY_PAGE_PAGE_SIZE
    offset = 0

    while True:
        batch = []
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="unifiedPagePathScreen"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="userEngagementDuration"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
            offset=offset,
        )
        response = ga4.run_report(request)

        if not response.rows:
            break

        for row in response.rows:
            raw_date = row.dimension_values[0].value
            page_path = row.dimension_values[1].value if len(row.dimension_values) > 1 else None
            page_location = f"{GA4_HOST}{page_path}" if page_path else None
            usuarios_ativos = int(row.metric_values[0].value)
            engagement_duration = float(row.metric_values[1].value)

            if usuarios_ativos > 0:
                page_time = format_engagement_time(engagement_duration / usuarios_ativos)
            else:
                page_time = "0s"

            batch.append({
                "data": f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}",
                "page_location": page_location,
                "usuarios_ativos": usuarios_ativos,
                "page_time": page_time,
            })

        last_page = len(response.rows) < limit
        del response
        yield batch

        if last_page:
            break

        offset += limit


def get_active_users_per_month(ga4: BetaAnalyticsDataClient, property_id: str, start_date: str, end_date: str):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="yearMonth")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=10000,
    )
    response = ga4.run_report(request)
    return [
        {"data": row.dimension_values[0].value, "usuarios_ativos": int(row.metric_values[0].value)}
        for row in response.rows
    ]


def ensure_tables_v2(bq: bigquery.Client) -> None:
    run_query(bq, f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}` (
      event_date_parsed DATE,
      event_timestamp INT64,
      event_name STRING,
      user_pseudo_id STRING,
      platform STRING,
      stream_id STRING,
      event_value_in_usd FLOAT64,
      session_id INT64,
      ga_session_number INT64,
      session_engaged STRING,
      engagement_time_msec INT64,
      is_active_user BOOL,
      param_source STRING,
      param_medium STRING,
      param_campaign STRING,
      user_id_param STRING,
      user_company STRING,
      user_plan STRING,
      is_pro_user_flag STRING,
      page_location STRING,
      page_title STRING,
      geo_continent STRING,
      geo_sub_continent STRING,
      geo_country STRING,
      geo_region STRING,
      geo_city STRING,
      geo_metro STRING,
      device_category STRING,
      device_operating_system STRING,
      device_operating_system_version STRING,
      device_web_info_browser STRING,
      device_browser STRING,
      device_browser_version STRING,
      device_language STRING,
      device_mobile_brand_name STRING,
      device_mobile_model_name STRING
    )
    PARTITION BY event_date_parsed
    CLUSTER BY event_name, user_pseudo_id;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}` (
      event_date_parsed DATE,
      event_timestamp INT64,
      event_ts_utc TIMESTAMP,
      user_pseudo_id STRING,
      platform STRING,
      stream_id STRING,
      event_name STRING,
      event_value_in_usd FLOAT64,
      session_id INT64,
      traffic_sk INT64,
      sk_geo INT64,
      device_sk INT64,
      event_sk INT64,
      page_sk INT64,
      fact_id INT64,
      user_id STRING,
      user_company STRING,
      user_plan STRING,
      is_pro_user_flag STRING,
      traffic_source_source STRING,
      traffic_source_medium STRING,
      traffic_source_name STRING,
      geo_continent STRING,
      geo_sub_continent STRING,
      geo_country STRING,
      geo_region STRING,
      geo_city STRING,
      geo_metro STRING,
      page_url_clean STRING,
      hostname_calculado STRING
    )
    PARTITION BY event_date_parsed
    CLUSTER BY event_name, user_pseudo_id, session_id;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_MAIN_V2}` (
      data_evento DATE,
      event_sk INT64,
      sk_geo INT64,
      device_sk INT64,
      traffic_sk INT64,
      user_company STRING,
      user_plan STRING,
      is_pro_user_flag STRING,
      total_eventos INT64,
      usuarios_unicos_aprox INT64,
      total_sessoes INT64
    )
    PARTITION BY data_evento
    CLUSTER BY event_sk, traffic_sk, user_company;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_CONT_V2}` (
      data_evento DATE,
      page_sk INT64,
      traffic_sk INT64,
      sk_geo INT64,
      user_company STRING,
      is_pro_user_flag STRING,
      pageviews INT64,
      leitores_unicos_aprox INT64
    )
    PARTITION BY data_evento
    CLUSTER BY page_sk, traffic_sk;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_V2}` (
      user_company STRING,
      plano_atual STRING,
      data_primeira_aparicao DATE,
      data_ultima_aparicao DATE,
      total_eventos_historicos INT64,
      tier_cliente STRING,
      segmento_mercado STRING,
      account_manager STRING
    );

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}` (
      user_company STRING,
      data DATE,
      total_eventos INT64,
      ultimo_plano STRING,
      ts_ultimo_plano INT64
    )
    PARTITION BY data
    CLUSTER BY user_company;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE}` (
      data DATE,
      usuarios_ativos INT64
    )
    PARTITION BY data;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_BY_PAGE}` (
      data DATE,
      page_location STRING,
      page_sk INT64,
      usuarios_ativos INT64,
      page_time STRING
    )
    PARTITION BY data
    CLUSTER BY page_location;

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE_MONTH}` (
      data STRING,
      usuarios_ativos INT64
    );
    """)


def upsert_active_users_api(bq: bigquery.Client, ga4: BetaAnalyticsDataClient, suffix: str) -> None:
    if not GA4_PROPERTY_ID:
        raise ValueError("A variável de ambiente GA4_PROPERTY_ID não foi definida.")

    start_date = suffix_to_date(suffix)
    end_date = start_date

    rows = get_active_users_per_day(ga4, property_id=GA4_PROPERTY_ID, start_date=start_date, end_date=end_date)

    if not rows:
        print(f"Nenhum registro de usuários ativos retornado pela API para {start_date}")
        return

    staging_table = create_temp_table(bq, T_TMP_USERS_ACTIVE, SCHEMA_USERS_ACTIVE)
    try:
        load_rows(bq, staging_table, SCHEMA_USERS_ACTIVE, rows)

        run_query(bq, f"""
        MERGE `{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE}` T
        USING (
          SELECT data, usuarios_ativos
          FROM `{staging_table}`
          WHERE data = DATE('{start_date}')
          QUALIFY ROW_NUMBER() OVER (PARTITION BY data ORDER BY usuarios_ativos DESC) = 1
        ) S
        -- Filtro constante na partição do destino: o MERGE lê só o dia, não a tabela inteira
        ON T.data = S.data
          AND T.data = DATE('{start_date}')
        WHEN MATCHED THEN
          UPDATE SET T.usuarios_ativos = S.usuarios_ativos
        WHEN NOT MATCHED THEN
          INSERT (data, usuarios_ativos)
          VALUES (S.data, S.usuarios_ativos);
        """)
    finally:
        bq.delete_table(staging_table, not_found_ok=True)


def upsert_total_users_by_page_api(bq: bigquery.Client, ga4: BetaAnalyticsDataClient, suffix: str) -> None:
    if not GA4_PROPERTY_ID:
        raise ValueError("A variável de ambiente GA4_PROPERTY_ID não foi definida.")

    start_date = suffix_to_date(suffix)
    end_date = start_date

    staging_table = None
    try:
        # Cada página da API é carregada na staging antes de buscar a próxima,
        # mantendo em memória só um lote por vez.
        total_rows = 0
        for batch in iter_active_users_per_page(ga4, property_id=GA4_PROPERTY_ID, start_date=start_date, end_date=end_date):
            if not batch:
                continue
            if staging_table is None:
                staging_table = create_temp_table(bq, T_TMP_USERS_BY_PAGE, SCHEMA_USERS_BY_PAGE)
            load_rows(bq, staging_table, SCHEMA_USERS_BY_PAGE, batch)
            total_rows += len(batch)
            del batch

        if staging_table is None:
            print(f"Nenhum registro de usuários por página retornado pela API para {start_date}")
            return
        print(f"Usuarios por pagina: {total_rows} linhas carregadas na staging para {start_date}")

        # page_location nulo é comparado via COALESCE para que reexecuções não insiram a linha de novo.
        # O filtro constante em T.data faz o MERGE ler só a partição do dia no destino.
        run_query(bq, f"""
        MERGE `{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_BY_PAGE}` T
        USING (
          SELECT
            data,
            page_location,
            ABS(FARM_FINGERPRINT(CONCAT(
              COALESCE(REGEXP_EXTRACT(page_location, r'^https?://([^/]+)'), 'N/A'), '|',
              COALESCE(SPLIT(page_location, '?')[SAFE_OFFSET(0)], 'N/A'), '|',
              COALESCE(REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)'), 'N/A')
            ))) AS page_sk,
            usuarios_ativos,
            page_time
          FROM `{staging_table}`
          WHERE data = DATE('{start_date}')
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data, page_location
            ORDER BY usuarios_ativos DESC, page_time DESC
          ) = 1
        ) S
        ON T.data = S.data
          AND T.data = DATE('{start_date}')
          AND COALESCE(T.page_location, '') = COALESCE(S.page_location, '')
        WHEN MATCHED THEN
          UPDATE SET
            T.usuarios_ativos = S.usuarios_ativos,
            T.page_sk = S.page_sk,
            T.page_time = S.page_time
        WHEN NOT MATCHED THEN
          INSERT (data, page_location, page_sk, usuarios_ativos, page_time)
          VALUES (S.data, S.page_location, S.page_sk, S.usuarios_ativos, S.page_time);
        """)
    finally:
        if staging_table is not None:
            bq.delete_table(staging_table, not_found_ok=True)


def upsert_active_users_monthly_api(bq: bigquery.Client, ga4: BetaAnalyticsDataClient, suffix: str) -> None:
    if not GA4_PROPERTY_ID:
        raise ValueError("GA4_PROPERTY_ID não definido.")

    start_date = f"{suffix[:4]}-{suffix[4:6]}-01"
    year = int(suffix[:4])
    month = int(suffix[4:6])

    if month == 12:
        end_date = f"{year}-12-31"
    else:
        next_month = datetime.date(year, month + 1, 1)
        end_date = (next_month - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    rows = get_active_users_per_month(ga4, property_id=GA4_PROPERTY_ID, start_date=start_date, end_date=end_date)

    if not rows:
        print(f"Nenhum dado mensal retornado para {start_date}")
        return

    target_table = f"{PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE_MONTH}"
    anomes = suffix[:6]

    staging_table = create_temp_table(bq, T_TMP_USERS_ACTIVE_MONTH, SCHEMA_USERS_ACTIVE_MONTH)
    try:
        load_rows(bq, staging_table, SCHEMA_USERS_ACTIVE_MONTH, rows)

        run_query(bq, f"""
        MERGE `{target_table}` T
        USING (
          SELECT data, usuarios_ativos
          FROM `{staging_table}`
          WHERE data = '{anomes}'
          QUALIFY ROW_NUMBER() OVER (PARTITION BY data ORDER BY usuarios_ativos DESC) = 1
        ) S
        ON T.data = S.data
        WHEN MATCHED THEN
          UPDATE SET T.usuarios_ativos = S.usuarios_ativos
        WHEN NOT MATCHED THEN
          INSERT (data, usuarios_ativos)
          VALUES (S.data, S.usuarios_ativos);
        """)
    finally:
        bq.delete_table(staging_table, not_found_ok=True)


def load_data_api(bq: bigquery.Client, ga4: BetaAnalyticsDataClient, suffix: str) -> None:
    upsert_active_users_api(bq, ga4, suffix)
    upsert_total_users_by_page_api(bq, ga4, suffix)
    upsert_active_users_monthly_api(bq, ga4, suffix)


def list_events_suffixes(bq: bigquery.Client) -> list:
    suffixes = []
    for table in bq.list_tables(f"{PROJECT_ID}.{DATASET_RAW}"):
        match = EVENTS_TABLE_RE.match(table.table_id)
        if match:
            suffixes.append(match.group(1))
    return sorted(suffixes)


def window_suffixes(lookback_days: int) -> list:
    """Dias de (ontem - lookback_days + 1) até ontem, em ordem cronológica."""
    yesterday = now_sp().date() - datetime.timedelta(days=1)
    return [
        (yesterday - datetime.timedelta(days=i)).strftime("%Y%m%d")
        for i in range(lookback_days - 1, -1, -1)
    ]


def loaded_suffixes(bq: bigquery.Client, first_suffix: str, last_suffix: str) -> set:
    # Um dia só conta como carregado se chegou até a última tabela do process_day,
    # para que um retry após falha no meio do processamento refaça o dia.
    rows = query_rows(bq, f"""
    SELECT FORMAT_DATE('%Y%m%d', event_date_parsed) AS suffix
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}`
    WHERE event_date_parsed BETWEEN PARSE_DATE('%Y%m%d', '{first_suffix}') AND PARSE_DATE('%Y%m%d', '{last_suffix}')
    GROUP BY 1
    INTERSECT DISTINCT
    SELECT FORMAT_DATE('%Y%m%d', data_evento) AS suffix
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_CONT_V2}`
    WHERE data_evento BETWEEN PARSE_DATE('%Y%m%d', '{first_suffix}') AND PARSE_DATE('%Y%m%d', '{last_suffix}')
    GROUP BY 1
    """)
    return {row["suffix"] for row in rows}


def pending_days(bq: bigquery.Client, lookback_days: int = 14) -> list:
    """Tabelas events_AAAAMMDD da janela que ainda não foram carregadas, em ordem cronológica."""
    window = window_suffixes(lookback_days)
    available = set(list_events_suffixes(bq)) & set(window)
    if not available:
        return []
    loaded = loaded_suffixes(bq, window[0], window[-1])
    return sorted(available - loaded)


def hours_since_day_end(suffix: str) -> float:
    day = datetime.datetime.strptime(suffix, "%Y%m%d").date()
    day_end = datetime.datetime.combine(day + datetime.timedelta(days=1), datetime.time(0, 0), tzinfo=TZ_SP)
    return (now_sp() - day_end).total_seconds() / 3600


def check_missing_tables(bq: bigquery.Client, lookback_days: int) -> list:
    """Avisa sobre events_ ausentes dentro da tolerância e devolve erros das que passaram do prazo."""
    available = list_events_suffixes(bq)
    errors = []
    for suffix in window_suffixes(lookback_days):
        if suffix in available or (available and suffix < available[0]):
            continue
        source = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"
        hours = hours_since_day_end(suffix)
        if hours > MISSING_TABLE_ALERT_HOURS:
            errors.append(
                f"Tabela de origem {source} ausente há {hours:.0f}h "
                f"(tolerância MISSING_TABLE_ALERT_HOURS={MISSING_TABLE_ALERT_HOURS}h)"
            )
        else:
            print(
                f"AVISO: tabela de origem {source} ainda não exportada ({hours:.0f}h após o fim do dia, "
                f"tolerância {MISSING_TABLE_ALERT_HOURS}h). Será processada numa próxima execução."
            )
    return errors


def duser_daily_insert_sql(fevents_filter: str) -> str:
    """Resumo por empresa e dia de fEvents_v2: contagem e último plano preenchido com seu timestamp."""
    return f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}` (
      user_company, data, total_eventos, ultimo_plano, ts_ultimo_plano
    )
    SELECT
      user_company,
      event_date_parsed AS data,
      COUNT(*) AS total_eventos,
      ARRAY_AGG(user_plan IGNORE NULLS ORDER BY event_timestamp DESC, user_plan DESC LIMIT 1)[SAFE_OFFSET(0)] AS ultimo_plano,
      MAX(IF(user_plan IS NOT NULL, event_timestamp, NULL)) AS ts_ultimo_plano
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
    WHERE {fevents_filter}
      AND user_company IS NOT NULL
    GROUP BY user_company, event_date_parsed;
    """


def duser_merge_sql(company_filter: str) -> str:
    """Recalcula valores absolutos de dUser_Company_v2 a partir do resumo diário (sem soma incremental)."""
    return f"""
    MERGE `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_V2}` T
    USING (
      SELECT
        user_company,
        ARRAY_AGG(ultimo_plano IGNORE NULLS ORDER BY ts_ultimo_plano DESC, ultimo_plano DESC LIMIT 1)[SAFE_OFFSET(0)] AS plano_atual,
        MIN(data) AS data_primeira_aparicao,
        MAX(data) AS data_ultima_aparicao,
        SUM(total_eventos) AS total_eventos_historicos
      FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}`
      WHERE {company_filter}
      GROUP BY 1
    ) S
    ON T.user_company = S.user_company
    WHEN MATCHED THEN UPDATE SET
      T.plano_atual = COALESCE(S.plano_atual, T.plano_atual),
      T.data_primeira_aparicao = S.data_primeira_aparicao,
      T.data_ultima_aparicao = S.data_ultima_aparicao,
      T.total_eventos_historicos = S.total_eventos_historicos
    WHEN NOT MATCHED THEN INSERT (
      user_company, plano_atual, data_primeira_aparicao, data_ultima_aparicao, total_eventos_historicos,
      tier_cliente, segmento_mercado, account_manager
    ) VALUES (
      S.user_company, S.plano_atual, S.data_primeira_aparicao, S.data_ultima_aparicao, S.total_eventos_historicos,
      NULL, NULL, NULL
    );
    """


def upsert_duser_company(bq: bigquery.Client, suffix: str) -> None:
    """Regrava o resumo do dia e recalcula dUser só para as empresas afetadas, lendo apenas o dia de fEvents_v2."""
    day_date_expr = f"PARSE_DATE('%Y%m%d', '{suffix}')"
    daily_table = f"{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}"

    # Empresas que estavam no dia antes da regravação também são recalculadas,
    # para o caso de o reprocessamento removê-las do dia.
    run_query(bq, f"""
    DECLARE empresas ARRAY<STRING> DEFAULT ARRAY(
      SELECT DISTINCT user_company FROM `{daily_table}` WHERE data = {day_date_expr}
    );

    BEGIN TRANSACTION;

    DELETE FROM `{daily_table}`
    WHERE data = {day_date_expr};
    {duser_daily_insert_sql(f"event_date_parsed = {day_date_expr}")}
    SET empresas = ARRAY_CONCAT(empresas, ARRAY(
      SELECT DISTINCT user_company FROM `{daily_table}` WHERE data = {day_date_expr}
    ));
    {duser_merge_sql("user_company IN UNNEST(empresas)")}
    COMMIT TRANSACTION;
    """)


def ensure_duser_daily_backfill(bq: bigquery.Client) -> None:
    """Na primeira execução, preenche o resumo diário com todo o histórico e corrige dUser inteira."""
    daily_table = f"{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}"
    # COUNT(*) sem filtro é respondido pelos metadados (0 bytes)
    rows = query_rows(bq, f"SELECT COUNT(*) AS n FROM `{daily_table}`")
    if rows[0]["n"] > 0:
        return

    print(f"{T_DUSER_DIARIO_V2} vazia: carga inicial a partir de todo o histórico de {T_FEVENTS_V2}...")
    run_query(bq, f"""
    BEGIN TRANSACTION;

    DELETE FROM `{daily_table}` WHERE TRUE;
    {duser_daily_insert_sql("TRUE")}
    {duser_merge_sql("TRUE")}
    COMMIT TRANSACTION;
    """)
    print("Carga inicial concluida")


def process_day(bq: bigquery.Client, suffix: str) -> None:
    day_date_expr = f"PARSE_DATE('%Y%m%d', '{suffix}')"
    source_events_day = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"

    run_query(bq, f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}`
    WHERE event_date_parsed = {day_date_expr};

    INSERT INTO `{PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}` (
      event_date_parsed,
      event_timestamp,
      event_name,
      user_pseudo_id,
      platform,
      stream_id,
      event_value_in_usd,
      session_id,
      ga_session_number,
      session_engaged,
      engagement_time_msec,
      is_active_user,
      param_source,
      param_medium,
      param_campaign,
      user_id_param,
      user_company,
      user_plan,
      is_pro_user_flag,
      page_location,
      geo_continent,
      geo_sub_continent,
      geo_country,
      geo_region,
      geo_city,
      geo_metro,
      device_category,
      device_operating_system,
      device_operating_system_version,
      device_web_info_browser,
      device_browser,
      device_browser_version,
      device_language,
      device_mobile_brand_name,
      device_mobile_model_name,
      page_title
    )
    SELECT
      PARSE_DATE('%Y%m%d', e.event_date) AS event_date_parsed,
      e.event_timestamp,
      e.event_name,
      e.user_pseudo_id,
      e.platform,
      e.stream_id,
      SAFE_CAST(e.event_value_in_usd AS FLOAT64) AS event_value_in_usd,

      (SELECT COALESCE(ep.value.int_value, SAFE_CAST(ep.value.string_value AS INT64))
      FROM UNNEST(e.event_params) ep
      WHERE ep.key = 'ga_session_id'
      LIMIT 1) AS session_id,

      (SELECT COALESCE(ep.value.int_value, SAFE_CAST(ep.value.string_value AS INT64))
      FROM UNNEST(e.event_params) ep
      WHERE ep.key = 'ga_session_number'
      LIMIT 1) AS ga_session_number,

      (SELECT COALESCE(
                ep.value.string_value,
                CAST(ep.value.int_value AS STRING)
              )
      FROM UNNEST(e.event_params) ep
      WHERE ep.key = 'session_engaged'
      LIMIT 1) AS session_engaged,

      (SELECT COALESCE(ep.value.int_value, SAFE_CAST(ep.value.string_value AS INT64))
      FROM UNNEST(e.event_params) ep
      WHERE ep.key = 'engagement_time_msec'
      LIMIT 1) AS engagement_time_msec,

      e.is_active_user AS is_active_user,

      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'source' LIMIT 1) AS param_source,
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'medium' LIMIT 1) AS param_medium,
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'campaign' LIMIT 1) AS param_campaign,

      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'JOTA_USERID' LIMIT 1) AS user_id_param,
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'JOTA_COMPANY' LIMIT 1) AS user_company,
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'JOTA_Planos' LIMIT 1) AS user_plan,
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'JOTA_isPro' LIMIT 1) AS is_pro_user_flag,

      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_location' LIMIT 1) AS page_location,

      e.geo.continent AS geo_continent,
      e.geo.sub_continent AS geo_sub_continent,
      e.geo.country AS geo_country,
      e.geo.region AS geo_region,
      e.geo.city AS geo_city,
      e.geo.metro AS geo_metro,

      e.device.category AS device_category,
      e.device.operating_system AS device_operating_system,
      e.device.operating_system_version AS device_operating_system_version,
      e.device.web_info.browser AS device_web_info_browser,
      e.device.browser AS device_browser,
      e.device.browser_version AS device_browser_version,
      e.device.language AS device_language,
      e.device.mobile_brand_name AS device_mobile_brand_name,
      e.device.mobile_model_name AS device_mobile_model_name,

      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_title' LIMIT 1) AS page_title
    FROM `{source_events_day}` e
    WHERE PARSE_DATE('%Y%m%d', e.event_date) = {day_date_expr};
    """)

    run_query(bq, f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
    WHERE event_date_parsed = {day_date_expr};

    INSERT INTO `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
    WITH base AS (
      SELECT
        g.*,
        LAST_VALUE(param_source IGNORE NULLS) OVER(
          PARTITION BY user_pseudo_id, session_id
          ORDER BY event_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS source_final,
        LAST_VALUE(param_medium IGNORE NULLS) OVER(
          PARTITION BY user_pseudo_id, session_id
          ORDER BY event_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS medium_final,
        LAST_VALUE(param_campaign IGNORE NULLS) OVER(
          PARTITION BY user_pseudo_id, session_id
          ORDER BY event_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS campaign_final
      FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}` g
      WHERE event_date_parsed = {day_date_expr}
    )
    SELECT
      event_date_parsed,
      event_timestamp,
      TIMESTAMP_MICROS(event_timestamp) AS event_ts_utc,

      user_pseudo_id,
      platform,
      stream_id,
      event_name,
      event_value_in_usd,

      session_id,

      ABS(FARM_FINGERPRINT(CONCAT(
        COALESCE(source_final, '(direct)'), '|',
        COALESCE(medium_final, '(none)'), '|',
        COALESCE(campaign_final, '(not set)')
      ))) AS traffic_sk,

      ABS(FARM_FINGERPRINT(CONCAT(
        COALESCE(geo_continent, 'N/A'), '|',
        COALESCE(geo_sub_continent, 'N/A'), '|',
        COALESCE(geo_country, 'N/A'), '|',
        COALESCE(geo_region, 'N/A'), '|',
        COALESCE(geo_city, 'N/A'), '|',
        COALESCE(geo_metro, 'N/A')
      ))) AS sk_geo,

      ABS(FARM_FINGERPRINT(CONCAT(
        COALESCE(device_category, 'N/A'), '|',
        COALESCE(device_operating_system, 'N/A'), '|',
        COALESCE(device_operating_system_version, 'N/A'), '|',
        COALESCE(device_web_info_browser, 'N/A'), '|',
        COALESCE(device_browser, 'N/A'), '|',
        COALESCE(device_browser_version, 'N/A'), '|',
        COALESCE(device_language, 'N/A'), '|',
        COALESCE(device_mobile_brand_name, 'N/A'), '|',
        COALESCE(device_mobile_model_name, 'N/A')
      ))) AS device_sk,

      ABS(FARM_FINGERPRINT(COALESCE(event_name, 'Unknown'))) AS event_sk,

      ABS(FARM_FINGERPRINT(CONCAT(
        COALESCE(REGEXP_EXTRACT(page_location, r'^https?://([^/]+)'), 'N/A'), '|',
        COALESCE(SPLIT(page_location, '?')[SAFE_OFFSET(0)], 'N/A'), '|',
        COALESCE(REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)'), 'N/A')
      ))) AS page_sk,

      ABS(FARM_FINGERPRINT(CONCAT(user_pseudo_id, CAST(event_timestamp AS STRING), event_name))) AS fact_id,

      COALESCE(MAX(user_id_param) OVER(PARTITION BY user_pseudo_id), user_id_param) AS user_id,
      user_company,
      user_plan,
      is_pro_user_flag,

      source_final AS traffic_source_source,
      medium_final AS traffic_source_medium,
      campaign_final AS traffic_source_name,

      geo_continent, geo_sub_continent, geo_country, geo_region, geo_city, geo_metro,

      SPLIT(page_location, '?')[SAFE_OFFSET(0)] AS page_url_clean,
      REGEXP_EXTRACT(page_location, r'^https?://([^/]+)') AS hostname_calculado
    FROM base;
    """)

    # dUser roda antes das agregadas: fEventos_Agregada_Conteudo_v2 é a última tabela gravada e
    # marca o dia como carregado (ver loaded_suffixes), então uma falha aqui faz o retry refazer o dia.
    upsert_duser_company(bq, suffix)

    run_query(bq, f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_MAIN_V2}`
    WHERE data_evento = {day_date_expr};

    INSERT INTO `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_MAIN_V2}`
    SELECT
      event_date_parsed AS data_evento,
      event_sk, sk_geo, device_sk, traffic_sk,
      user_company, user_plan, is_pro_user_flag,
      COUNT(*) AS total_eventos,
      COUNT(DISTINCT user_pseudo_id) AS usuarios_unicos_aprox,
      SUM(CASE WHEN event_name='session_start' THEN 1 ELSE 0 END) AS total_sessoes
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
    WHERE event_date_parsed = {day_date_expr}
    GROUP BY 1,2,3,4,5,6,7,8;
    """)

    run_query(bq, f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_CONT_V2}`
    WHERE data_evento = {day_date_expr};

    INSERT INTO `{PROJECT_ID}.{DATASET_SILVER}.{T_AG_CONT_V2}`
    WITH user_attrs AS (
      SELECT
        user_pseudo_id,
        event_date_parsed,
        MAX(user_company) AS empresa_encontrada,
        MAX(is_pro_user_flag) AS status_pro_encontrado
      FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
      WHERE event_date_parsed = {day_date_expr}
        AND (user_company IS NOT NULL OR is_pro_user_flag IS NOT NULL)
      GROUP BY 1,2
    )
    SELECT
      t1.event_date_parsed AS data_evento,
      t1.page_sk, t1.traffic_sk, t1.sk_geo,
      COALESCE(t2.empresa_encontrada, t1.user_company, 'N/A') AS user_company,
      COALESCE(t2.status_pro_encontrado, t1.is_pro_user_flag, 'false') AS is_pro_user_flag,
      COUNT(*) AS pageviews,
      COUNT(DISTINCT t1.user_pseudo_id) AS leitores_unicos_aprox
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}` t1
    LEFT JOIN user_attrs t2
      ON t1.user_pseudo_id = t2.user_pseudo_id
     AND t1.event_date_parsed = t2.event_date_parsed
    WHERE t1.event_date_parsed = {day_date_expr}
      AND t1.event_name = 'page_view'
    GROUP BY 1,2,3,4,5,6;
    """)


def table_exists(bq: bigquery.Client, table_id: str) -> bool:
    try:
        bq.get_table(table_id)
        return True
    except NotFound:
        return False


def run(bq: bigquery.Client, ga4: BetaAnalyticsDataClient) -> None:
    if RUN_DATE and not re.fullmatch(r"\d{8}", RUN_DATE):
        raise ValueError(f"RUN_DATE inválido: {RUN_DATE!r}. Use o formato AAAAMMDD.")

    if RUN_DATE:
        api_suffix = RUN_DATE
    else:
        api_suffix = (now_sp().date() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    print("=" * 100)
    print("Inicio do processamento GA4")
    print(f"Modo: {'RUN_DATE=' + RUN_DATE if RUN_DATE else 'dias pendentes'}")
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset RAW: {DATASET_RAW}")
    print(f"Dataset SILVER: {DATASET_SILVER}")
    print("=" * 100)

    print("Garantindo existencia das tabelas de destino...")
    ensure_tables_v2(bq)
    ensure_duser_daily_backfill(bq)
    print("Tabelas validadas/criadas com sucesso")

    # A Data API não depende da exportação; uma falha nela é reportada no final,
    # sem impedir o processamento dos eventos.
    errors = []
    print(f"Carregando GA4 Data API para {suffix_to_date(api_suffix)}...")
    try:
        load_data_api(bq, ga4, api_suffix)
        print("Cargas da Data API concluidas")
    except Exception as exc:
        print(f"ERRO nas cargas da Data API: {exc!r}")
        errors.append(f"Falha nas cargas da Data API para {api_suffix}: {exc!r}")

    if RUN_DATE:
        source_events_day = f"{PROJECT_ID}.{DATASET_RAW}.events_{RUN_DATE}"
        if not table_exists(bq, source_events_day):
            errors.append(f"Tabela de origem nao encontrada para RUN_DATE={RUN_DATE}: {source_events_day}")
            raise RuntimeError("\n".join(errors))
        days = [RUN_DATE]
    else:
        days = pending_days(bq, PENDING_LOOKBACK_DAYS)
        print(f"Dias pendentes: {days or 'nenhum'}")

    for suffix in days:
        print(f"Processando {suffix_to_date(suffix)} (events_{suffix})...")
        process_day(bq, suffix)
        print(f"Dia {suffix_to_date(suffix)} concluido")

    if not RUN_DATE:
        errors.extend(check_missing_tables(bq, PENDING_LOOKBACK_DAYS))

    if errors:
        raise RuntimeError("Execucao concluida com erros:\n" + "\n".join(errors))

    print("Processamento finalizado com sucesso")
    print(f"Dias processados: {[suffix_to_date(s) for s in days]}")
    print("Tabelas de destino:")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_GA4_EVENTS_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_AG_MAIN_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_AG_CONT_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_DIARIO_V2}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_USERS_BY_PAGE}")
    print(f"  {PROJECT_ID}.{DATASET_SILVER}.{T_USERS_ACTIVE_MONTH}")


def main():
    bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    ga4 = BetaAnalyticsDataClient()
    run(bq, ga4)


if __name__ == "__main__":
    main()
