import os
import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_RAW = os.environ.get("DATASET_RAW")
DATASET_SILVER = os.environ.get("DATASET_SILVER")
RUN_DATE = os.environ.get("RUN_DATE")

BQ_LOCATION = "US"
TZ_SP = ZoneInfo("America/Sao_Paulo")

T_GA4_EVENTS_V2 = "ga4_events_v2"
T_FEVENTS_V2 = "fEvents_v2"
T_AG_MAIN_V2 = "fEventos_Agregada_Main_v2"
T_AG_CONT_V2 = "fEventos_Agregada_Conteudo_v2"
T_DUSER_V2 = "dUser_Company_v2"


def run_query(bq: bigquery.Client, sql: str) -> None:
    bq.query(sql, location=BQ_LOCATION).result()


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
    """)

    run_query(bq, f"""
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
    """)

    run_query(bq, f"""
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
    """)

    run_query(bq, f"""
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
    """)

    run_query(bq, f"""
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
    """)


def process_day(bq: bigquery.Client, suffix: str) -> None:
    day_date_expr = f"PARSE_DATE('%Y%m%d', '{suffix}')"
    source_events_day = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"

    # 1) RAW day -> ga4_events_v2
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

    # 2) ga4_events_v2 -> fEvents_v2
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

    # 3) agregada main v2
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

    # 4) agregada conteudo v2
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

    # 5) dUser_Company v2
    run_query(bq, f"""
    MERGE `{PROJECT_ID}.{DATASET_SILVER}.{T_DUSER_V2}` T
    USING (
      SELECT
        user_company,
        ARRAY_AGG(user_plan ORDER BY event_date_parsed DESC LIMIT 1)[OFFSET(0)] AS plano_atual,
        MIN(event_date_parsed) AS data_primeira_aparicao,
        MAX(event_date_parsed) AS data_ultima_aparicao,
        COUNT(*) AS total_eventos_historicos
      FROM `{PROJECT_ID}.{DATASET_SILVER}.{T_FEVENTS_V2}`
      WHERE event_date_parsed = {day_date_expr}
        AND user_company IS NOT NULL
      GROUP BY 1
    ) S
    ON T.user_company = S.user_company
    WHEN MATCHED THEN UPDATE SET
      T.plano_atual = COALESCE(S.plano_atual, T.plano_atual),
      T.data_primeira_aparicao = LEAST(T.data_primeira_aparicao, S.data_primeira_aparicao),
      T.data_ultima_aparicao = GREATEST(T.data_ultima_aparicao, S.data_ultima_aparicao),
      T.total_eventos_historicos = T.total_eventos_historicos + S.total_eventos_historicos
    WHEN NOT MATCHED THEN INSERT (
      user_company, plano_atual, data_primeira_aparicao, data_ultima_aparicao, total_eventos_historicos,
      tier_cliente, segmento_mercado, account_manager
    ) VALUES (
      S.user_company, S.plano_atual, S.data_primeira_aparicao, S.data_ultima_aparicao, S.total_eventos_historicos,
      NULL, NULL, NULL
    );
    """)


def main():
    if RUN_DATE:
        suffix = RUN_DATE
    else:
        now_sp = datetime.datetime.now(TZ_SP)
        suffix = (now_sp.date() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    source_events_day = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"
    try:
        bq.get_table(source_events_day)
    except NotFound:
        print(f"Tabela não encontrada: {source_events_day}")
        return

    ensure_tables_v2(bq)
    process_day(bq, suffix)

    print(f"✅ OK: processamento do dia {suffix} concluído.")


if __name__ == "__main__":
    main()