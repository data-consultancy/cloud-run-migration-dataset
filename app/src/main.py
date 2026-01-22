import os
import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_RAW = os.environ.get("DATASET_RAW")        
DATASET_SILVER = os.environ.get("DATASET_SILVER") 
TARGET_TABLE = os.environ.get("TARGET_TABLE") 
GCS_BUCKET = os.environ.get("GCS_BUCKET")
RUN_DATE = os.environ.get("RUN_DATE")        

BQ_LOCATION = "US"
TZ_SP = ZoneInfo("America/Sao_Paulo")


def export_flatten_ga4_to_gcs(
    source_table_id: str,
    gcs_uri: str,
    bq_client: bigquery.Client,
) -> None:
    """
    Executa um EXPORT DATA com SELECT flatten,
    gerando Parquet JÁ no schema correto (silver).
    """

    print(f"[EXPORT-FLATTEN] {source_table_id} -> {gcs_uri}")

    sql = f"""
    EXPORT DATA OPTIONS(
      uri='{gcs_uri}',
      format='PARQUET',
      overwrite=true
    ) AS
    SELECT
      -- CAMPOS SIMPLES
      e.event_date,
      e.event_timestamp,
      e.event_name,
      e.event_previous_timestamp,
      e.event_value_in_usd,
      e.event_bundle_sequence_id,
      e.event_server_timestamp_offset,
      e.user_id,
      e.user_pseudo_id,
      e.user_first_touch_timestamp,
      e.stream_id,
      e.platform,
      e.is_active_user,
      e.batch_event_index,
      e.batch_page_id,
      e.batch_ordering_id,

      -- PRIVACY_INFO
      e.privacy_info.analytics_storage    AS privacy_info_analytics_storage,
      e.privacy_info.ads_storage          AS privacy_info_ads_storage,
      e.privacy_info.uses_transient_token AS privacy_info_uses_transient_token,

      -- USER_LTV
      e.user_ltv.revenue  AS user_ltv_revenue,
      e.user_ltv.currency AS user_ltv_currency,

      -- DEVICE
      e.device.category                 AS device_category,
      e.device.operating_system         AS device_operating_system,
      e.device.browser                  AS device_browser,

      -- GEO
      e.geo.country AS geo_country,
      e.geo.region  AS geo_region,
      e.geo.city    AS geo_city,

      -- EVENT_PARAMS (flatten)
      ep.key                AS event_params_key,
      ep.value.string_value AS event_params_value_string_value,
      ep.value.int_value    AS event_params_value_int_value,
      ep.value.double_value AS event_params_value_double_value

    FROM `{source_table_id}` AS e
    LEFT JOIN UNNEST(e.event_params) AS ep
    """

    job = bq_client.query(sql, location=BQ_LOCATION)
    job.result()

    print("[EXPORT-FLATTEN OK]")


def load_parquet_into_bq(
    target_table_id: str,
    gcs_uri: str,
    bq_client: bigquery.Client,
) -> None:
    """
    Faz APPEND do Parquet (schema já correto) na tabela silver.
    """

    print(f"[LOAD] {gcs_uri} -> {target_table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = bq_client.load_table_from_uri(
        gcs_uri,
        target_table_id,
        location=BQ_LOCATION,
        job_config=job_config,
    )
    job.result()

    print("[LOAD OK]")



def main():
    
    if RUN_DATE:
        suffix = RUN_DATE 
    else:
        now_sp = datetime.datetime.now(TZ_SP)
        suffix = (now_sp.date() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    source_table_id = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"
    target_table_id = f"{PROJECT_ID}.{DATASET_SILVER}.{TARGET_TABLE}"
    gcs_uri = f"gs://{GCS_BUCKET}/ga4/silver/anomesdia={suffix}/*.parquet"

    bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    try:
        bq_client.get_table(source_table_id)
    except NotFound:
        print(f"[SKIP] Tabela não encontrada: {source_table_id}")
        return

    export_flatten_ga4_to_gcs(source_table_id, gcs_uri, bq_client)
    load_parquet_into_bq(target_table_id, gcs_uri, bq_client)


if __name__ == "__main__":
    main()
