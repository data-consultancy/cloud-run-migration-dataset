import os
import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from utils.query_ga4_events import query_ga4_events
from utils.query_ga4_fevents import query_ga4_fevents


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
    query: str
) -> None:

    print(f"[EXPORT-FLATTEN] {source_table_id} -> {gcs_uri}")

    sql = f"""
    EXPORT DATA OPTIONS(
      uri='{gcs_uri}',
      format='PARQUET',
      overwrite=true
    ) AS {query}
    """

    job = bq_client.query(sql, location=BQ_LOCATION)
    job.result()

    print("[EXPORT-FLATTEN OK]")


def load_parquet_into_bq(
    target_table_id: str,
    gcs_uri: str,
    bq_client: bigquery.Client,
) -> None:

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
    gcs_uri = f"gs://{GCS_BUCKET}/ga4/silver/events/anomesdia={suffix}/*.parquet"

    bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    try:
        bq_client.get_table(source_table_id)
    except NotFound:
        print(f"[SKIP] Tabela não encontrada: {source_table_id}")
        return

    query_events = query_ga4_events(source_table_id)
    export_flatten_ga4_to_gcs(source_table_id, gcs_uri, bq_client, query_events)
    load_parquet_into_bq(target_table_id, gcs_uri, bq_client)


    source_table_id_fevents = f"{PROJECT_ID}.{DATASET_SILVER}.ga4_events"
    target_table_id_fevents = f"{PROJECT_ID}.{DATASET_SILVER}.fEvents"
    gcs_uri_fevents = f"gs://{GCS_BUCKET}/ga4/silver/fevents/anomesdia={suffix}/*.parquet"
    query_fevents = query_ga4_fevents(source_table_id_fevents)

    export_flatten_ga4_to_gcs(source_table_id_fevents, gcs_uri_fevents, bq_client, query_fevents)
    load_parquet_into_bq(target_table_id_fevents, gcs_uri_fevents, bq_client)


if __name__ == "__main__":
    main()
