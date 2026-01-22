import os
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from zoneinfo import ZoneInfo

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET = os.environ.get("DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
TZ_SP = ZoneInfo("America/Sao_Paulo")
TARGET_TABLE = os.environ.get("TARGET_TABLE")
RUN_DATE = os.environ.get("RUN_DATE")


def export_table_to_bucket(source_table_id, gcs_uri, bq_client) -> None:
    """Exporta uma tabela do BigQuery para o GCS em formato PARQUET."""

    print(f"[EXPORT] {source_table_id} -> {gcs_uri}")

    try:
        bq_client.get_table(source_table_id)
    except NotFound:
        print(f"[SKIP] Tabela não encontrada: {source_table_id}")
        return

    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression=bigquery.Compression.NONE,
    )

    extract_job = bq_client.extract_table(
        source_table_id,
        gcs_uri,
        location="US",
        job_config=job_config,
    )
    
    extract_job.result()

    print(f"[EXPORT OK] {source_table_id}")


def load_parquet_from_gcs_into_bq(target_table_id, gcs_uri, bq_client) -> None:
    """
    Lê o Parquet do GCS (do dia) e faz INSERT na tabela do BigQuery via Load Job (WRITE_APPEND).
    """
    print(f"[LOAD->BQ] {gcs_uri} -> {target_table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        target_table_id,
        location="US",
        job_config=job_config,
    )
    load_job.result()

    print(f"[LOAD OK] {target_table_id}")


def main():
    
    if RUN_DATE:
        suffix = RUN_DATE
    else:   
        now_sp = datetime.datetime.now(TZ_SP)
        previous_date = (now_sp.date() - datetime.timedelta(days=1))

        suffix = previous_date.strftime("%Y%m%d")

    source_table_id = f"{PROJECT_ID}.{DATASET}.events_{suffix}"
    gcs_uri = f"gs://{GCS_BUCKET}/ga4/events/anomesdia={suffix}/*.parquet"

    target_table_id = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"

    bq_client = bigquery.Client(project=PROJECT_ID, location="US")

    export_table_to_bucket(source_table_id, gcs_uri, bq_client)

    load_parquet_from_gcs_into_bq(target_table_id, gcs_uri, bq_client)


if __name__ == "__main__":
    main()