import os
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from zoneinfo import ZoneInfo

PROJECT_ID = os.environ.get("PROJECT_ID")
SOURCE_DATASET = os.environ.get("SOURCE_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
TZ_SP = ZoneInfo("America/Sao_Paulo")




def export_table_to_bucket(source_table_id, export_uri, bq_client) -> None:
    """Exporta uma tabela do BigQuery para o GCS em formato PARQUET."""

    print(f"[EXPORT] {source_table_id} -> {export_uri}")

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
        export_uri,
        location="US",
        job_config=job_config,
    )
    
    extract_job.result()

    print(f"[EXPORT OK] {source_table_id}")



def main():
    now_sp = datetime.datetime.now(TZ_SP)
    previous_date = (now_sp.date() - datetime.timedelta(days=1))

    suffix = previous_date.strftime("%Y%m%d")

    source_table_id = f"{PROJECT_ID}.{SOURCE_DATASET}.events_{suffix}"
    export_uri = f"gs://{GCS_BUCKET}/ga4/events/anomesdia={suffix}/*.parquet"

    bq_client = bigquery.Client(project=PROJECT_ID, location="US")


    export_table_to_bucket(source_table_id, export_uri, bq_client)


if __name__ == "__main__":
    main()