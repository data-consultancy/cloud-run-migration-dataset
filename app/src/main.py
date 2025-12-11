import os
import datetime
from google.cloud import bigquery

# Variáveis de ambiente (vamos configurar no Cloud Run)
PROJECT_ID = os.environ.get("PROJECT_ID")
SOURCE_DATASET = os.environ.get("SOURCE_DATASET")
TARGET_DATASET = os.environ.get("TARGET_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
LOCATION_SOURCE = os.environ.get("LOCATION_SOURCE")   # ex: "southamerica-east1"
LOCATION_TARGET = os.environ.get("LOCATION_TARGET")   # ex: "US"

if not all([
    PROJECT_ID,
    SOURCE_DATASET,
    TARGET_DATASET,
    GCS_BUCKET,
    LOCATION_SOURCE,
    LOCATION_TARGET,
]):
    raise RuntimeError(
        "Defina as variáveis de ambiente: "
        "PROJECT_ID, SOURCE_DATASET, TARGET_DATASET, "
        "GCS_BUCKET, LOCATION_SOURCE, LOCATION_TARGET"
    )

bq_client = bigquery.Client(project=PROJECT_ID)


def export_table_to_gcs(source_table_id: str, export_uri: str) -> None:
    """Exporta uma tabela do BigQuery para o GCS em formato PARQUET."""
    print(f"[EXPORT] {source_table_id} -> {export_uri}")

    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression=bigquery.Compression.NONE,
    )

    extract_job = bq_client.extract_table(
        source_table_id,
        export_uri,
        location=LOCATION_SOURCE,  # mesma região do dataset origem
        job_config=job_config,
    )
    extract_job.result()  # espera o job concluir

    print(f"[EXPORT OK] {source_table_id}")


def load_table_from_gcs(target_table_id: str, export_uri: str) -> None:
    """Carrega arquivos PARQUET do GCS para uma tabela do BigQuery."""
    print(f"[LOAD] {export_uri} -> {target_table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,  # schema vem do Parquet
    )

    load_job = bq_client.load_table_from_uri(
        export_uri,
        target_table_id,
        location=LOCATION_TARGET,  # mesma localização do dataset destino
        job_config=job_config,
    )
    load_job.result()

    print(f"[LOAD OK] {target_table_id}")


def mirror_dataset() -> None:
    """Copia TODAS as tabelas de um dataset para outro via GCS."""
    print("=== INÍCIO DO JOB DE MIGRAÇÃO DE DATASET ===")
    print(f"Projeto........: {PROJECT_ID}")
    print(f"Dataset origem.: {SOURCE_DATASET} ({LOCATION_SOURCE})")
    print(f"Dataset destino: {TARGET_DATASET} ({LOCATION_TARGET})")
    print(f"Bucket GCS.....: {GCS_BUCKET}")

    dataset_ref = bigquery.DatasetReference(PROJECT_ID, SOURCE_DATASET)
    tables = list(bq_client.list_tables(dataset_ref))

    if not tables:
        print("[INFO] Nenhuma tabela encontrada no dataset de origem.")
        return

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    for table in tables:
        # só copia TABLE (ignora VIEW, MATERIALIZED_VIEW etc)
        if table.table_type != "TABLE":
            print(f"[SKIP] {table.table_id} (tipo: {table.table_type})")
            continue

        source_table_id = f"{PROJECT_ID}.{SOURCE_DATASET}.{table.table_id}"
        target_table_id = f"{PROJECT_ID}.{TARGET_DATASET}.{table.table_id}"

        export_uri = (
            f"gs://{GCS_BUCKET}/bq_mirror/{SOURCE_DATASET}/{table.table_id}/"
            f"{table.table_id}_{timestamp}-*.parquet"
        )

        try:
            export_table_to_gcs(source_table_id, export_uri)
            load_table_from_gcs(target_table_id, export_uri)
        except Exception as e:
            print(f"[ERRO] Falha ao migrar tabela {table.table_id}: {e}")

    print("=== FIM DO JOB DE MIGRAÇÃO DE DATASET ===")


def main():
    mirror_dataset()


if __name__ == "__main__":
    main()
