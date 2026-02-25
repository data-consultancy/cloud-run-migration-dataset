import os
import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

from utils.query_ga4_events import query_ga4_events
from utils.query_ga4_fevents import query_ga4_fevents
from utils.query_ga4_fevents_agregada_main import query_ga4_fevents_agregada_main
from utils.query_ga4_fevents_agregada_conteudo import query_ga4_fevents_agregada_conteudo
from utils.query_ga4_duser_company import query_ga4_duser_company


PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_RAW = os.environ.get("PROJECT_ID") and os.environ.get("DATASET_RAW")
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


def _gcs_uri_to_bucket_and_prefix(gcs_uri: str) -> tuple[str, str]:
    """
    Converte:
      gs://bucket/path/to/files/*.parquet  -> (bucket, "path/to/files/")
      gs://bucket/path/to/files/           -> (bucket, "path/to/files/")
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"gcs_uri inválida: {gcs_uri}")

    no_scheme = gcs_uri[5:]  # remove gs://
    bucket, _, path = no_scheme.partition("/")

    # remove wildcard e garante prefixo "diretório"
    path = path.replace("*", "")
    if path and not path.endswith("/"):
        path = path.rsplit("/", 1)[0] + "/"

    return bucket, path


def cleanup_gcs_parquet(gcs_uri: str, gcs_client: storage.Client) -> int:
    """
    Apaga objetos no bucket baseado no prefixo derivado do gcs_uri.
    Retorna a quantidade de objetos deletados.
    """
    bucket_name, prefix = _gcs_uri_to_bucket_and_prefix(gcs_uri)

    if not prefix:
        raise ValueError(
            f"Prefix vazio derivado de {gcs_uri}. "
            "Para segurança, não vou deletar o bucket inteiro."
        )

    print(f"[CLEANUP] gs://{bucket_name}/{prefix} (deletando objetos do prefixo)")

    bucket = gcs_client.bucket(bucket_name)
    blobs = list(gcs_client.list_blobs(bucket, prefix=prefix))

    if not blobs:
        print("[CLEANUP] Nenhum objeto encontrado para deletar.")
        return 0

    # delete em lote
    deleted = 0
    for b in blobs:
        # segurança: só apaga parquet (se quiser apagar tudo do prefixo, remova esse if)
        if b.name.endswith(".parquet"):
            bucket.blob(b.name).delete()
            deleted += 1

    print(f"[CLEANUP] Deletados {deleted} arquivos .parquet")
    return deleted


def run_stage(
    *,
    stage_name: str,
    source_table_id: str,
    target_table_id: str,
    gcs_uri: str,
    query_sql: str,
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
) -> None:
    print(f"\n=== STAGE: {stage_name} ===")

    export_flatten_ga4_to_gcs(source_table_id, gcs_uri, bq_client, query_sql)
    load_parquet_into_bq(target_table_id, gcs_uri, bq_client)

    # só limpa depois do load terminar com sucesso
    cleanup_gcs_parquet(gcs_uri, gcs_client)


def main():
    if RUN_DATE:
        suffix = RUN_DATE
    else:
        now_sp = datetime.datetime.now(TZ_SP)
        suffix = (now_sp.date() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    gcs_client = storage.Client(project=PROJECT_ID)

    # --- Valida source do dia (events_YYYYMMDD) ---
    source_events_day = f"{PROJECT_ID}.{DATASET_RAW}.events_{suffix}"
    try:
        bq_client.get_table(source_events_day)
    except NotFound:
        print(f"Tabela não encontrada: {source_events_day}")
        return

    # 1) GA4_EVENTS (carrega no dataset silver / tabela TARGET_TABLE)
    target_ga4_events = f"{PROJECT_ID}.{DATASET_SILVER}.{TARGET_TABLE}"
    gcs_uri_events = f"gs://{GCS_BUCKET}/ga4/silver/events/anomesdia={suffix}/*.parquet"
    run_stage(
        stage_name="GA4_EVENTS",
        source_table_id=source_events_day,
        target_table_id=target_ga4_events,
        gcs_uri=gcs_uri_events,
        query_sql=query_ga4_events(source_events_day),
        bq_client=bq_client,
        gcs_client=gcs_client,
    )

    # 2) fEvents
    source_ga4_events_table = f"{PROJECT_ID}.{DATASET_SILVER}.ga4_events"
    target_fevents = f"{PROJECT_ID}.{DATASET_SILVER}.fEvents"
    gcs_uri_fevents = f"gs://{GCS_BUCKET}/ga4/silver/fevents/{suffix}/*.parquet"
    run_stage(
        stage_name="fEvents",
        source_table_id=source_ga4_events_table,
        target_table_id=target_fevents,
        gcs_uri=gcs_uri_fevents,
        query_sql=query_ga4_fevents(source_ga4_events_table),
        bq_client=bq_client,
        gcs_client=gcs_client,
    )

    # 3) fEventos_Agregada_Main
    source_fevents = target_fevents
    target_ag_main = f"{PROJECT_ID}.{DATASET_SILVER}.fEventos_Agregada_Main"
    gcs_uri_ag_main = f"gs://{GCS_BUCKET}/ga4/silver/feventos_agregada_main/{suffix}/*.parquet"
    run_stage(
        stage_name="fEventos_Agregada_Main",
        source_table_id=source_fevents,
        target_table_id=target_ag_main,
        gcs_uri=gcs_uri_ag_main,
        query_sql=query_ga4_fevents_agregada_main(source_fevents),
        bq_client=bq_client,
        gcs_client=gcs_client,
    )

    # 4) fEventos_Agregada_Conteudo
    target_ag_cont = f"{PROJECT_ID}.{DATASET_SILVER}.fEventos_Agregada_Conteudo"
    gcs_uri_ag_cont = f"gs://{GCS_BUCKET}/ga4/silver/feventos_agregada_conteudo/{suffix}/*.parquet"
    run_stage(
        stage_name="fEventos_Agregada_Conteudo",
        source_table_id=source_fevents,
        target_table_id=target_ag_cont,
        gcs_uri=gcs_uri_ag_cont,
        query_sql=query_ga4_fevents_agregada_conteudo(source_fevents),
        bq_client=bq_client,
        gcs_client=gcs_client,
    )

    # 5) dUser_Company
    target_duser = f"{PROJECT_ID}.{DATASET_SILVER}.dUser_Company"
    gcs_uri_duser = f"gs://{GCS_BUCKET}/ga4/silver/duser_company/{suffix}/*.parquet"
    run_stage(
        stage_name="dUser_Company",
        source_table_id=source_fevents,
        target_table_id=target_duser,
        gcs_uri=gcs_uri_duser,
        query_sql=query_ga4_duser_company(source_fevents),
        bq_client=bq_client,
        gcs_client=gcs_client,
    )


if __name__ == "__main__":
    main()