"""Configuração fixa do backfill descartável via GA4 Data API."""
import datetime

PROJECT_ID = "jota-dados-integracao-ga4"
BQ_LOCATION = "US"
GA4_PROPERTY_ID = "420210992"

DATASET_RAW = "analytics_420210992"
DATASET_GA4 = "ga4_metrics_us"
DATASET_MP_SILVER = "midiapaga_silver"
DATASET_MP_GOLD = "midiapaga_gold"


def _date_range(start: str, end: str) -> list:
    first, last = datetime.date.fromisoformat(start), datetime.date.fromisoformat(end)
    return [(first + datetime.timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


# Lista fechada: os únicos dias que este módulo pode gravar nas tabelas finais.
BACKFILL_DATES = tuple(_date_range("2026-10-04", "2026-10-10") + _date_range("2026-10-26", "2026-11-02"))
assert len(BACKFILL_DATES) == 15

# Dias com dados reais usados só para comparação (gravados apenas em staging)
CONTROL_DATES = ("2026-10-12", "2026-10-20")

TABLES = ("main", "conteudo", "duser", "trafego", "sessoes", "gold")

PREFIX = "_backfill_dataapi_"


def ga4_table(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_GA4}.{name}"


# Tabelas do módulo (todas com o prefixo _backfill_dataapi_)
T_RAW_MAIN = ga4_table(PREFIX + "raw_main")
T_RAW_CONTEUDO = ga4_table(PREFIX + "raw_conteudo")
T_RAW_DUSER = ga4_table(PREFIX + "raw_duser")
T_STG_MAIN = ga4_table(PREFIX + "main")
T_STG_CONTEUDO = ga4_table(PREFIX + "conteudo")
T_STG_DUSER_DIARIO = ga4_table(PREFIX + "duser_diario")
T_STG_TRAFEGO_FEVENTS = ga4_table(PREFIX + "trafego_fevents")
T_STG_TRAFEGO = ga4_table(PREFIX + "trafego")
T_LOG = ga4_table(PREFIX + "log")

# Tabelas do fluxo atual
T_GA4_EVENTS = ga4_table("ga4_events_v2")
T_FEVENTS = ga4_table("fEvents_v2")
T_MAIN = ga4_table("fEventos_Agregada_Main_v2")
T_CONTEUDO = ga4_table("fEventos_Agregada_Conteudo_v2")
T_DUSER_DIARIO = ga4_table("dUser_Company_diario_v2")
T_DUSER = ga4_table("dUser_Company_v2")
VIEW_DTRAFFIC = "vw_dTraffic"
T_DIM_TRAFEGO = f"{PROJECT_ID}.{DATASET_MP_SILVER}.dim_trafego_ga4"
T_FATO_SESSOES = f"{PROJECT_ID}.{DATASET_MP_SILVER}.fato_sessoes_ga4"
T_FATO_PERFORMANCE_MIDIA = f"{PROJECT_ID}.{DATASET_MP_GOLD}.fato_performance_midia"

# Fórmulas de SK copiadas do pipeline (bq-mirror-job, process_day)
TRAFFIC_SK_SQL = """ABS(FARM_FINGERPRINT(CONCAT(
  COALESCE(source,'(direct)'),'|',COALESCE(medium,'(none)'),'|',COALESCE(campaign,'(not set)'))))"""
EVENT_SK_SQL = "ABS(FARM_FINGERPRINT(COALESCE(event_name,'Unknown')))"
PAGE_SK_SQL = """ABS(FARM_FINGERPRINT(CONCAT(
  COALESCE(REGEXP_EXTRACT(page_location, r'^https?://([^/]+)'),'N/A'),'|',
  COALESCE(SPLIT(page_location,'?')[SAFE_OFFSET(0)],'N/A'),'|',
  COALESCE(REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)'),'N/A'))))"""

# A Data API não tem metro nem versão do navegador: sk_geo e device_sk usam a chave de
# "desconhecido" (todos os campos 'N/A' na mesma fórmula). Essas chaves não existem em vw_geo/vw_dDevice.
SK_GEO_DESCONHECIDO_SQL = "ABS(FARM_FINGERPRINT(CONCAT(" + ", '|', ".join(["'N/A'"] * 6) + ")))"
DEVICE_SK_DESCONHECIDO_SQL = "ABS(FARM_FINGERPRINT(CONCAT(" + ", '|', ".join(["'N/A'"] * 9) + ")))"
