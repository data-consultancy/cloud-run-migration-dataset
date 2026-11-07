"""Relatórios da GA4 Data API usados no backfill."""
from dataclasses import dataclass, field

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest,
)

from backfill_ga4_dataapi.config import GA4_PROPERTY_ID

NOT_SET = "(not set)"
OTHER = "(other)"
PAGE_SIZE = 100_000
MAX_DIMENSIONS = 9

_TRAFFIC = [("customEvent:source", "source"), ("customEvent:medium", "medium"), ("customEvent:campaign", "campaign")]


@dataclass(frozen=True)
class ReportSpec:
    name: str
    dimensions: tuple  # (nome na API, coluna na staging)
    metrics: tuple
    event_name: str = None  # filtro opcional eventName = valor


REPORTS = {
    "main": ReportSpec(
        name="main",
        dimensions=(("date", "data"), ("eventName", "event_name"), *_TRAFFIC,
                    ("customEvent:JOTA_COMPANY", "user_company"),
                    ("customEvent:JOTA_Planos", "user_plan"),
                    ("customEvent:JOTA_isPro", "is_pro_user_flag")),
        metrics=(("eventCount", "event_count"), ("totalUsers", "total_users")),
    ),
    "conteudo": ReportSpec(
        name="conteudo",
        dimensions=(("date", "data"), ("pageLocation", "page_location"), *_TRAFFIC,
                    ("customEvent:JOTA_COMPANY", "user_company"),
                    ("customEvent:JOTA_isPro", "is_pro_user_flag")),
        metrics=(("eventCount", "event_count"), ("totalUsers", "total_users")),
        event_name="page_view",
    ),
    # Alternativa para quando o relatório de conteúdo devolver (other): sem as dimensões de tráfego
    "conteudo_sem_trafego": ReportSpec(
        name="conteudo_sem_trafego",
        dimensions=(("date", "data"), ("pageLocation", "page_location"),
                    ("customEvent:JOTA_COMPANY", "user_company"),
                    ("customEvent:JOTA_isPro", "is_pro_user_flag")),
        metrics=(("eventCount", "event_count"), ("totalUsers", "total_users")),
        event_name="page_view",
    ),
    "duser": ReportSpec(
        name="duser",
        dimensions=(("date", "data"), ("customEvent:JOTA_COMPANY", "user_company"),
                    ("customEvent:JOTA_Planos", "user_plan")),
        metrics=(("eventCount", "event_count"),),
    ),
}

for _spec in REPORTS.values():
    assert len(_spec.dimensions) <= MAX_DIMENSIONS, _spec.name


@dataclass
class ReportResult:
    rows: list = field(default_factory=list)
    observacoes: set = field(default_factory=set)
    linhas_other: int = 0
    eventos_other: int = 0
    tokens_dia_restantes: int = None
    tokens_hora_restantes: int = None
    requisicoes: int = 0

    @property
    def eventos(self) -> int:
        return sum(r.get("event_count") or 0 for r in self.rows)


def _clean(api_name: str, value: str):
    if value == NOT_SET:
        return None
    # A Data API devolve '' para parâmetros JOTA_* enviados vazios; a produção (export) nunca tem
    # user_company = '' e trata a ausência como NULL (no controle, '' era 100% da diferença do duser).
    if value == "" and api_name.startswith("customEvent:JOTA_"):
        return None
    if api_name == "date":
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def _request(spec: ReportSpec, date: str, offset: int) -> RunReportRequest:
    dimension_filter = None
    if spec.event_name:
        dimension_filter = FilterExpression(filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(value=spec.event_name, match_type=Filter.StringFilter.MatchType.EXACT),
        ))
    return RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name=api) for api, _ in spec.dimensions],
        metrics=[Metric(name=api) for api, _ in spec.metrics],
        date_ranges=[DateRange(start_date=date, end_date=date)],
        dimension_filter=dimension_filter,
        limit=PAGE_SIZE,
        offset=offset,
        return_property_quota=True,
    )


def fetch_report(client: BetaAnalyticsDataClient, spec: ReportSpec, date: str) -> ReportResult:
    """Busca um dia inteiro com paginação, convertendo "(not set)" em None e registrando (other)/limitações."""
    result = ReportResult()
    offset = 0
    while True:
        response = client.run_report(_request(spec, date, offset))
        result.requisicoes += 1

        for api_row in response.rows:
            row = {}
            for (api, col), value in zip(spec.dimensions, api_row.dimension_values):
                row[col] = _clean(api, value.value)
            for (_, col), value in zip(spec.metrics, api_row.metric_values):
                row[col] = int(float(value.value))
            if any(v.value == OTHER for v in api_row.dimension_values):
                result.linhas_other += 1
                result.eventos_other += row.get("event_count") or 0
            result.rows.append(row)

        metadata = response.metadata
        if metadata.data_loss_from_other_row:
            result.observacoes.add("data_loss_from_other_row")
        if metadata.subject_to_thresholding:
            result.observacoes.add("subject_to_thresholding")
        if len(metadata.sampling_metadatas):
            result.observacoes.add("amostrado")
            # Fração da base lida pela amostra (ex.: 12.5% = só 1/8 dos eventos foi lido e o resto é estimado)
            for sm in metadata.sampling_metadatas:
                if sm.sampling_space_size:
                    pct = 100 * sm.samples_read_count / sm.sampling_space_size
                    result.observacoes.add(f"amostra={pct:.1f}% de {sm.sampling_space_size} eventos")

        quota = response.property_quota
        if quota:
            result.tokens_dia_restantes = quota.tokens_per_day.remaining
            result.tokens_hora_restantes = quota.tokens_per_hour.remaining

        offset += len(response.rows)
        if not response.rows or offset >= response.row_count:
            break

    if result.linhas_other:
        result.observacoes.add(f"(other): {result.linhas_other} linhas, {result.eventos_other} eventos")
    return result
