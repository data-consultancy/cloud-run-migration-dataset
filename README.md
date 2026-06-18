# bq-mirror-job

Job executado no Google Cloud Run que coleta dados do Google Analytics 4 (via eventos brutos no BigQuery e via GA4 Data API) e os transforma em tabelas analíticas no dataset Silver do BigQuery.

## Visão geral

```
RAW (BigQuery)          GA4 Data API
     │                       │
     ▼                       ▼
ga4_events_v2        staging tables
     │                       │
     ▼                       ▼
fEvents_v2           usuarios_ativos_v2
     │                usuarios_paginas_v2
     ├──► fEventos_Agregada_Main_v2      usuarios_ativos_mensal_v2
     ├──► fEventos_Agregada_Conteudo_v2
     └──► dUser_Company_v2
```

O job é acionado diariamente e processa por padrão o dia anterior (horário de Brasília). A data pode ser sobrescrita via variável de ambiente `RUN_DATE`.

## Variáveis de ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `PROJECT_ID` | Sim | ID do projeto GCP |
| `DATASET_RAW` | Sim | Dataset BigQuery com os eventos brutos do GA4 |
| `DATASET_SILVER` | Sim | Dataset BigQuery de destino (camada Silver) |
| `GA4_PROPERTY_ID` | Sim | ID da propriedade GA4 (somente números) |
| `RUN_DATE` | Não | Data de processamento no formato `YYYYMMDD`. Padrão: dia anterior |
| `GA4_HOST` | Não | Host base para montar URLs de página. Padrão: `https://www.jota.info` |

## Tabelas

### Tabelas Silver (destino)

| Tabela | Descrição | Partição | Cluster |
|---|---|---|---|
| `ga4_events_v2` | Eventos GA4 com event_params desaninhados | `event_date_parsed` | `event_name`, `user_pseudo_id` |
| `fEvents_v2` | Fato de eventos com surrogate keys de dimensões | `event_date_parsed` | `event_name`, `user_pseudo_id`, `session_id` |
| `fEventos_Agregada_Main_v2` | Contagem de eventos, usuários e sessões por dimensão | `data_evento` | `event_sk`, `traffic_sk`, `user_company` |
| `fEventos_Agregada_Conteudo_v2` | Pageviews e leitores únicos por página | `data_evento` | `page_sk`, `traffic_sk` |
| `dUser_Company_v2` | Dimensão de empresas dos usuários | — | — |
| `usuarios_ativos_v2` | Usuários ativos por dia (GA4 API) | `data` | — |
| `usuarios_paginas_v2` | Usuários ativos e tempo médio por página (GA4 API) | `data` | `page_location` |
| `usuarios_ativos_mensal_v2` | Usuários ativos por mês no formato `YYYYMM` (GA4 API) | — | — |

### Tabelas de staging (intermediárias)

| Tabela | Descrição |
|---|---|
| `_stg_usuarios_ativos_v2` | Staging para carga da API antes do MERGE em `usuarios_ativos_v2` |
| `_stg_usuarios_paginas_v2` | Staging para carga da API antes do MERGE em `usuarios_paginas_v2` |
| `_stg_usuarios_ativos_mensal_v2` | Staging para carga da API antes do MERGE em `usuarios_ativos_mensal_v2` |

## Fluxo de processamento

### 1. Eventos brutos → `ga4_events_v2`
Lê a tabela `events_YYYYMMDD` do dataset RAW e desaninha os campos `event_params`, `geo` e `device`, extraindo parâmetros como `ga_session_id`, `page_location`, `JOTA_COMPANY`, `JOTA_isPro`, entre outros.

### 2. `ga4_events_v2` → `fEvents_v2`
Gera surrogate keys (via `FARM_FINGERPRINT`) para as dimensões de tráfego, geo, device, evento e página. Propaga `source/medium/campaign` dentro da sessão com `LAST_VALUE IGNORE NULLS`.

### 3. `fEvents_v2` → `fEventos_Agregada_Main_v2`
Agrega contagem de eventos, usuários únicos (aproximado) e sessões por combinação de dimensões.

### 4. `fEvents_v2` → `fEventos_Agregada_Conteudo_v2`
Filtra eventos `page_view` e agrega pageviews e leitores únicos por página, enriquecendo com atributos de empresa do usuário.

### 5. `fEvents_v2` → `dUser_Company_v2`
Atualiza a dimensão de empresas via MERGE, mantendo histórico de primeira/última aparição e total de eventos acumulado.

### 6–8. GA4 Data API → tabelas de usuários ativos
Para cada uma das três métricas (diária, por página, mensal), o padrão é:
1. Consulta a GA4 Data API
2. Limpa a partição na tabela de staging
3. Carrega os dados na staging via `load_table_from_json`
4. Executa MERGE da staging na tabela final

## Estrutura do repositório

```
.
├── app/
│   ├── requirements.txt
│   └── src/
│       └── main.py
├── cloudbuild.yaml
├── Dockerfile
└── .dockerignore
```

## Build e deploy

O build é gerenciado pelo Cloud Build:

```bash
gcloud builds submit --config cloudbuild.yaml .
```

A imagem gerada é `gcr.io/jota-dados-integracao-ga4/bq-mirror-job:latest`. O Cloud Build utiliza cache da imagem anterior para acelerar builds subsequentes.

## Execução local

```bash
docker build -t bq-mirror-job .

docker run --rm \
  -e PROJECT_ID=seu-projeto \
  -e DATASET_RAW=analytics_raw \
  -e DATASET_SILVER=analytics_silver \
  -e GA4_PROPERTY_ID=123456789 \
  -e RUN_DATE=20250101 \
  -v "$HOME/.config/gcloud:/root/.config/gcloud" \
  bq-mirror-job
```

## Dependências

- `google-cloud-bigquery==3.25.0`
- `google-analytics-data==0.18.7`
