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

O job é acionado diariamente (09:00, horário de Brasília) e é idempotente: pode rodar mais de uma vez para o mesmo dia sem alterar o resultado.

- **Sem `RUN_DATE`**: carrega a GA4 Data API do dia anterior e processa, em ordem cronológica, todas as tabelas `events_AAAAMMDD` dos últimos `PENDING_LOOKBACK_DAYS` dias que ainda não foram carregadas (exportações que chegaram atrasadas). Tabelas `events_intraday_` são ignoradas.
- **Com `RUN_DATE=AAAAMMDD`**: carrega a Data API e reprocessa apenas aquele dia. Se `events_AAAAMMDD` não existir, o job falha.

Um dia é considerado carregado quando existe em `ga4_events_v2` **e** em `fEventos_Agregada_Conteudo_v2` (última tabela do processamento diário), de modo que um retry após falha no meio do processamento refaz o dia.

Se uma tabela `events_` da janela não existir, o job registra um aviso e termina com sucesso enquanto o atraso for de até `MISSING_TABLE_ALERT_HOURS` horas após o fim do dia. Passado esse prazo, o job falha com erro explícito — mas só depois de concluir a Data API e os dias pendentes. Falhas da Data API também não impedem o processamento dos eventos e são reportadas como erro no final.

## Variáveis de ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `PROJECT_ID` | Sim | ID do projeto GCP |
| `DATASET_RAW` | Sim | Dataset BigQuery com os eventos brutos do GA4 |
| `DATASET_SILVER` | Sim | Dataset BigQuery de destino (camada Silver) |
| `GA4_PROPERTY_ID` | Sim | ID da propriedade GA4 (somente números) |
| `RUN_DATE` | Não | Reprocessa apenas este dia (`YYYYMMDD`). Padrão: dias pendentes |
| `GA4_HOST` | Não | Host base para montar URLs de página. Padrão: `https://www.jota.info` |
| `MISSING_TABLE_ALERT_HOURS` | Não | Horas após o fim do dia até a ausência de `events_` virar erro. Padrão: `48` |
| `PENDING_LOOKBACK_DAYS` | Não | Janela (em dias, até ontem) para buscar dias pendentes e tabelas ausentes. Padrão: `14` |

## Tabelas

### Tabelas Silver (destino)

| Tabela | Descrição | Partição | Cluster |
|---|---|---|---|
| `ga4_events_v2` | Eventos GA4 com event_params desaninhados | `event_date_parsed` | `event_name`, `user_pseudo_id` |
| `fEvents_v2` | Fato de eventos com surrogate keys de dimensões | `event_date_parsed` | `event_name`, `user_pseudo_id`, `session_id` |
| `fEventos_Agregada_Main_v2` | Contagem de eventos, usuários e sessões por dimensão | `data_evento` | `event_sk`, `traffic_sk`, `user_company` |
| `fEventos_Agregada_Conteudo_v2` | Pageviews e leitores únicos por página | `data_evento` | `page_sk`, `traffic_sk` |
| `dUser_Company_v2` | Dimensão de empresas dos usuários | — | — |
| `dUser_Company_diario_v2` | Resumo por empresa e dia (eventos, último plano) usado para recalcular `dUser_Company_v2` sem ler todo o histórico | `data` | `user_company` |
| `usuarios_ativos_v2` | Usuários ativos por dia (GA4 API) | `data` | — |
| `usuarios_paginas_v2` | Usuários ativos e tempo médio por página (GA4 API) | `data` | `page_location` |
| `usuarios_ativos_mensal_v2` | Usuários ativos por mês no formato `YYYYMM` (GA4 API) | — | — |

### Tabelas de staging (intermediárias)

Cada carga da Data API cria uma tabela temporária exclusiva `_tmp_<destino>_<uuid>` (por exemplo `_tmp_usuarios_ativos_v2_3f9c...`), com expiração automática de 6 horas, e a remove ao final. Execuções paralelas não compartilham estado. As antigas tabelas fixas `_stg_*` não são mais usadas pelo job (não foram removidas do BigQuery).

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
Executado logo após `fEvents_v2` e antes das agregadas, numa única transação:
1. Regrava (DELETE + INSERT) a partição do dia em `dUser_Company_diario_v2`, lendo **apenas o dia** de `fEvents_v2`: uma linha por empresa com total de eventos, último plano preenchido e seu timestamp.
2. Para as empresas presentes no dia (antes ou depois da regravação), recalcula em `dUser_Company_v2` a partir do resumo diário: `total_eventos_historicos` (`SUM`), `data_primeira_aparicao`/`data_ultima_aparicao` (`MIN`/`MAX`) e `plano_atual` (plano do evento mais recente com plano preenchido; mantém o valor atual se não houver).

`tier_cliente`, `segmento_mercado` e `account_manager` não são alterados em empresas existentes.

**Carga inicial:** se `dUser_Company_diario_v2` estiver vazia, o job a preenche uma única vez com todo o histórico de `fEvents_v2` e recalcula `dUser_Company_v2` inteira (corrigindo totais duplicados anteriores). A verificação usa `COUNT(*)` sem filtro, que não lê bytes.

### 6–8. GA4 Data API → tabelas de usuários ativos
Executadas independentemente da existência de `events_`. Para cada uma das três métricas (diária, por página, mensal), o padrão é:
1. Consulta a GA4 Data API
2. Cria uma tabela temporária exclusiva (sufixo uuid, expiração automática)
3. Carrega os dados nela via `load_table_from_json`
4. Executa MERGE na tabela final, deduplicando a origem com `QUALIFY ROW_NUMBER()`
5. Remove a tabela temporária

## Estrutura do repositório

```
.
├── app/
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── src/
│   │   └── main.py
│   └── tests/
│       ├── conftest.py
│       └── test_main.py
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

## Testes

Os testes usam mocks e não acessam BigQuery, GA4 nem Cloud Run:

```bash
pip install -r app/requirements-dev.txt
pytest app/tests -v
```

## Dependências

- `google-cloud-bigquery==3.25.0`
- `google-analytics-data==0.18.7`
