# backfill_ga4_dataapi (módulo descartável)

Preenche, com granularidade reduzida e via **GA4 Data API**, as tabelas agregadas dos 15 dias em que o export diário do GA4 para o BigQuery não foi gerado:

- 2026-10-04 a 2026-10-10
- 2026-10-26 a 2026-11-02

Esses dias não existem em `analytics_420210992.events_*`, então `ga4_events_v2` e `fEvents_v2` (dados por evento) **não são recriados**. Depois da carga, o módulo deve ser removido (ver [Remoção](#remoção)).

## O que é gravado

| Destino | Origem na Data API | Observações |
|---|---|---|
| `ga4_metrics_us.fEventos_Agregada_Main_v2` | `date, eventName, customEvent:source/medium/campaign, customEvent:JOTA_COMPANY/JOTA_Planos/JOTA_isPro` × `eventCount, totalUsers` | `total_sessoes` = `eventCount` de `session_start` |
| `ga4_metrics_us.fEventos_Agregada_Conteudo_v2` | `date, pageLocation, customEvent:source/medium/campaign, JOTA_COMPANY, JOTA_isPro` (filtro `eventName = page_view`) | `user_company` → `'N/A'` e `is_pro_user_flag` → `'false'` quando nulos |
| `ga4_metrics_us.dUser_Company_diario_v2` | `date, JOTA_COMPANY, JOTA_Planos` × `eventCount` | `ultimo_plano` = plano com mais eventos no dia; `ts_ultimo_plano` = NULL |
| `ga4_metrics_us.dUser_Company_v2` | recalculada a partir de `dUser_Company_diario_v2` | mesmo MERGE do bq-mirror-job (`duser_merge_sql` de `app/src/main.py`), só para as empresas afetadas |
| `midiapaga_silver.dim_trafego_ga4` | — | só `traffic_sk` **ausentes**, derivados pela própria `vw_dTraffic` (ver abaixo) |
| `midiapaga_silver.fato_sessoes_ga4` | — | SQL do paid-media-job com a janela D-3 trocada pelas datas |
| `midiapaga_gold.fato_performance_midia` | — | SQL do paid-media-job, uma vez por faixa contínua de datas |

As tabelas `usuarios_ativos_v2`, `usuarios_ativos_mensal_v2` e `usuarios_paginas_v2` **não são tocadas**.

### Regras aplicadas

- `"(not set)"` da API vira `NULL` antes de gravar ou calcular hashes.
- Todos os hashes (`traffic_sk`, `event_sk`, `page_sk`) são calculados **no BigQuery** com as fórmulas do pipeline.
- `sk_geo` e `device_sk` usam a chave fixa de **desconhecido**: o hash com todos os campos `'N/A'` (6 campos para geo, 9 para device). **Essa chave não existe em `vw_geo` nem em `vw_dDevice`**: joins com essas views não encontram linha para os dias do backfill.
- **`dim_trafego_ga4`:** o módulo lê a definição de `vw_dTraffic` em `INFORMATION_SCHEMA.VIEWS` e troca a referência a `fEvents_v2` pela staging `_backfill_dataapi_trafego_fevents`, que tem o mesmo schema de `fEvents_v2`. Assim `traffic_group`, `origem_ga4` e `source_medium` saem da lógica da view, sem regra copiada. Se a view não referenciar `fEvents_v2` de forma reconhecível, o módulo aborta e mostra a definição.
- **SQLs do paid-media-job:** `sql_midiaspagas/` tem cópias idênticas (byte a byte) de `fato_sessoes_ga4.sql` e `fato_performance_midia.sql` do repositório `midias-pagas-gcp-cloud-run` (commit `e2f2f6b`). Os originais não estão na imagem deste job, por isso as cópias. Antes de usá-las, o módulo compara cada cópia com o último SQL executado pelo paid-media-job (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`):
  - se forem diferentes, **aborta**;
  - se não tiver permissão para consultar, só registra um aviso no log.

## Travas

- **Lista fechada de datas:** as 15 datas acima estão fixas em `config.py`. Qualquer outra data é rejeitada.
- **Datas que o fluxo normal deve tratar:** antes de qualquer coisa, e de novo imediatamente antes de gravar, o módulo **aborta** se para alguma data:
  - existir `analytics_420210992.events_AAAAMMDD`, ou
  - `ga4_events_v2` tiver linhas.
- **Dry-run por padrão:** busca na API, grava só em `ga4_metrics_us._backfill_dataapi_*`, valida e imprime o resumo.
- **`--execute` não chama a API.** Ele grava a partir da staging do último dry-run, ou seja, exatamente o que foi revisado. Aborta se:
  - faltar staging para alguma data;
  - não houver dry-run no log;
  - o último dry-run tiver linhas `(other)`, a menos que se passe `--allow-other`.
- **Idempotência:** cada data é gravada com `DELETE` daquela data seguido de `INSERT`, dentro de `BEGIN TRANSACTION ... COMMIT TRANSACTION`. Pode ser reexecutado sem duplicar.

## Validação (feita no dry-run)

1. **Dias de controle** (padrão 2026-10-12 e 2026-10-20, que existem na raw): os mesmos relatórios da API, comparados com as tabelas atuais. Mostra a diferença percentual de:
   - total de eventos;
   - sessões;
   - pageviews;
   - eventos e número de empresas;
   - sessões pagas.

   Também lista as 5 empresas com maior diferença.
2. **Datas do backfill:** os totais de cada data ao lado da mediana dos 7 dias vizinhos com dados reais.
3. **Registro:** tudo vai para `_backfill_dataapi_log` e para os logs da execução no Cloud Run.

## Como rodar

O módulo está dentro de `app/` para entrar na imagem do `bq-mirror-job`, já que o Dockerfile copia `app/`. Nenhum arquivo do job foi alterado. Com a imagem nova publicada no job, rode **sobrescrevendo os argumentos só desta execução**. O agendamento diário continua rodando `app/src/main.py` normalmente.

```bash
# 1. Dry-run (padrão): API -> staging + validação
gcloud run jobs execute bq-mirror-job --region southamerica-east1 --task-timeout=3600s --wait \
  --args="python,app/backfill_ga4_dataapi"

# Opcional: subconjunto de datas/tabelas
gcloud run jobs execute bq-mirror-job --region southamerica-east1 --task-timeout=3600s --wait \
  --args="python,app/backfill_ga4_dataapi,--dates,2026-10-04,2026-10-05,--tables,main,conteudo"

# Se o relatório de conteúdo tiver (other): repetir o dry-run sem as dimensões de tráfego
gcloud run jobs execute bq-mirror-job --region southamerica-east1 --task-timeout=3600s --wait \
  --args="python,app/backfill_ga4_dataapi,--conteudo-sem-trafego"

# 2. Depois de revisar o resumo: gravar nas tabelas finais
gcloud run jobs execute bq-mirror-job --region southamerica-east1 --task-timeout=3600s --wait \
  --args="python,app/backfill_ga4_dataapi,--execute"
```

Cada valor de `--dates` e `--tables` é um item separado da lista de `--args`.

**Requisitos de ambiente:**
- **Variáveis:** a execução usa `PROJECT_ID`, `DATASET_SILVER` (precisa ser `ga4_metrics_us`) e `DATASET_RAW` do job, como já estão configuradas.
- **Permissões da service account do job:** além das que ela já tem, precisa de:
  - escrita em `midiapaga_silver.dim_trafego_ga4`, `midiapaga_silver.fato_sessoes_ga4` e `midiapaga_gold.fato_performance_midia`;
  - leitura em `midiapaga_silver.dim_campanha`, `dim_plataforma` e `fato_performance_ad`;
  - opcionalmente, `bigquery.jobs.listAll`, para a verificação dos SQLs contra produção.
- **Retries:** o job tem 3 tentativas por tarefa. Repetir é seguro, mas um dry-run que falha consome cota da Data API a cada tentativa.

Localmente, a partir de `app/`, com credenciais que tenham acesso ao BigQuery e à propriedade GA4:

```bash
pip install -r backfill_ga4_dataapi/requirements.txt
PROJECT_ID=jota-dados-integracao-ga4 DATASET_SILVER=ga4_metrics_us DATASET_RAW=analytics_420210992 \
  python -m backfill_ga4_dataapi --dates 2026-10-04
```

### Revisar o resumo

```sql
SELECT data, tabela, linhas, totais, observacoes
FROM `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_log`
WHERE run_id = '<run_id impresso no log da execução>'
ORDER BY tabela, data;
```

Prefixos de `tabela` no log:
- `api:*`: linhas e eventos por relatório, `(other)`, thresholding, amostragem e cota restante;
- `atual:*`: linhas existentes nas tabelas finais;
- `staging:*`: o que seria gravado;
- `validacao_controle:*` e `validacao_vizinhos:*`: a validação;
- `previa:*`: prévia de `fato_sessoes_ga4` e da gold;
- `verificacao_sql`: resultado da comparação dos SQLs com produção;
- `erro`: falhas.

## Limitações conhecidas

- **Atribuição de tráfego:** o pipeline propaga `source`/`medium`/`campaign` dentro da sessão (`LAST_VALUE ... IGNORE NULLS`). A API, com `customEvent:source`, só traz o parâmetro do próprio evento. Eventos sem UTM caem em `(direct)/(none)`. Sessões (`session_start`) tendem a ficar corretas, mas a distribuição por tráfego dos demais eventos fica puxada para o direto. A validação nos dias de controle mostra o tamanho do desvio.
- **`totalUsers`:** é a contagem de usuários da API por combinação de dimensões. Quando duas combinações da API caem no mesmo `traffic_sk`, por exemplo `(not set)` e `(direct)`, os eventos são somados e os usuários também, com uma pequena dupla contagem.
- **`plano_atual`:** com `ts_ultimo_plano = NULL`, o plano dos dias do backfill só define `plano_atual` de uma empresa que não tenha nenhum dia com dados reais.

## Remoção

Depois da carga e da conferência:

1. Apagar a pasta `app/backfill_ga4_dataapi/` e publicar a imagem do `bq-mirror-job` de novo, sem ela.
2. Apagar as tabelas intermediárias:

```sql
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_raw_main`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_raw_conteudo`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_raw_duser`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_main`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_conteudo`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_duser_diario`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_trafego_fevents`;
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_trafego`;
```

3. Decidir sobre o log. Ele é a rastreabilidade do que foi gravado: mantenha, ou apague quando não precisar mais:

```sql
DROP TABLE IF EXISTS `jota-dados-integracao-ga4.ga4_metrics_us._backfill_dataapi_log`;
```

Antes de apagar `_backfill_dataapi_trafego`, se quiser guardar quais `traffic_sk` foram inseridos em `dim_trafego_ga4`, exporte essa tabela ou mantenha o log. Os dados gravados nas tabelas finais **permanecem** depois da remoção do módulo. As linhas do backfill em `fEventos_Agregada_Main_v2` e `fEventos_Agregada_Conteudo_v2` são identificáveis pela chave de geo desconhecida em `sk_geo`.
