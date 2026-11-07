MERGE INTO `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_FATO_SESSOES_GA4}` T
USING (
    WITH dimensao_trafego_pago AS (
        SELECT
            traffic_sk,
            source_medium,
            CASE
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-extensao-acao-lead') THEN 'bing-cpc-extensao-acao-lead'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-lead-ad') THEN 'bing-cpc-lead-ad'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-pmax-lead') THEN 'bing-cpc-pmax-lead'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-search-conversao-generica') THEN 'bing-cpc-search-conversao-generica'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-search-conversao-institucional') THEN 'bing-cpc-search-conversao-institucional'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-search-generica-trabalhista') THEN 'bing-cpc-search-generica-trabalhista'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-sitelink-ad-marketing-conversao') THEN 'bing-cpc-sitelink-ad-marketing-conversao'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-sitelink-eleicoes') THEN 'bing-cpc-sitelink-eleicoes'
                WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bing-cpc-sitelink-pro') THEN 'bing-cpc-sitelink-pro'
                ELSE campaign
            END AS campanha_tratada
        FROM `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_DIM_TRAFEGO_GA4}`
        WHERE traffic_group = 'Paid'
    ),
    sessoes_recentes AS (
        SELECT
            data_evento,
            traffic_sk,
            SUM(total_sessoes) AS sessoes_ga4
        FROM `{PROJECT_ID}.{DATASET_GA4}.fEventos_Agregada_Main_v2`
        WHERE event_sk = 2194543135944167961
        AND data_evento >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY)
        GROUP BY
            data_evento,
            traffic_sk
    )
    SELECT
        s.data_evento        AS data,
        t.campanha_tratada   AS campanha,
        t.source_medium,
        SUM(s.sessoes_ga4)   AS sessoes_ga4
    FROM sessoes_recentes s
    INNER JOIN dimensao_trafego_pago t ON s.traffic_sk = t.traffic_sk
    GROUP BY
        data,
        campanha,
        source_medium
) S
ON T.data = S.data
AND T.campanha = S.campanha
AND T.source_medium = S.source_medium
AND T.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY)

WHEN MATCHED THEN
    UPDATE SET T.sessoes_ga4 = S.sessoes_ga4

WHEN NOT MATCHED BY TARGET THEN
    INSERT (data, campanha, source_medium, sessoes_ga4)
    VALUES (S.data, S.campanha, S.source_medium, S.sessoes_ga4);
