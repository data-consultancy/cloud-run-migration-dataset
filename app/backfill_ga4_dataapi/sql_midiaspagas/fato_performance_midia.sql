BEGIN TRANSACTION;

-- Remove a janela que será recalculada. Como a tabela é agregada (sem chave natural estável),
-- DELETE + INSERT da janela é idempotente e evita duplicidade em reprocessamentos.
DELETE FROM `{PROJECT_ID}.{DATASET_GOLD}.{TABLE_FATO_PERFORMANCE_MIDIA}`
WHERE Date BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}';

INSERT INTO `{PROJECT_ID}.{DATASET_GOLD}.{TABLE_FATO_PERFORMANCE_MIDIA}` (
    Date, nm_campanha, campanha_tratada, plataforma, funil, dif,
    Investimento, `Impressões`, Cliques, `Conversões`, `Sessões`,
    CPM, CPC, CPL
)
WITH dim_campanha_unica AS (
    -- A mesma 'campanha_tratada' pode ter gerado vários IDs na API com o tempo.
    -- Garante relação 1:1 escolhendo UMA linha inteira da dimensão (e não MAX coluna a coluna),
    -- para que nm_campanha, funil, dif e plataforma sejam sempre de uma mesma campanha real
    -- e as sessões se fundam com a linha de investimento dessa campanha.
    SELECT
        campanha_tratada,
        nm_campanha,
        sk_plataforma,
        funil,
        dif
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_DIM_CAMPANHA}`
    WHERE campanha_tratada IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campanha_tratada
        ORDER BY nm_campanha DESC, sk_plataforma DESC
    ) = 1
),

combinacao_bruta AS (
    -- BLOCO 1: DADOS DAS PLATAFORMAS DE ANÚNCIO (Pré-Click e Conversões da Plataforma)
    SELECT
        f.dt_referencia AS Date,
        p.nm_plataforma AS plataforma,
        c.nm_campanha,
        c.campanha_tratada,
        c.funil,
        c.dif,
        SAFE_CAST(f.vl_custo AS NUMERIC) AS Investimento,
        SAFE_CAST(f.nr_impressoes AS INT64) AS Impressoes,
        SAFE_CAST(f.nr_cliques AS INT64) AS Cliques,
        SAFE_CAST(f.nr_conversoes AS NUMERIC) AS Conversoes,
        0 AS Sessoes
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_FATO_PERFORMANCE_AD}` f
    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_DIM_CAMPANHA}` c
        ON f.sk_campanha = c.sk_campanha
    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_DIM_PLATAFORMA}` p
        ON f.sk_plataforma = p.sk_plataforma
    WHERE f.dt_referencia BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'

    UNION ALL

    -- BLOCO 2: DADOS DO GOOGLE ANALYTICS 4 (Pós-Click / Sessões)
    SELECT
        s.data AS Date,
        p.nm_plataforma AS plataforma,
        dc.nm_campanha,
        s.campanha AS campanha_tratada,
        dc.funil,
        dc.dif,
        0 AS Investimento,
        0 AS Impressoes,
        0 AS Cliques,
        0 AS Conversoes,
        SAFE_CAST(s.sessoes_ga4 AS INT64) AS Sessoes
    FROM `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_FATO_SESSOES_GA4}` s
    -- INNER JOIN: bloqueia sessões do GA4 que não possuem campanha correspondente nas mídias pagas
    INNER JOIN dim_campanha_unica dc
        ON s.campanha = dc.campanha_tratada
    INNER JOIN `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_DIM_PLATAFORMA}` p
        ON dc.sk_plataforma = p.sk_plataforma
    WHERE s.data BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
),

agregacao_final AS (
    -- Se houver custo e sessão no mesmo dia para a mesma campanha, as duas linhas se fundem.
    SELECT
        Date,
        nm_campanha,
        campanha_tratada,
        plataforma,
        funil,
        dif,
        SUM(Investimento) AS Investimento,
        SUM(Impressoes) AS Impressoes,
        SUM(Cliques) AS Cliques,
        SUM(Conversoes) AS Conversoes,
        SUM(Sessoes) AS Sessoes
    FROM combinacao_bruta
    -- FILTRO DE SEGURANÇA: nenhuma linha entra na Gold sem os metadados
    WHERE nm_campanha IS NOT NULL
    AND plataforma IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    Date,
    nm_campanha,
    campanha_tratada,
    plataforma,
    funil,
    dif,
    Investimento,
    Impressoes AS `Impressões`,
    Cliques,
    Conversoes AS `Conversões`,
    Sessoes AS `Sessões`,
    -- SAFE_DIVIDE evita erro de divisão por zero; taxas monetárias em NUMERIC, como o Investimento
    SAFE_DIVIDE(Investimento, Impressoes) * 1000 AS CPM,
    SAFE_DIVIDE(Investimento, Cliques) AS CPC,
    SAFE_CAST(SAFE_DIVIDE(Investimento, Conversoes) AS NUMERIC) AS CPL
FROM agregacao_final;

COMMIT TRANSACTION;
