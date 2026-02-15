def query_ga4_fevents_agregada_conteudo(source_table_id_fevents_agregada_conteudo):
    query = f"""
        WITH User_Attributes AS (
            SELECT
                event_date_parsed,
                user_pseudo_id,
                MAX(user_company) AS empresa_encontrada,
                MAX(is_pro_user_flag) AS status_pro_encontrado
            FROM `jota-dados-integracao-ga4.ga4_metrics_us.fEvents`
            WHERE user_company IS NOT NULL OR is_pro_user_flag IS NOT NULL
            GROUP BY 1, 2
        )

        SELECT
            t1.event_date_parsed AS data_evento,

            -- Chaves
            t1.page_sk,
            t1.traffic_sk,
            t1.sk_geo,

            -- Enriquecimento (Preenche nulos com o que achou na CTE)
            COALESCE(t2.empresa_encontrada, t1.user_company, 'N/A') AS user_company,
            COALESCE(t2.status_pro_encontrado, t1.is_pro_user_flag, 'false') AS is_pro_user_flag,

            -- Métricas
            COUNT(*) AS pageviews,
            COUNT(DISTINCT t1.user_pseudo_id) AS leitores_unicos_aprox

        FROM `jota-dados-integracao-ga4.ga4_metrics_us.fEvents` t1
        LEFT JOIN User_Attributes t2
            ON t1.user_pseudo_id = t2.user_pseudo_id
            AND t1.event_date_parsed = t2.event_date_parsed

        WHERE t1.event_name = 'page_view'

        GROUP BY 1, 2, 3, 4, 5, 6;
"""
    
    return query