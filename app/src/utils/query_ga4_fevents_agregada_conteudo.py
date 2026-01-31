def query_ga4_fevents_agregada_conteudo(source_table_id_fevents_agregada_conteudo):
    query = f"""
        WITH 
            -- 1. MAPA DE ATRIBUTOS (Para corrigir User Company/Pro Nulos)
            User_Attributes AS (
                SELECT
                    event_date_parsed,
                    user_pseudo_id,
                    MAX(user_company) AS empresa_encontrada,
                    MAX(is_pro_user_flag) AS status_pro_encontrado
                FROM `{source_table_id_fevents_agregada_conteudo}`
                WHERE user_company IS NOT NULL OR is_pro_user_flag IS NOT NULL
                GROUP BY 1, 2
            )

            SELECT
                -- 1. Tempo
                t1.event_date_parsed AS data_evento,

                -- 2. Chaves de Dimensão
                t1.page_sk,        -- Liga na vw_dPage (Conteúdo)
                t1.traffic_sk,     -- Liga na vw_dTraffic (Origem)
                
                -- 🆕 NOVIDADE: Geo adicionado a pedido do Genin!
                t1.sk_geo,         -- Liga na vw_dGeo (Localização)

                -- 3. Dados B2B CORRIGIDOS (Enriquecimento)
                COALESCE(t2.empresa_encontrada, t1.user_company, 'N/A') AS user_company,
                COALESCE(t2.status_pro_encontrado, t1.is_pro_user_flag, 'false') AS is_pro_user_flag,

                -- 4. Métricas
                COUNT(*) AS pageviews,
                COUNT(DISTINCT t1.user_pseudo_id) AS leitores_unicos_aprox

            FROM `{source_table_id_fevents_agregada_conteudo}` t1

            -- Join para corrigir os nulos de empresa
            LEFT JOIN User_Attributes t2
                ON t1.user_pseudo_id = t2.user_pseudo_id
                AND t1.event_date_parsed = t2.event_date_parsed

            -- Filtro de Conteúdo
            WHERE t1.event_name = 'page_view'

            GROUP BY
                1, 2, 3, 4, 5, 6; -- Adicionei o índice 4 (geo) no group by
"""
    
    return query