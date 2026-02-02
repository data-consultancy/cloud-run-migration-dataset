def query_ga4_fevents_agregada_main(source_table_id_agregada_main):
    query = f"""
        SELECT
            -- 1. Dimensão de Tempo (Fundamental)
            event_date_parsed AS data_evento,

            -- 2. Chaves para ligar nas Dimensões (Star Schema)
            event_sk,       -- Liga na vw_dEvents
            sk_geo,         -- Liga na vw_dGeo
            device_sk,      -- Liga na vw_dDevice
            traffic_sk,     -- Liga na vw_dTraffic

            -- 3. Dados de Negócio (B2B & User)
            user_company,
            user_plan,
            is_pro_user_flag,
            
            -- 4. Métricas Agregadas
            COUNT(*) AS total_eventos,
            
            -- Contagem aproximada de usuários (HyperLogLog) para performance em Big Data
            -- No Power BI, você vai somar isso (SUM)
            COUNT(DISTINCT user_pseudo_id) AS usuarios_unicos_aprox,
            
            -- Contagem distinta de Sessões (Baseada no evento session_start)
            -- Melhor métrica para somar ao longo do mês
            SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS total_sessoes

        FROM `{source_table_id_agregada_main}`

        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8
        """
    return query