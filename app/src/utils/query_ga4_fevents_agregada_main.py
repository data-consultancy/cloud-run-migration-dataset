def query_ga4_fevents_agregada_main(source_table_id_agregada_main):
    query = f"""
        SELECT
            -- 1. Tempo
            event_date_parsed AS data_evento,

            -- 2. Chaves (Lendo da fEvents já corrigida)
            event_sk,
            sk_geo,       -- Geo agora padronizado
            device_sk,
            traffic_sk,   -- Tráfego agora preenchido (não é mais só null/NA)

            -- 3. Negócio
            user_company,
            user_plan,
            is_pro_user_flag,
            
            -- 4. Métricas
            COUNT(*) AS total_eventos,
            COUNT(DISTINCT user_pseudo_id) AS usuarios_unicos_aprox,
            
            -- Sessões (Baseada no session_start)
            SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS total_sessoes

        FROM `jota-dados-integracao-ga4.ga4_metrics_us.fEvents`

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
        """
    return query