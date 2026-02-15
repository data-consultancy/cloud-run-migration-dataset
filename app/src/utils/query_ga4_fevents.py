def query_ga4_fevents(source_table_id_fevents):
    query = f"""
        WITH Base_Extraction AS (
            SELECT
                user_pseudo_id,
                event_timestamp,
                event_name,
                event_date,
                platform,
                stream_id,
                event_value_in_usd,
                
                -- ID da Sessão (Fundamental)
                MAX(CASE WHEN event_params_key = 'ga_session_id' THEN 
                    COALESCE(event_params_value_int_value, SAFE_CAST(event_params_value_string_value AS INT64)) 
                END) AS session_id,
                
                -- 🕵️‍♂️ AQUI ESTÁ O OURO: Extraindo a origem direto dos parâmetros
                MAX(CASE WHEN event_params_key = 'source' THEN event_params_value_string_value END) AS param_source,
                MAX(CASE WHEN event_params_key = 'medium' THEN event_params_value_string_value END) AS param_medium,
                MAX(CASE WHEN event_params_key = 'campaign' THEN event_params_value_string_value END) AS param_campaign,

                -- Seus Atributos de Negócio
                MAX(CASE WHEN event_params_key = 'JOTA_USERID' THEN event_params_value_string_value END) AS user_id_param,
                MAX(CASE WHEN event_params_key = 'JOTA_COMPANY' THEN event_params_value_string_value END) AS user_company,
                MAX(CASE WHEN event_params_key = 'JOTA_Planos' THEN event_params_value_string_value END) AS user_plan,
                MAX(CASE WHEN event_params_key = 'JOTA_isPro' THEN event_params_value_string_value END) AS is_pro_user_flag,
                
                -- URL
                MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END) AS page_location,
                
                -- Geo e Device (Mantidos das colunas originais, que costumam funcionar bem)
                geo_continent, geo_sub_continent, geo_country, geo_region, geo_city, geo_metro,
                device_category, device_operating_system, device_operating_system_version,
                device_web_info_browser, device_browser, device_browser_version,
                device_language, device_mobile_brand_name, device_mobile_model_name

            FROM `jota-dados-integracao-ga4.ga4_metrics_us.ga4_events`
            
            -- Agrupamos para achatar os parâmetros numa linha só por evento
            GROUP BY
                user_pseudo_id, event_timestamp, event_name, event_date, platform, stream_id, event_value_in_usd,
                geo_continent, geo_sub_continent, geo_country, geo_region, geo_city, geo_metro,
                device_category, device_operating_system, device_operating_system_version,
                device_web_info_browser, device_browser, device_browser_version,
                device_language, device_mobile_brand_name, device_mobile_model_name
        ),

        -- 2. FILL DOWN: Preenchimento Inteligente
        -- Se o evento atual (ex: page_view) não tem parâmetro de source, pega o da session_start
        Traffic_Filled AS (
            SELECT
                *,
                -- Source
                LAST_VALUE(param_source IGNORE NULLS) OVER (
                    PARTITION BY user_pseudo_id, session_id 
                    ORDER BY event_timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS source_final,
                
                -- Medium
                LAST_VALUE(param_medium IGNORE NULLS) OVER (
                    PARTITION BY user_pseudo_id, session_id 
                    ORDER BY event_timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS medium_final,
                
                -- Campaign
                LAST_VALUE(param_campaign IGNORE NULLS) OVER (
                    PARTITION BY user_pseudo_id, session_id 
                    ORDER BY event_timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS campaign_final

            FROM Base_Extraction
        )

        -- 3. GERAÇÃO DA TABELA FATO
        SELECT
            -- ID Único
            ABS(FARM_FINGERPRINT(CONCAT(user_pseudo_id, event_timestamp, event_name))) AS fact_id,

            -- 🔑 TRAFFIC SK REFORMULADO
            -- Agora usando as colunas _final que vieram dos parâmetros
            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(source_final, '(direct)'), '|',
                COALESCE(medium_final, '(none)'), '|',
                COALESCE(campaign_final, '(not set)')
            ))) AS traffic_sk,

            -- 🔑 GEO SK
            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(geo_continent, 'N/A'), '|', 
                COALESCE(geo_sub_continent, 'N/A'), '|', 
                COALESCE(geo_country, 'N/A'), '|', 
                COALESCE(geo_region, 'N/A'), '|', 
                COALESCE(geo_city, 'N/A'), '|',
                COALESCE(geo_metro, 'N/A')
            ))) AS sk_geo,

            -- 🔑 DEVICE SK
            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(device_category, 'N/A'), '|',
                COALESCE(device_operating_system, 'N/A'), '|',
                COALESCE(device_operating_system_version, 'N/A'), '|',
                COALESCE(device_web_info_browser, 'N/A'), '|',
                COALESCE(device_browser, 'N/A'), '|',
                COALESCE(device_browser_version, 'N/A'), '|',
                COALESCE(device_language, 'N/A'), '|',
                COALESCE(device_mobile_brand_name, 'N/A'), '|',
                COALESCE(device_mobile_model_name, 'N/A')
            ))) AS device_sk,

            ABS(FARM_FINGERPRINT(COALESCE(event_name, 'Unknown'))) AS event_sk,

            -- Page SK
            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(REGEXP_EXTRACT(page_location, r'^https?://([^/]+)'), 'N/A'), '|',
                COALESCE(SPLIT(page_location, '?')[SAFE_OFFSET(0)], 'N/A'), '|',
                COALESCE(REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)'), 'N/A')
            ))) AS page_sk,

            SAFE_CAST(event_date AS STRING) AS date_sk,

            -- Negócio
            COALESCE(MAX(user_id_param) OVER(PARTITION BY user_pseudo_id), user_id_param) AS user_id, 
            user_company,
            user_plan,
            is_pro_user_flag,

            -- Técnicos
            PARSE_DATE('%Y%m%d', event_date) AS event_date_parsed,
            event_timestamp,
            TIMESTAMP_MICROS(event_timestamp) AS event_ts_utc,
            user_pseudo_id,
            platform,
            stream_id,
            event_name,
            event_value_in_usd,
            
            -- Campos Originais para Debug (Agora mostram o que extraímos dos parâmetros)
            source_final AS traffic_source_source,
            medium_final AS traffic_source_medium,
            campaign_final AS traffic_source_name,

            geo_continent, geo_sub_continent, geo_country, geo_region, geo_city, geo_metro,

            SPLIT(page_location, '?')[SAFE_OFFSET(0)] AS page_url_clean,
            REGEXP_EXTRACT(page_location, r'^https?://([^/]+)') AS hostname_calculado

        FROM Traffic_Filled;
    """

    return query