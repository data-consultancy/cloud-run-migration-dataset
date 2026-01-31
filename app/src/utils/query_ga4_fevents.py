def query_ga4_fevents(source_table_id_fevents):
    query = f"""
        SELECT
            ABS(FARM_FINGERPRINT(CONCAT(user_pseudo_id, event_timestamp, event_name))) AS fact_id,

            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(geo_continent, 'N/A'), '|', 
                COALESCE(geo_sub_continent, 'N/A'), '|', 
                COALESCE(geo_country, 'N/A'), '|', 
                COALESCE(geo_region, 'N/A'), '|', 
                COALESCE(geo_city, 'N/A'), '|',
                COALESCE(geo_metro, 'N/A')
            ))) AS sk_geo,

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

            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(traffic_source_source, 'N/A'), '|',
                COALESCE(traffic_source_medium, 'N/A'), '|',
                COALESCE(traffic_source_name, 'N/A')
            ))) AS traffic_sk,

            ABS(FARM_FINGERPRINT(COALESCE(event_name, 'Unknown'))) AS event_sk,

            ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(REGEXP_EXTRACT(MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END), r'^https?://([^/]+)'), 'N/A'), '|',
                COALESCE(SPLIT(MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END), '?')[SAFE_OFFSET(0)], 'N/A'), '|',
                COALESCE(REGEXP_EXTRACT(MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END), r'https?://[^/]+(/.*)'), 'N/A')
            ))) AS page_sk,

            SAFE_CAST(event_date AS STRING) AS date_sk,

            COALESCE(
                MAX(user_id), 
                MAX(CASE WHEN event_params_key = 'JOTA_USERID' THEN event_params_value_string_value END)
            ) AS user_id,

            MAX(CASE WHEN event_params_key = 'JOTA_COMPANY' THEN event_params_value_string_value END) AS user_company,

            MAX(CASE WHEN event_params_key = 'JOTA_Planos' THEN event_params_value_string_value END) AS user_plan,

            MAX(CASE WHEN event_params_key = 'JOTA_isPro' THEN event_params_value_string_value END) AS is_pro_user_flag,

            PARSE_DATE('%Y%m%d', event_date) AS event_date_parsed,
            event_timestamp,
            TIMESTAMP_MICROS(event_timestamp) AS event_ts_utc,
            user_pseudo_id,
            
            platform,
            stream_id,
            event_name,
            event_value_in_usd,
            
            SPLIT(MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END), '?')[SAFE_OFFSET(0)] AS page_url_clean,
            
            REGEXP_EXTRACT(
                MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value_string_value END), 
                r'^https?://([^/]+)'
            ) AS hostname_calculado

        FROM `{source_table_id_fevents}`

        GROUP BY
            user_pseudo_id,
            event_timestamp,
            event_name,
            event_date,
            platform,
            stream_id,
            event_value_in_usd,
            geo_continent, geo_sub_continent, geo_country, geo_region, geo_city, geo_metro,
            device_category, device_operating_system, device_operating_system_version,
            device_web_info_browser, device_browser, device_browser_version,
            device_language, device_mobile_brand_name, device_mobile_model_name,
            traffic_source_source, traffic_source_medium, traffic_source_name;
    """