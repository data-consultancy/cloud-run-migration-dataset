def query_ga4_events(source_table_id):
    query = f"""
        SELECT
        -- CAMPOS SIMPLES
        e.event_date,
        e.event_timestamp,
        e.event_name,
        e.event_previous_timestamp,
        e.event_value_in_usd,
        e.event_bundle_sequence_id,
        e.event_server_timestamp_offset,
        e.user_id,
        e.user_pseudo_id,
        e.user_first_touch_timestamp,
        e.stream_id,
        e.platform,
        e.is_active_user,
        e.batch_event_index,
        e.batch_page_id,
        e.batch_ordering_id,

        -- PRIVACY_INFO
        e.privacy_info.analytics_storage    AS privacy_info_analytics_storage,
        e.privacy_info.ads_storage          AS privacy_info_ads_storage,
        e.privacy_info.uses_transient_token AS privacy_info_uses_transient_token,

        -- USER_LTV
        e.user_ltv.revenue  AS user_ltv_revenue,
        e.user_ltv.currency AS user_ltv_currency,

        -- DEVICE
        e.device.category                 AS device_category,
        e.device.operating_system         AS device_operating_system,
        e.device.browser                  AS device_browser,

        -- GEO
        e.geo.country AS geo_country,
        e.geo.region  AS geo_region,
        e.geo.city    AS geo_city,

        -- EVENT_PARAMS (flatten)
        ep.key                AS event_params_key,
        ep.value.string_value AS event_params_value_string_value,
        ep.value.int_value    AS event_params_value_int_value,
        ep.value.double_value AS event_params_value_double_value

        FROM `{source_table_id}` AS e
        LEFT JOIN UNNEST(e.event_params) AS ep

    """
    return query