def query_ga4_duser_company(source_table_id_duser_company):
    query = f"""
        WITH Historico_Empresas AS (
            SELECT
                -- A Chave Primária será o próprio nome
                user_company,
                
                -- Truque para pegar o plano mais recente registrado
                ARRAY_AGG(user_plan ORDER BY event_date_parsed DESC LIMIT 1)[OFFSET(0)] AS ultimo_plano_detectado,
                
                -- Datas de atividade
                MIN(event_date_parsed) AS data_primeira_aparicao,
                MAX(event_date_parsed) AS data_ultima_aparicao,
                
                -- Volume total (só para você ordenar as top empresas se quiser)
                COUNT(*) AS total_eventos_historicos
                
            FROM `{source_table_id_duser_company}`
            WHERE user_company IS NOT NULL
            GROUP BY 1
        )

        SELECT
            -- 1. Chave Primária (Texto)
            user_company,
            
            -- 2. Atributos do GA4
            ultimo_plano_detectado AS plano_atual,
            data_primeira_aparicao,
            data_ultima_aparicao,
            total_eventos_historicos,

            -- 3. Colunas para Input Manual (Futuro)
            -- Quando você conectar a planilha, vamos preencher isso aqui
            CAST(NULL AS STRING) AS tier_cliente,      -- Ex: Enterprise, SMB
            CAST(NULL AS STRING) AS segmento_mercado,  -- Ex: Jurídico, Financeiro
            CAST(NULL AS STRING) AS account_manager    -- Ex: João Silva

        FROM Historico_Empresas;
                    """
    
    return query