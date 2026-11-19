WITH cte_movimento AS (
    SELECT
        ind_sistema AS sistema,
        CAST(cod_movimento AS BIGINT) AS cd_movimento,
        CAST(cod_movimento_sistema AS BIGINT) AS cd_movimento_sistema,
        UPPER(TRIM(nm_movimento)) AS ds_movimento,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM dim_2inst_ouro.tb_dim_movimento
)
SELECT
    b.sistema,
    b.cd_movimento,
    b.cd_movimento_sistema,
    b.ds_movimento,
    COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
    CASE
        WHEN s.cd_movimento IS NULL THEN b.dt_criacao
        WHEN NOT (s.ds_movimento <=> b.ds_movimento) 
            THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
        ELSE s.dt_atualizacao
    END AS dt_atualizacao
FROM cte_movimento b
LEFT JOIN dw_apex.sigab_movimento_hist s 
    ON s.cd_movimento = b.cd_movimento 
   AND s.sistema = b.sistema
WHERE s.cd_movimento IS NULL 
   OR NOT (s.ds_movimento <=> b.ds_movimento);