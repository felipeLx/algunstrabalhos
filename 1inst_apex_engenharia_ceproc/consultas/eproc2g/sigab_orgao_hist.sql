WITH cte_orgao AS (
    SELECT 
        CAST(cod_orgao AS BIGINT) AS cd_orgao, 
        UPPER(TRIM(nm_orgao_scol)) AS ds_orgao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM dim_2inst_ouro.tb_dim_orgao
)
SELECT 
    b.cd_orgao,
    b.ds_orgao,
    COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
    CASE 
        WHEN s.cd_orgao IS NULL THEN b.dt_criacao
        WHEN NOT (s.ds_orgao <=> b.ds_orgao) 
            THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
        ELSE s.dt_atualizacao
    END AS dt_atualizacao
FROM cte_orgao b
LEFT JOIN dw_apex.sigab_orgao_hist s 
    ON s.cd_orgao = b.cd_orgao
WHERE s.cd_orgao IS NULL 
   OR NOT (s.ds_orgao <=> b.ds_orgao);