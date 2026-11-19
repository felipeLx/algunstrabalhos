WITH cte_comp_ejud AS (
    SELECT 
        'EJUD' AS sistema,
        CAST(com.codcompt AS BIGINT) AS cd_competencia,
        UPPER(TRIM(com.descr)) AS ds_competencia,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM ejud.competencia com
),
cte_comp_eproc AS (
    SELECT 
        'EPROC2G' AS sistema,
        CAST(cod_competencia AS BIGINT) AS cd_competencia,
        UPPER(TRIM(des_competencia)) AS ds_competencia,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM eproc2g.competencia_judicial
),
cte_base AS (
    SELECT sistema, cd_competencia, ds_competencia, dt_criacao, dt_atualizacao 
    FROM cte_comp_ejud

    UNION ALL

    SELECT sistema, cd_competencia, ds_competencia, dt_criacao, dt_atualizacao 
    FROM cte_comp_eproc
)
SELECT 
    b.sistema,
    b.cd_competencia,
    b.ds_competencia,
    COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
    CASE 
        WHEN s.cd_competencia IS NULL THEN b.dt_criacao
        WHEN NOT (s.ds_competencia <=> b.ds_competencia) 
            THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
        ELSE s.dt_atualizacao
    END AS dt_atualizacao
FROM cte_base b
LEFT JOIN dw_apex.sigab_competencia_hist s 
    ON s.cd_competencia = b.cd_competencia 
   AND s.sistema = b.sistema
WHERE s.cd_competencia IS NULL 
   OR NOT (s.ds_competencia <=> b.ds_competencia);