WITH cte_classe AS (
    SELECT 
        cla.sistema,
        CAST(cla.codclasse AS BIGINT) AS cd_classe,
        UPPER(TRIM(cla.classe)) AS ds_classe,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM dw_2inst_prata.tb_associacao_classe cla
)
SELECT 
    b.sistema,
    b.cd_classe,
    b.ds_classe,
    COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
    CASE 
        WHEN s.cd_classe IS NULL THEN b.dt_criacao
        WHEN NOT (s.ds_classe <=> b.ds_classe) THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
        ELSE s.dt_atualizacao
    END AS dt_atualizacao
FROM cte_classe b
LEFT JOIN dw_apex.sigab_classe_hist s 
    ON s.cd_classe = b.cd_classe 
   AND s.sistema = b.sistema
WHERE s.cd_classe IS NULL 
   OR NOT (s.ds_classe <=> b.ds_classe);