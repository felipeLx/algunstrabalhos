  SELECT
    CAST(id_orgao         AS BIGINT)  AS cod_orgao,
    CAST(des_orgao        AS STRING)  AS des_orgao,
    CAST(sig_orgao        AS STRING)  AS sig_orgao,
    CAST(tipo_orgao       AS STRING)  AS tipo_orgao,
    CAST(com_tipo_orgao   AS STRING)  AS com_tipo_orgao,
    CAST(id_grupo_orgao   AS BIGINT)  AS id_grupo_orgao,
    CAST(id_orgao_cnj     AS BIGINT)  AS id_orgao_cnj--,
    --'EPROC2G' AS sistema
  FROM eproc2g_orgao

