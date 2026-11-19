SELECT 
    CAST(coddoc AS DECIMAL(30,0))       AS cod_documento,
    CAST(dth_movimento AS TIMESTAMP)    AS dth_movimento,
    CAST(cod_fase AS INT)               AS cod_movimento,
    CAST(fase AS STRING)                AS movimento,
    CAST(cod_magistrado AS STRING)      AS cod_magistrado,
    CAST(cod_orgao_gabinete AS INT)     AS cod_orgao_gabinete,
    CAST(tp_movimento AS STRING)        AS tp_movimento,
    CAST(item AS INT)                	AS item,
    CAST(SUBITEM AS INT)                AS subitem,
    CAST(ind_sistema AS STRING)         AS ind_sistema,
    dth_etl
FROM dw2inst.tf_movimento_inspecao_ejud
UNION ALL
SELECT 
    CAST(id_processo AS DECIMAL(30,0))  AS cod_documento,
    CAST(dth_evento AS TIMESTAMP)       AS dth_movimento,
    CAST(id_evento_judicial AS INT)     AS cod_movimento,
    CAST(des_evento AS STRING)          AS movimento,
    CAST(cod_magistrado AS STRING)      AS cod_magistrado,
    CAST(cod_orgao_gabinete AS INT)     AS cod_orgao_gabinete,
    CAST(tp_movimento AS STRING)        AS tp_movimento,
    CAST(item AS INT)                	AS item,
    CAST(SUBITEM AS INT)                AS subitem,
    CAST(ind_sistema AS STRING)         AS ind_sistema,
    dth_etl
FROM dw2inst.tf_movimento_inspecao_eproc2g