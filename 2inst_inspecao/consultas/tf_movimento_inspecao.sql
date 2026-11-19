with movimentos_unidos as (
SELECT 
    CAST(DTH_REFERENCIA AS TIMESTAMP)   AS dth_referencia,
    CAST(coddoc AS DECIMAL(30,0))       AS cod_documento,
    CAST(dth_movimento AS TIMESTAMP)    AS dth_movimento,
    CAST(cod_fase AS INT)               AS cod_movimento,
    CAST(fase AS varchar(255))          AS movimento,
    CAST(cod_magistrado AS int)         AS cod_magistrado,
    CAST(cod_orgao_gabinete AS INT)     AS cod_orgao_gabinete,
    cast(COD_ORGAOJULGADOR as int)      AS cod_orgjulgador,
    CAST(tp_movimento AS varchar(255))  AS tp_movimento,
    CAST(item AS INT)                	AS item,
    CAST(SUBITEM AS INT)                AS subitem,
    CAST(item_v2 AS INT)                AS item_v2,
    CAST(subitem_v2 AS INT)             AS subitem_v2,
    CAST(ind_sistema AS varchar(255))   AS ind_sistema,
    dth_etl
FROM dw_2inst_ouro.tb_ft_movimento_inspecao_ejud
UNION ALL
SELECT 
    CAST(DTH_REFERENCIA AS TIMESTAMP)   AS dth_referencia,
    CAST(id_processo AS DECIMAL(30,0))  AS cod_documento,
    CAST(dth_evento AS TIMESTAMP)       AS dth_movimento,
    CAST(id_evento_judicial AS INT)     AS cod_movimento,
    CAST(des_evento AS varchar(255))    AS movimento,
    CAST(cod_magistrado AS int)         AS cod_magistrado,
    CAST(cod_orgao_gabinete AS INT)     AS cod_orgao_gabinete,
    cast(cod_orgjulgador as int)        AS cod_orgjulgador,
    CAST(tp_movimento AS varchar(255))  AS tp_movimento,
    CAST(item AS INT)                	AS item,
    CAST(subitem AS INT)                AS subitem,
    CAST(item_v2 AS INT)                AS item_v2,
    CAST(subitem_v2 AS INT)             AS subitem_v2,
    CAST(ind_sistema AS varchar(255))   AS ind_sistema,
    dth_etl
FROM dw_2inst_ouro.tb_ft_movimento_inspecao_eproc2g
), processo_tratado as (
    SELECT DISTINCT
        CAST(COD_DOCUMENTO AS DECIMAL(30,0))        AS cod_documento,
        CAST(COD_CNJ_SIGILO AS VARCHAR(255))        AS cod_cnj,
        CAST(IND_SISTEMA AS VARCHAR(255))           AS ind_sistema,
        CAST(NM_CLASSE AS VARCHAR(255))             AS nm_classe,
        CAST(IND_ORIGINARIO AS VARCHAR(255))        AS ind_originario,
        CAST(IND_PRIORIDADE_IDOSO AS VARCHAR(255))  AS ind_prioridade_idoso,
        CAST(IND_PRIORIDADE_IDOSO_80 AS VARCHAR(255)) AS ind_prioridade_idoso_80,
        CAST(IND_DOENCA_GRAVE AS VARCHAR(255))      AS ind_doenca_grave,
        CAST(IND_PRIORIDADE_DEFICIENTE AS VARCHAR(255)) AS ind_prioridade_deficiente,
        CAST(COD_TIPO_PROCESSO AS INT)              AS cod_classe_sistema,
        CAST(IND_CRIME_ADM_PUBLICO AS VARCHAR(255)) AS ind_crime_adm_publico
    FROM dim_2inst_ouro.tb_dim_processo_ejud
    UNION ALL
    SELECT DISTINCT
        CAST(ID_PROCESSO AS DECIMAL(30,0))          AS cod_documento,
        CAST(COD_CNJ_SIGILO AS VARCHAR(255))        AS cod_cnj,
        CAST(IND_SISTEMA AS VARCHAR(255))           AS ind_sistema,
        CAST(DES_CLASSE AS VARCHAR(255))            AS nm_classe,
        CAST(IND_ORIGINARIO AS VARCHAR(255))        AS ind_originario,
        CAST(IDOSOS AS VARCHAR(255))                AS ind_prioridade_idoso,
        CAST(IDOSOS_80_MAIS AS VARCHAR(255))        AS ind_prioridade_idoso_80,
        CAST(DOENCA_GRAVE AS VARCHAR(255))          AS ind_doenca_grave,
        CAST(PRIORIDADE_DEFICIENTE AS VARCHAR(255)) AS ind_prioridade_deficiente,
        CAST(ID_CLASSE_JUDICIAL AS INT)             AS cod_classe_sistema,
        CAST(IND_CRIME_ADM_PUBLICO AS VARCHAR(255)) AS ind_crime_adm_publico
    FROM dim_2inst_ouro.tb_dim_processo_eproc2g
)
select 
    mu.dth_referencia,
    mu.cod_documento,
    p.cod_cnj as cod_cnj,
    p.nm_classe,
    mu.dth_movimento,
    mu.cod_movimento,
    mu.movimento,
    mu.cod_magistrado as cod_relator,
    dm.cpf as cpf_relator,
    dm.nome as nome_relator,
    mu.cod_orgjulgador as cod_orgao_julgador,
    do.nm_orgao as orgao_julgador,
    mu.cod_orgao_gabinete,
    mu.tp_movimento,
    mu.item,
    mu.subitem,
    mu.item_v2,
    mu.subitem_v2,
    mu.ind_sistema
from movimentos_unidos mu
join processo_tratado p on mu.cod_documento = p.cod_documento and mu.ind_sistema = p.ind_sistema
left join dw_2inst_prata.tb_associacao_magistrado ram on mu.cod_magistrado = ram.cod_magistrado and mu.ind_sistema = ram.ind_sistema
left join dim_2inst_ouro.tb_dim_mag_cpf dm on ram.cpf = dm.cpf
left join dw_2inst_prata.tb_associacao_orgao ao on mu.cod_orgjulgador = ao.codorg_antigo
left join dim_2inst_ouro.tb_dim_orgao do on ao.codorg_novo = do.cod_orgao;
