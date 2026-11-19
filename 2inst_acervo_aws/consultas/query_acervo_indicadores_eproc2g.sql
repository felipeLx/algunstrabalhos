SELECT
    a.dth_referencia,
    a.id_processo,

    -- INDICADORES DE DISTRIBUICAO
    a.dth_distribuicao_ultima,
    a.cod_relator_ultimo AS cod_relator_ultimo,
    pdm.cod_magistrado AS cod_relator_primeiro,
    a.cod_orgao_julgador_ultimo AS cod_orgao_julgador_ultimo,
    COALESCE(pdc.id_orgao_colegiado, p.id_orgao_colegiado, 0) AS cod_orgao_julgador_primeiro,

    -- INDICADORES DE MOVIMENTO
    m.dth_evento AS dth_ultimo_movimento,
    m.id_evento_judicial AS id_evento_judicial_ultimo,
    m.cod_mni_movimento AS cod_mni_movimento_ultimo,

    -- INDICADORES DE SUSPENSAO
    msv.dth_evento AS dth_suspensao,
    msv.id_evento_judicial AS id_evento_judicial_suspensao,
    msv.cod_mni_movimento AS cod_mni_movimento_suspensao,
    CASE WHEN msv.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,

    -- INDICADORES DE SOBRESTAMENTO
    msobv.dth_evento AS dth_sobrestamento,
    msobv.id_evento_judicial AS id_evento_judicial_sobrestamento,
    msobv.cod_mni_movimento AS cod_mni_movimento_sobrestamento,
    CASE WHEN msobv.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestamento,

    -- INDICADORES DE JULGAMENTO
    mj.dth_evento AS dth_julgamento,
    mj.id_evento_judicial AS id_evento_judicial_julgamento,
    mj.cod_mni_movimento AS cod_mni_movimento_julgamento,
    CASE WHEN mj.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento,

    -- INDICADORES DE LOCAL
    mlf.dth_evento AS dth_ultimo_local,
    mlf.id_orgao_destino AS cod_ultimo_local,

    -- INDICADORES DE LOCALIZADORES
    CAST(NULL AS TIMESTAMP) AS dth_entrada_localizador,
    CAST(NULL AS BIGINT) AS cod_ultimo_localizador,

    -- INDICADORES DE CONCLUSAO
    mc.dth_evento AS dth_ultima_conclusao,
    mc.id_evento_judicial AS id_evento_judicial_conclusao,
    mc.cod_mni_movimento AS cod_mni_movimento_conclusao,
    CASE WHEN mc.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_conclusao,

    -- INDICADORES DE JULGAMENTO MONOCRATICO
    mjm.dth_evento AS dth_ultimo_julg_mono,
    mjm.id_evento_judicial AS id_evento_judicial_julg_mono,
    mjm.cod_mni_movimento AS cod_mni_movimento_julg_mono,

    -- INDICADORES DE JULGAMENTO COLEGIADO
    mjc.dth_evento AS dth_ultimo_julg_coleg,
    mjc.id_evento_judicial AS id_evento_judicial_julg_coleg,
    mjc.cod_mni_movimento AS cod_mni_movimento_julg_coleg,

    -- PARTICIONAMENTO
    YEAR(CAST(a.dth_referencia AS DATE)) AS ano,
    MONTH(CAST(a.dth_referencia AS DATE)) AS mes,
    DAY(CAST(a.dth_referencia AS DATE)) AS dia,

    -- INDICADORES DE DECISAO
    mdec.dth_evento AS dth_decisao,
    CASE WHEN mdec.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao,

    -- CONTROLE
    'EPROC2G' AS ind_sistema,
    CURRENT_TIMESTAMP AS dth_etl,

    -- COMPLEMENTO DE DECISAO
    mdec.id_evento_judicial AS id_evento_judicial_decisao,
    mdec.cod_mni_movimento AS cod_mni_movimento_decisao,

    -- INDICADORES DE MOVIMENTO CNJ
    mcnj.dth_evento AS dth_movimento_ultimo_cnj,
    mcnj.id_evento_judicial AS id_evento_judicial_ultimo_cnj,
    mcnj.cod_mni_movimento AS cod_mni_movimento_ultimo_cnj,

    -- INDICADORES DE DILIGENCIA
    CAST(mdil.dth_evento_entrada_diligencia AS TIMESTAMP) AS dth_evento_entrada_diligencia,
    CAST(mdil.dth_evento_saida_diligencia AS TIMESTAMP) AS dth_evento_saida_diligencia,
    CASE 
        WHEN mdil.id_processo IS NULL THEN 'N'
        ELSE mdil.ind_diligencia
    END AS ind_diligencia

FROM acervo a
JOIN eproc2g.processo p 
    ON a.id_processo = p.id_processo
LEFT JOIN movimento m
    ON a.id_processo = m.id_processo
LEFT JOIN mov_julgamento mj
    ON a.id_processo = mj.id_processo
LEFT JOIN mov_conclusao mc
    ON a.id_processo = mc.id_processo
LEFT JOIN mov_suspensao_valida msv
    ON a.id_processo = msv.id_processo
LEFT JOIN mov_sobrestamento_valida msobv
    ON a.id_processo = msobv.id_processo
LEFT JOIN mov_localfisico mlf
    ON a.id_processo = mlf.id_processo
LEFT JOIN mov_julgamento_monocratico mjm
    ON a.id_processo = mjm.id_processo
LEFT JOIN mov_julgamento_colegiado mjc
    ON a.id_processo = mjc.id_processo
LEFT JOIN primeira_distribuicao_magistrado pdm
    ON a.id_processo = pdm.id_processo
LEFT JOIN primeira_distribuicao_colegiado pdc
    ON a.id_processo = pdc.id_processo
LEFT JOIN mov_decisao mdec
    ON a.id_processo = mdec.id_processo
LEFT JOIN movimento_cnj mcnj 
    ON a.id_processo = mcnj.id_processo
LEFT JOIN mov_diligencia mdil
    ON a.id_processo = mdil.id_processo