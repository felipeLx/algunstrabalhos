WITH acervo AS (
    SELECT 
        a.dth_referencia,
        a.id_processo,
        a.dth_distribuicao_ultima,
        a.cod_relator_ultimo,
        a.cod_orgao_julgador_ultimo
    FROM dw2inst.tb_acervo_eproc2g a
), primeira_distribuicao_magistrado AS (
    SELECT * FROM (
        SELECT
            a.id_processo,
            hdj.cod_magistrado,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento ASC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe 
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej 
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.historico_distribuicao_judicial hdj 
            ON pe.id_processo = hdj.id_processo
           AND pe.id_processo_evento = hdj.id_processo_evento
        WHERE ej.cod_mni_movimento IN (26, 36)
          AND pe.dth_evento <= a.dth_referencia
    ) x WHERE rn = 1
), primeira_distribuicao_colegiado AS (
    SELECT *
    FROM (
        SELECT 
            lp.id_processo,
            lp.id_orgao_colegiado,
            ROW_NUMBER() OVER (PARTITION BY lp.id_processo ORDER BY lp.dth_alteracao ASC) AS rn
        FROM acervo a
        LEFT JOIN eproc2g.log_processo lp
            ON a.id_processo = lp.id_processo
           AND lp.dth_alteracao <= a.dth_referencia
           AND lp.id_orgao_colegiado IS NOT NULL
           AND lp.id_orgao_colegiado NOT IN (3583, 358301)
    ) x
    WHERE rn = 1
), movimento AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            pe.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
    ) x
    WHERE rn = 1
), mov_julgamento AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
    	join eproc2g.evento_judicial EJ 
    		on pe.id_Evento_judicial = ej.id_evento_judicial
    	join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 193 > %') mm 
    		on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    ) x
    WHERE rn = 1
), mov_decisao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
    	join eproc2g.evento_judicial EJ 
    		on pe.id_Evento_judicial = ej.id_evento_judicial
    	join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 3 > %') mm 
    		on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    ) x
    WHERE rn = 1
), mov_conclusao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE mm.cod_mni_movimento = 51
    ) x
    WHERE rn = 1
), mov_suspensao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE mm.cod_mni_movimento_pai = 25
    ) x
    WHERE rn = 1
), mov_sobrestamento AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE mm.cod_mni_movimento IN (265, 11975, 12098, 14968, 14969, 12099, 12100, 14971)
    ) x
    WHERE rn = 1
), mov_levantamento_suspensao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE mm.cod_mni_movimento IN (
            15414, 14982, 14981, 12066, 14974, 12067, 12068, 14980, 14979,
            14985, 15416, 14984, 14976, 14975, 14977, 14978, 14983
        )
    ) x
    WHERE rn = 1
), mov_suspensao_valida AS (
    SELECT 
        ms.id_processo,
        ms.dth_evento,
        ms.id_evento_judicial,
        ms.cod_mni_movimento
    FROM mov_suspensao ms
    LEFT JOIN mov_levantamento_suspensao mls
        ON ms.id_processo = mls.id_processo
       AND mls.dth_evento > ms.dth_evento
    WHERE mls.id_processo IS NULL
), mov_sobrestamento_valida AS (
    SELECT 
        ms.id_processo,
        ms.dth_evento,
        ms.id_evento_judicial,
        ms.cod_mni_movimento
    FROM mov_sobrestamento ms
    LEFT JOIN mov_levantamento_suspensao mls
        ON ms.id_processo = mls.id_processo
       AND mls.dth_evento > ms.dth_evento
    WHERE mls.id_processo IS NULL
), mov_localfisico AS (
    SELECT *
    FROM (
        SELECT 
            pe.id_processo,
            pe.dth_evento,
            pe.complemento,
            pro.id_orgao_destino,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
        JOIN eproc2g.processo_remessa_orgao pro
            ON pe.id_processo_evento = pro.id_processo_evento_remessa
    ) x
    WHERE rn = 1
), mov_julgamento_monocratico AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.des_evento,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe
            ON a.id_processo = pe.id_processo
           AND pe.dth_evento <= a.dth_referencia
   		join eproc2g.evento_judicial EJ 
    		on pe.id_Evento_judicial = ej.id_evento_judicial
    	join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 193 > %') mm 
    		on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    	left join EPROC2G.sessao_julgamento_item SJI 
    		on PE.id_processo_evento = SJI.id_processo_evento_resultado 
		left join (select distinct id_sessao_julgamento_item from eproc2g.sessao_julgamento_item_MAGISTRADO) SJIM 
			on SJIM.ID_SESSAO_JULGAMENTO_ITEM = SJI.ID_SESSAO_JULGAMENTO_ITEM 
    where sjim.id_sessao_julgamento_item is null   
    ) x
    WHERE rn = 1
), mov_julgamento_colegiado AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.id_evento_judicial,
            ej.des_evento,
            mm.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY pe.id_processo ORDER BY pe.dth_evento DESC) AS rn
        FROM acervo a
        JOIN eproc2g.processo_evento pe on a.id_processo = pe.id_processo and pe.dth_evento <=  a.dth_referencia
		join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
	    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 193 > %') mm 
	    	on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
	    join EPROC2G.sessao_julgamento_item SJI on PE.id_processo_evento = SJI.id_processo_evento_resultado 
		join (select distinct id_sessao_julgamento_item from eproc2g.sessao_julgamento_item_MAGISTRADO) SJIM 
			on SJIM.ID_SESSAO_JULGAMENTO_ITEM = SJI.ID_SESSAO_JULGAMENTO_ITEM 
	    where sjim.id_sessao_julgamento_item is not null 
    ) x WHERE rn = 1
)
SELECT
    a.dth_referencia,
    a.id_processo,
    --INDICADORES DA PRIMEIRA DISTRIBUICAO
    a.dth_distribuicao_ultima,
    a.cod_relator_ultimo,
    pdm.cod_magistrado AS cod_relator_primeiro,
    a.cod_orgao_julgador_ultimo,
    pdc.id_orgao_colegiado AS cod_orgao_julgador_primeiro,
    --INDICADORES DE MOVIMENTO
    m.dth_evento AS dth_ultimo_movimento,
    m.id_evento_judicial AS id_evento_judicial_ultimo,
    m.cod_mni_movimento AS cod_mni_movimento_ultimo,
    --INDICADORES DE SUSPENSAO
    msv.dth_evento AS dth_suspensao,
    msv.id_evento_judicial AS id_evento_judicial_suspensao,
    msv.cod_mni_movimento AS cod_mni_movimento_suspensao,
    CASE WHEN msv.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
    --INDICADORES DE SOBRESTAMENTO
    msobv.dth_evento AS dth_sobrestamento,
    msobv.id_evento_judicial AS id_evento_judicial_sobrestamento,
    msobv.cod_mni_movimento AS cod_mni_movimento_sobrestamento,
    CASE WHEN msobv.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestamento,
    --INDICADORES DE JULGAMENTO
    mj.dth_evento AS dth_julgamento,
    mj.id_evento_judicial AS id_evento_judicial_julgamento,
    mj.cod_mni_movimento AS cod_mni_movimento_julgamento,
    CASE WHEN mj.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento,
    --INDICADORES DE DECISAO
    mdec.dth_evento AS dth_decisao,
    mdec.id_evento_judicial AS id_evento_judicial_decisao,
    mdec.cod_mni_movimento AS cod_mni_movimento_decisao,
    CASE WHEN mdec.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao, 
    --INDICADORES DE LOCAL
    mlf.dth_evento AS dth_ultimo_local,
    mlf.id_orgao_destino AS cod_ultimo_local,
    --INDICADORES DE LOCALIZADORES
    CAST(NULL AS TIMESTAMP) AS dth_entrada_localizador,
    CAST(NULL AS BIGINT) AS cod_ultimo_localizador,
    --INDICADORES DE ULTIMA CONCLUSAO
    mc.dth_evento AS dth_ultima_conclusao,
    mc.id_evento_judicial AS id_evento_judicial_conclusao,
    mc.cod_mni_movimento AS cod_mni_movimento_conclusao,
    CASE WHEN mc.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_conclusao,
    --INDICADORES DE JULGAMENTO MONOCRATICO
    mjm.dth_evento AS dth_ultimo_julg_mono,
    mjm.id_evento_judicial AS id_evento_judicial_julg_mono,
    mjm.cod_mni_movimento AS cod_mni_movimento_julg_mono,
    --INDICADORES DE JULGAMENTO COLEGIADO
    mjc.dth_evento AS dth_ultimo_julg_coleg,
    mjc.id_evento_judicial AS id_evento_judicial_julg_coleg,
    mjc.cod_mni_movimento AS cod_mni_movimento_julg_coleg
FROM acervo a
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
left join mov_decisao mdec
	on a.id_processo = mdec.id_processo;