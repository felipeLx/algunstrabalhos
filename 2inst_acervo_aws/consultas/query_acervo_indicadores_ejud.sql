WITH acervo AS ( 
    SELECT  
        datareferencia AS dth_referencia,
        coddoc,
        dataultdis AS dth_ultima_distribuicao,
        codultrel AS cod_ultimo_relator,
        codultorgjulg AS cod_ultimo_orgao_julgador
    FROM dw_2inst_prata.tb_acervo_ejud
), mov_distribuicao AS (
    SELECT 
        md.coddoc, 
        md.dthrmov, 
        md.codfase
    FROM acervo a
    JOIN ejud.movdistribuicao md ON a.coddoc = md.coddoc
    WHERE md.dthrmov <= a.dth_referencia
    AND codfase IN (26, 36, 60012)
), mov_distribuicao_cancelada AS (
    SELECT 
        mcr.coddoc, 
        mcr.dthrmov, 
        mcr.dthrmovdistrcance
    FROM acervo a
    JOIN ejud.movcancedistr mcr ON a.coddoc = mcr.coddoc
    WHERE mcr.dthrmov <= a.dth_referencia
), mov_distribuicao_primeira_valida AS (
    SELECT * FROM (
        SELECT 
            md.coddoc,
            md.dthrmov,
            mr.codmagrel,
            mr.codorgjulg,
            ROW_NUMBER() OVER (PARTITION BY md.coddoc ORDER BY md.dthrmov ASC) AS rn
        FROM mov_distribuicao md
        LEFT JOIN mov_distribuicao_cancelada mdc ON md.coddoc = mdc.coddoc AND md.dthrmov = mdc.dthrmovdistrcance
        JOIN ejud.movimentorelator mr ON md.coddoc = mr.coddoc AND md.dthrmov = mr.dthrmov
        WHERE mdc.dthrmovdistrcance IS NULL
    ) WHERE rn = 1
), mov_suspensao AS (
    SELECT * FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a 
        JOIN ejud.movimento m ON a.coddoc = m.coddoc
        WHERE m.dthrmov <= a.dth_referencia
        AND (
            (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
            OR (m.codfase IN (3, 20) AND m.codcompl1 = 25)
            OR (m.codfase IN (3, 20, 29) AND m.codcompl1 = 1013 AND COALESCE(m.codcompl2, 0) = 1015)
            OR (m.codfase = 861 AND m.codcompl1 IN (690, 50068))
            OR (m.codfase = 861 AND m.codcompl1 IN (359, 360, 361, 362, 363, 445, 50069)
                AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
            OR (m.codfase = 50002 AND m.codcompl1 IN (306, 12065))
        )
    ) x WHERE rn = 1
), mov_reativacao AS (
    SELECT
        m.coddoc,
        m.dthrmov,
        m.codfase,
        m.codcompl1,
        m.codcompl2,
        m.codcompl3
    FROM acervo a 
    JOIN ejud.movimento m ON a.coddoc = m.coddoc
    WHERE m.dthrmov <= a.dth_referencia
    AND (
        (m.codfase = 50002 AND m.codcompl1 = 12066)
        OR m.codfase IN (893, 24, 193)
    )
), mov_reperussao AS (
    SELECT * FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            CASE
                WHEN ms.dthrterminosobre >= a.dth_referencia THEN NULL
                ELSE ms.dthrterminosobre
            END AS dthrterminosobre
        FROM acervo a
        JOIN ejud.movimento m ON a.coddoc = m.coddoc
        JOIN ejud.movsobrestamento ms ON ms.coddoc = m.coddoc AND ms.dthrmov = m.dthrmov
        WHERE m.dthrmov <= a.dth_referencia
        AND m.codfase = 50040
    ) x WHERE dthrterminosobre IS NULL
), mov_suspensao_repercussao AS (
    SELECT
        x.coddoc,
        MIN(x.dthrmov) AS datasusp,
        MIN(x.codfase) AS codfase_susp,
        MIN(x.codcompl1) AS codcompl1_susp,
        MIN(x.codcompl2) AS codcompl2_susp,
        MIN(x.codcompl3) AS codcompl3_susp,
        MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END) AS datarepercussao,
        MIN(CASE WHEN x.src = 'R' THEN x.codfase END) AS codfase_reper,
        MIN(CASE WHEN x.src = 'R' THEN x.codcompl1 END) AS codcompl1_reper,
        MIN(CASE WHEN x.src = 'R' THEN x.codcompl2 END) AS codcompl2_reper,
        MIN(CASE WHEN x.src = 'R' THEN x.codcompl3 END) AS codcompl3_reper
    FROM (
        SELECT s.coddoc, s.dthrmov, s.codfase, s.codcompl1, s.codcompl2, s.codcompl3, 'S' AS src 
        FROM mov_suspensao s LEFT JOIN mov_reativacao r
        ON r.coddoc = s.coddoc AND r.dthrmov > s.dthrmov
        WHERE r.coddoc IS NULL
        UNION ALL
        SELECT r.coddoc, r.dthrmov, r.codfase, r.codcompl1, r.codcompl2, r.codcompl3, 'R' AS src 
        FROM mov_reperussao r
    ) x
    GROUP BY x.coddoc
), mov_julgamento AS (
    SELECT * FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a 
        JOIN ejud.movimento m ON a.coddoc = m.coddoc
        WHERE m.dthrmov <= a.dth_referencia
        AND m.codfase IN (193, 24) 
    ) x WHERE rn = 1
), mov_localfisico AS (
    SELECT * FROM (
        SELECT mf.coddoc, mf.dthrmov, mf.codlocal, ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn 
        FROM acervo a
        JOIN ejud.movimentofisico mf ON a.coddoc = mf.coddoc AND mf.dthrmov <= a.dth_referencia 
    ) x WHERE rn = 1
), mov_localvirtual AS (
    SELECT * FROM (
        SELECT lvd.coddoc, lvd.dthrentr, lvd.codlocalvirt, lvd.codlocal, ROW_NUMBER() OVER (PARTITION BY lvd.coddoc ORDER BY lvd.dthrentr DESC) AS rn
        FROM acervo a
        JOIN ejud.localvirtualdocumento lvd ON a.coddoc = lvd.coddoc AND lvd.dthrentr <= a.dth_referencia
    ) x WHERE rn = 1
), mov_julgamento_monocratico AS (
    SELECT *
    FROM (
        SELECT 
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a
        JOIN ejud.movimento m ON a.coddoc = m.coddoc AND m.dthrmov <= a.dth_referencia
        WHERE m.codfase = 24
    ) t
    WHERE rn = 1
), mov_julgamento_colegiado AS (
    SELECT *
    FROM (
        SELECT 
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a
        JOIN ejud.movimento m ON a.coddoc = m.coddoc AND m.dthrmov <= a.dth_referencia
        WHERE m.codfase = 193
    ) x WHERE rn = 1
), mov_decisao AS (
	SELECT * FROM (
		SELECT
		    m.coddoc,
		    m.dthrmov,
		    m.codfase,
		    m.codcompl1,
		    m.codcompl2,
		    m.codcompl3,
		    ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
		FROM acervo a
		JOIN ejud.movimento m ON a.coddoc = m.coddoc AND m.dthrmov <= a.dth_referencia
		WHERE m.codfase IN (3, 20)
	) x WHERE rn = 1
), mov_movimento AS (
    SELECT * FROM (
        SELECT 
            m.coddoc, 
            m.dthrmov, 
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a
        JOIN ejud.movimento m ON a.coddoc = m.coddoc AND m.dthrmov <= a.dth_referencia
    ) x WHERE rn = 1
), mov_movimento_cnj AS (
    SELECT * FROM (
        SELECT 
            m.coddoc, 
            m.dthrmov, 
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM acervo a
        JOIN ejud.movimento m ON a.coddoc = m.coddoc AND m.dthrmov <= a.dth_referencia
        join dim_2inst_ouro.tb_dim_movimento tdm on m.codfase = tdm.cod_movimento and tdm.ind_sistema = 'EJUD'
        WHERE tdm.ind_movimento_cnj = 'S'
    ) x WHERE rn = 1
), mov_distribuicao_primeira_cargo AS (
    SELECT * FROM (
        SELECT  
            mdpv.codmagrel, 
            CASE 
                WHEN lm.codcargo = 3 AND lm.codjustorig = 2 THEN 9
                WHEN lm.codcargo = 2 AND lm.codjustorig = 2 THEN 2
                ELSE lm.codcargo 
            END AS codcargo,
            ROW_NUMBER() OVER (PARTITION BY lm.codmag ORDER BY lm.dthralt DESC) AS rn
        FROM mov_distribuicao_primeira_valida mdpv
        LEFT JOIN ejud.logmagistrado lm ON mdpv.codmagrel = lm.codmag 
        AND lm.codcargo IS NOT NULL AND lm.codjustorig IS NOT NULL AND lm.tipoper != 'E'
        WHERE lm.dthralt <= mdpv.dthrmov
    ) x WHERE rn = 1
), mov_conclusao AS (
    select  
        mm.coddoc, 
        mm.dthrmov, 
        mm.codfase,
        mm.codcompl1,
        mm.codcompl2,
        mm.codcompl3
    from mov_localfisico mlf
    join ejud.movimento mm on mm.coddoc = mlf.coddoc and mm.dthrmov = mlf.dthrmov
    join (select codlocal, descr as localfisico from ejud.localfisico where codmag is not null and codlocal not in (3142,3144,3148)) lf on mlf.codlocal = lf.codlocal
), mov_distribuicao_ultima_cargo AS (
    SELECT * FROM (
        SELECT 
            a.cod_ultimo_relator,
            CASE 
                WHEN lm.codcargo = 3 AND lm.codjustorig = 2 THEN 9
                WHEN lm.codcargo = 2 AND lm.codjustorig = 2 THEN 2
                ELSE lm.codcargo 
            END AS codcargo,
            ROW_NUMBER() OVER (PARTITION BY lm.codmag ORDER BY lm.dthralt DESC) AS rn
        FROM acervo a
        JOIN ejud.logmagistrado lm ON a.cod_ultimo_relator = lm.codmag AND lm.codcargo IS NOT NULL AND lm.codjustorig IS NOT NULL AND lm.tipoper != 'E'
        WHERE lm.dthralt <= a.dth_ultima_distribuicao 
    ) x WHERE rn = 1    
)
SELECT
    a.dth_referencia,
    a.coddoc,
    --indicadores de distribuicao
    a.dth_ultima_distribuicao AS dth_distribuicao_ultima,
    a.cod_ultimo_relator AS cod_magistrado_relator_ultimo,
    a.cod_ultimo_orgao_julgador AS cod_orgao_julgador_ultimo,
    mduc.codcargo AS cod_cargo_ultimo_relator,
    mdpv.dthrmov AS dth_distribuicao_primeira, 
    mdpv.codmagrel AS cod_magistrado_relator_primeiro, 
    mdpv.codorgjulg AS cod_orgao_julgador_primeiro, 
    mdpc.codcargo AS cod_cargo_primeiro_relator, 
    --indicadores do ultimo movimento
    m.dthrmov AS dth_movimento_ultimo,
    m.codfase AS cod_fase_ultimo,
    m.codcompl1 AS cod_complemento1_ultimo,
    m.codcompl2 AS cod_complemento2_ultimo,
    m.codcompl3 AS cod_complemento3_ultimo,
    --indicadores de suspensao
    msr.datasusp AS dth_suspensao,
    msr.codfase_susp AS cod_fase_suspensao,
    msr.codcompl1_susp AS cod_complemento1_suspensao,
    msr.codcompl2_susp AS cod_complemento2_suspensao,
    msr.codcompl3_susp AS cod_complemento3_suspensao,
    CASE WHEN msr.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
    --indicadores de repercussao
    msr.datarepercussao AS dth_repercussao,
    msr.codfase_reper AS cod_fase_repercussao,
    msr.codcompl1_reper AS cod_complemento1_repercussao,
    msr.codcompl2_reper AS cod_complemento2_repercussao,
    msr.codcompl3_reper AS cod_complemento3_repercussao,
    CASE WHEN msr.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestamento,
    --indicadores de julgamento
    mj.dthrmov AS dth_julgamento,
    mj.codfase AS cod_fase_julgamento,
    mj.codcompl1 AS cod_complemento1_julgamento,
    mj.codcompl2 AS cod_complemento2_julgamento,
    mj.codcompl3 AS cod_complemento3_julgamento,
    CASE WHEN mj.dthrmov IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento,
    --indicadores de decisao
    mdec.dthrmov AS dth_decisao,
    mdec.codfase AS cod_fase_decisao,
    mdec.codcompl1 AS cod_complemento1_decisao,
    mdec.codcompl2 AS cod_complemento2_decisao,
    mdec.codcompl3 AS cod_complemento3_decisao,
    CASE WHEN mdec.dthrmov IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao,    
    --indicadores de movimento local
    mlf.dthrmov AS dth_ultimo_local,
    mlf.codlocal AS cod_ultimo_local,
    --indicadores de local virtual
    mlv.dthrentr AS dth_ultimo_local_virtual,
    mlv.codlocalvirt AS cod_ultimo_local_virtual,
    --indicadores de conclusao
    mc.dthrmov AS dth_ultima_conclusao,
    mc.codfase AS cod_fase_conclusao,
    mc.codcompl1 AS cod_complemento1_ultima_conclusao,
    mc.codcompl2 AS cod_complemento2_ultima_conclusao,
    mc.codcompl3 AS cod_complemento3_ultima_conclusao,
    CASE WHEN mc.dthrmov IS NOT NULL THEN 'S' ELSE 'N' END AS ind_conclusao,
    --indicadores de julgamento monocratico
    mjm.dthrmov AS dth_ultima_julgamento_monocratico,
    mjm.codfase AS cod_fase_julgamento_monocratico,
    mjm.codcompl1 AS cod_complemento1_julgamento_monocratico,
    mjm.codcompl2 AS cod_complemento2_julgamento_monocratico,
    mjm.codcompl3 AS cod_complemento3_julgamento_monocratico,
    CASE WHEN mjm.dthrmov IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento_monocratico,
    --indicadores de colegiado
    mjc.dthrmov AS dth_ultima_julgamento_colegiado,
    mjc.codfase AS cod_fase_julgamento_colegiado,
    mjc.codcompl1 AS cod_complemento1_julgamento_colegiado,
    mjc.codcompl2 AS cod_complemento2_julgamento_colegiado,
    mjc.codcompl3 AS cod_complemento3_julgamento_colegiado,
    CASE WHEN mjc.dthrmov IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento_colegiado,
    'EJUD' AS ind_sistema,
    current_timestamp as DTH_ETL,
    --indicadores do ultimo movimento CNJ
    mcnj.dthrmov AS dth_movimento_ultimo_cnj,
    mcnj.codfase AS cod_fase_ultimo_cnj,
    mcnj.codcompl1 AS cod_complemento1_ultimo_cnj,
    mcnj.codcompl2 AS cod_complemento2_ultimo_cnj,
    mcnj.codcompl3 AS cod_complemento3_ultimo_cnj
FROM acervo a
LEFT JOIN mov_movimento m ON a.coddoc = m.coddoc
LEFT JOIN mov_suspensao_repercussao msr ON a.coddoc = msr.coddoc
LEFT JOIN mov_julgamento mj ON a.coddoc = mj.coddoc
LEFT JOIN mov_localfisico mlf ON a.coddoc = mlf.coddoc
LEFT JOIN mov_localvirtual mlv ON a.coddoc = mlv.coddoc
LEFT JOIN mov_conclusao mc ON a.coddoc = mc.coddoc
left join mov_decisao mdec on a.coddoc = mdec.coddoc
LEFT JOIN mov_julgamento_monocratico mjm ON a.coddoc = mjm.coddoc
LEFT JOIN mov_julgamento_colegiado mjc ON a.coddoc = mjc.coddoc
LEFT JOIN mov_distribuicao_primeira_valida mdpv ON a.coddoc = mdpv.coddoc
LEFT JOIN mov_distribuicao_primeira_cargo mdpc ON mdpv.codmagrel = mdpc.codmagrel
LEFT JOIN mov_distribuicao_ultima_cargo mduc ON a.cod_ultimo_relator = mduc.cod_ultimo_relator
LEFT JOIN mov_movimento_cnj mcnj on a.coddoc = mcnj.coddoc;
