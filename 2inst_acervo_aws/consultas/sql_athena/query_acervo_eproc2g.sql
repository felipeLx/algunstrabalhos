WITH processo_referencia AS (
    SELECT 
        id_processo, 
    	LPAD(CAST(num_processo AS VARCHAR), 20, '0') AS num_processo, 
    	id_orgao_colegiado,
        current_timestamp AS dth_referencia
    FROM EPROC2G.PROCESSO
), ultima_distribuicao AS (
    SELECT * FROM (
        SELECT
            PE.id_processo,
            PE.dth_evento,
            EJ.cod_mni_movimento,
            PE.ID_PROCESSO_EVENTO,
            ROW_NUMBER() OVER (PARTITION BY PR.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM EPROC2G.PROCESSO_EVENTO PE
        JOIN processo_referencia PR 
            ON PE.id_processo = PR.id_processo 
           AND PE.dth_evento <= current_timestamp
        JOIN EPROC2G.EVENTO_JUDICIAL EJ 
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN EPROC2G.MNI_MOVIMENTO MM 
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
        WHERE EJ.cod_mni_movimento IN (26,36)
    ) x WHERE rn = 1
), ultimo_cancelamento_distribuicao AS (
    SELECT * FROM (
        SELECT
            PE.id_processo,
            PE.ID_PROCESSO_EVENTO,
            PE.dth_evento,
            EJ.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY PR.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM EPROC2G.PROCESSO_EVENTO PE
        JOIN processo_referencia PR 
            ON PE.id_processo = PR.id_processo AND PE.dth_evento <= current_timestamp
        JOIN EPROC2G.EVENTO_JUDICIAL EJ 
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN EPROC2G.MNI_MOVIMENTO MM 
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
        WHERE EJ.cod_mni_movimento IN (488,12186)
    ) x WHERE rn = 1
), ultima_distribuicao_valida AS (
    SELECT 
        ud.id_processo,
        ud.dth_evento AS dth_ultima_distribuicao,
        ud.cod_mni_movimento,
        UD.ID_PROCESSO_EVENTO
    FROM ultima_distribuicao ud
    LEFT JOIN ultimo_cancelamento_distribuicao ucd 
        ON ud.id_processo = ucd.id_processo AND ud.dth_evento < ucd.dth_evento 
    WHERE ucd.id_processo IS NULL
), ultima_baixa AS (
    SELECT * FROM (
        SELECT
            PE.id_processo,
            PE.dth_evento AS dth_ultima_baixa,
            EJ.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY PR.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM EPROC2G.PROCESSO_EVENTO PE
        JOIN processo_referencia PR
            ON PE.id_processo = PR.id_processo
           AND PE.dth_evento <= current_timestamp
        JOIN EPROC2G.EVENTO_JUDICIAL EJ
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN EPROC2G.MNI_MOVIMENTO MM
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
        WHERE EJ.cod_mni_movimento = 22
    ) x WHERE rn = 1
), ultimo_movimento AS (
    SELECT * FROM (
        SELECT
            PE.id_processo,
            PE.dth_evento AS dth_ultimo_movimento,
            EJ.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY PR.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM EPROC2G.PROCESSO_EVENTO PE
        JOIN processo_referencia PR
            ON PE.id_processo = PR.id_processo
           AND PE.dth_evento <= current_timestamp
        JOIN EPROC2G.EVENTO_JUDICIAL EJ
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN EPROC2G.MNI_MOVIMENTO MM
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
    ) x WHERE rn = 1
), ultima_distribuicao_orgao as (
		SELECT *
		FROM (
			SELECT 
				lp.id_processo as id_processo,
				lp.id_orgao_colegiado,
				ROW_NUMBER() OVER (PARTITION BY lp.id_processo ORDER BY lp.dth_alteracao DESC) AS rn
			FROM processo_referencia PR
			LEFT JOIN eproc2g.log_processo LP
				ON PR.id_processo  = LP.id_processo
			AND LP.dth_alteracao <= current_timestamp
			AND LP.id_orgao_colegiado IS NOT NULL
			AND LP.id_orgao_colegiado not in (3583,358301)
		) X
		WHERE rn = 1
)
SELECT 
    current_timestamp 																	AS dth_referencia,
    pr.id_processo 																		AS cod_documento,
    LPAD(SUBSTRING(pr.num_processo, 1, 7), 7, '0') || '-' ||
    LPAD(SUBSTRING(pr.num_processo, 8, 2), 2, '0') || '.' ||
    LPAD(SUBSTRING(pr.num_processo, 10, 4), 4, '0') || '.' ||
    LPAD(SUBSTRING(pr.num_processo, 14, 1), 1, '0') || '.' ||
    LPAD(SUBSTRING(pr.num_processo, 15, 2), 2, '0') || '.' ||
    LPAD(SUBSTRING(pr.num_processo, 17, 4), 4, '0') 									AS num_processo,
    ud.dth_ultima_distribuicao,
    hdj.cod_magistrado 																	as cod_ultimo_relator,
    coalesce(udo.id_orgao_colegiado, pr.id_orgao_colegiado, 0)							as cod_ultimo_orgao_julgador
FROM processo_referencia pr
JOIN ultima_distribuicao_valida ud ON pr.id_processo = ud.id_processo
LEFT JOIN ultima_baixa ub 
    ON ud.id_processo = ub.id_processo AND ub.dth_ultima_baixa >= ud.dth_ultima_distribuicao
LEFT JOIN ultimo_movimento um ON ud.id_processo = um.id_processo
LEFT JOIN EPROC2G.MNI_MOVIMENTO mm ON um.cod_mni_movimento = mm.cod_mni_movimento
JOIN EPROC2G.HISTORICO_DISTRIBUICAO_JUDICIAL hdj
    ON ud.id_processo = hdj.id_processo AND ud.ID_PROCESSO_EVENTO = hdj.id_processo_evento 
left JOIN ultima_distribuicao_orgao udo on pr.id_processo = udo.id_processo
WHERE ub.id_processo IS NULL;