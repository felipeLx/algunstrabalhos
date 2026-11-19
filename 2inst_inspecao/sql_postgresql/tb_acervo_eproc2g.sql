WITH ultima_distribuicao AS (
	SELECT * FROM (
	    SELECT
	        PE.id_processo,
	        PE.dth_evento,
	        EJ.cod_mni_movimento,
	        hdj.cod_magistrado,
	        ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
	    FROM EPROC2G.PROCESSO_EVENTO PE 
	    left JOIN EVENTO_JUDICIAL EJ 
	        ON PE.id_evento_judicial = EJ.id_evento_judicial
	    left JOIN MNI_MOVIMENTO MM 
	        ON MM.cod_mni_movimento = EJ.cod_mni_movimento
	    join EPROC2G.historico_distribuicao_judicial hdj on PE.ID_PROCESSO_EVENTO = hdj.id_processo_evento and pe.id_processo = hdj.id_processo
	    and PE.DTH_EVENTO <= '{v_dateref_str}'  
	    and EJ.cod_mni_movimento IN (26,36)
	) x WHERE rn = 1
), ultimo_cancelamento_distribuicao AS (
    SELECT * FROM (
        SELECT
            PE.id_processo,
            PE.dth_evento,
            EJ.cod_mni_movimento,
            ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM PROCESSO_EVENTO PE 
        JOIN EVENTO_JUDICIAL EJ 
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN MNI_MOVIMENTO MM 
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
        WHERE EJ.cod_mni_movimento IN (488,83) 
        and PE.dth_evento <= '{v_dateref_str}' 
    ) x WHERE rn = 1
), ultima_distribuicao_valida AS (
    SELECT 
        ud.id_processo,
        ud.dth_evento AS dth_ultima_distribuicao,
        ud.cod_mni_movimento,
        ud.cod_magistrado
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
            ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
        FROM PROCESSO_EVENTO PE 
        JOIN EVENTO_JUDICIAL EJ
            ON PE.id_evento_judicial = EJ.id_evento_judicial
        JOIN MNI_MOVIMENTO MM
            ON MM.cod_mni_movimento = EJ.cod_mni_movimento
        WHERE EJ.cod_mni_movimento = 22
        and PE.dth_evento <= '{v_dateref_str}' 
    ) x WHERE rn = 1
), ultimo_orgaojulgador as (
	SELECT ID_PROCESSO, id_orgao_colegiado
	FROM (
		SELECT 		
			LP.id_processo,
			lp.id_orgao_colegiado,
			ROW_NUMBER() OVER (PARTITION BY lp.id_processo ORDER BY lp.dth_alteracao DESC) AS rn
		FROM EPROC2G.log_processo LP
		WHERE LP.dth_alteracao <= '{v_dateref_str}' 
		AND LP.id_orgao_colegiado IS NOT NULL
	) X
	WHERE rn = 1
)
SELECT 
    '{v_dateref_str}'::timestamp                                                                        as dth_referencia,
   	ud.id_processo::numeric(30,0)                                                                       AS cod_documento,
    ud.dth_ultima_distribuicao::timestamp                                                               AS DTH_ULTIMA_DISTRIBUICAO,
    ud.cod_magistrado::INT                                                                              AS cod_ultimo_relator,
    coalesce(uoj.id_orgao_colegiado, p.id_orgao_colegiado, 0)::int                                      as cod_ultimo_orgao_julgador
FROM ultima_distribuicao_valida ud
left join ultimo_orgaojulgador uoj on ud.id_processo = uoj.id_processo
LEFT JOIN ultima_baixa ub 
    ON ud.id_processo = ub.id_processo AND ub.dth_ultima_baixa >= ud.dth_ultima_distribuicao
left join eproc2g.processo p on ud.id_processo = p.id_processo
WHERE ub.id_processo IS NULL;