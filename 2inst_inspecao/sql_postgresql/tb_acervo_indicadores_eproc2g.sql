with acervo as  (
	select 
		DTH_REFERENCIA,
		COD_DOCUMENTO,
		dth_ultima_distribuicao,
		COD_ULTIMO_RELATOR,
		cod_ultimo_orgao_julgador 
	from stg2inst.tb_acervo_eproc2g
), movimento as (
		SELECT *
		FROM (
			SELECT
				pe.id_processo,
				PE.dth_evento,
				PE.ID_EVENTO_JUDICIAL,
				ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
			FROM acervo A
			JOIN EPROC2G.processo_evento PE ON A.COD_DOCUMENTO = PE.ID_PROCESSO AND PE.dth_evento <= A.dth_referencia
		) X
		WHERE rn = 1
), mov_julgamento as (
		SELECT *
		FROM (
			SELECT
				PE.ID_PROCESSO,
				PE.dth_evento,
				EJ.id_evento_judicial,
				MM.cod_mni_movimento,
				ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
			FROM acervo a
			JOIN EPROC2G.processo_evento PE on A.cod_documento = PE.id_processo
			AND PE.dth_evento <= A.dth_referencia
			JOIN EPROC2G.evento_judicial EJ
				ON PE.id_evento_judicial = EJ.id_evento_judicial
			JOIN EPROC2G.mni_movimento MM
				ON MM.cod_mni_movimento = EJ.cod_mni_movimento
			WHERE MM.cod_mni_movimento_pai IN (
				385, 210, 214, 12433, 12326, 212, 973, 12450, 12661, 12321, 12678,
				218, 228, 15253, 456, 15260, 12709
			)
		) X
		WHERE rn = 1
), mov_conclusao as (
		SELECT *
		FROM (
			SELECT
				PE.ID_PROCESSO,
				PE.dth_evento,
				EJ.id_evento_judicial,
				MM.cod_mni_movimento,
				ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
			FROM acervo a
			JOIN EPROC2G.processo_evento PE on A.cod_documento = PE.id_processo
			AND PE.dth_evento <= A.dth_referencia
			JOIN EPROC2G.evento_judicial EJ
				ON PE.id_evento_judicial = EJ.id_evento_judicial
			JOIN EPROC2G.mni_movimento MM
				ON MM.cod_mni_movimento = EJ.cod_mni_movimento
			WHERE MM.cod_mni_movimento = 51
		) X
		WHERE rn = 1
), mov_suspensao as (
	SELECT *
	FROM (
		SELECT
			PE.ID_PROCESSO,
			PE.dth_evento,
			EJ.id_evento_judicial,
			MM.cod_mni_movimento,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		FROM ACERVO A
		JOIN processo_evento PE
			ON A.COD_DOCUMENTO = PE.id_processo AND PE.dth_evento <= A.dth_referencia
		JOIN evento_judicial EJ
			ON PE.id_evento_judicial = EJ.id_evento_judicial
		JOIN mni_movimento MM
			ON MM.cod_mni_movimento = EJ.cod_mni_movimento
		WHERE MM.cod_mni_movimento_pai = 25
	) AS X
	WHERE rn = 1
), mov_sobrestamento as (
	SELECT *
	FROM (
		SELECT
			PE.ID_PROCESSO,
			PE.dth_evento,
			EJ.id_evento_judicial,
			MM.cod_mni_movimento,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		FROM ACERVO A
		JOIN processo_evento PE
			ON A.COD_DOCUMENTO = PE.id_processo AND PE.dth_evento <= A.dth_referencia
		JOIN evento_judicial EJ
			ON PE.id_evento_judicial = EJ.id_evento_judicial
		JOIN mni_movimento MM
			ON MM.cod_mni_movimento = EJ.cod_mni_movimento
		WHERE MM.cod_mni_movimento in (265, 11975, 12098, 14968, 14969, 12099, 12100, 14971)
	) AS X
	WHERE rn = 1
), mov_levantamento_suspensao as (
	SELECT *
	FROM (
		SELECT
			PE.ID_PROCESSO AS id_processo,
			PE.dth_evento,
			EJ.id_evento_judicial,
			MM.cod_mni_movimento,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		FROM ACERVO A
		JOIN processo_evento PE
			ON A.COD_DOCUMENTO = PE.id_processo AND PE.dth_evento <= A.dth_referencia
		JOIN evento_judicial EJ
			ON PE.id_evento_judicial = EJ.id_evento_judicial
		JOIN mni_movimento MM
			ON MM.cod_mni_movimento = EJ.cod_mni_movimento
		WHERE MM.cod_mni_movimento in (15414, 14982, 14981, 12066, 14974, 12067, 12068, 14980, 14979, 14985, 15416, 14984, 14976, 14975, 14977, 14978, 14983)
	) AS X
	WHERE rn = 1
), mov_suspensao_valida as (
	select 
		ms.id_processo,
		ms.dth_evento
	from mov_suspensao ms
	left join mov_levantamento_suspensao mls on ms.id_processo = mls.id_processo and mls.dth_evento > ms.dth_evento
	where mls.id_processo is null
), mov_sobrestamento_valida as (
	select 
		ms.id_processo,
		ms.dth_evento
	from mov_sobrestamento ms
	left join mov_levantamento_suspensao mls on ms.id_processo = mls.id_processo and mls.dth_evento > ms.dth_evento
	where mls.id_processo is null
), mov_localfisico as (
	select * from (
		select 
			pe.id_processo,
			pe.dth_evento,
			pe.complemento,
			pro.id_orgao_destino,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		from acervo A
		join eproc2g.processo_evento pe on A.COD_DOCUMENTO = PE.ID_PROCESSO and PE.DTH_EVENTO < A.dth_referencia 
		join eproc2g.processo_remessa_orgao pro on pe.id_processo_evento = pro.id_processo_evento_remessa
	) X where RN = 1
), mov_julgamento_monocratico as (
	SELECT *
	FROM (
		SELECT
			PE.ID_PROCESSO,
			PE.dth_evento,
			EJ.id_evento_judicial,
			ej.des_evento,
			MM.cod_mni_movimento,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		FROM acervo a 
		join EPROC2G.processo_evento PE on a.cod_documento = pe.id_processo and PE.DTH_EVENTO < A.dth_referencia 
		JOIN EPROC2G.evento_judicial EJ
			ON PE.id_evento_judicial = EJ.id_evento_judicial
		JOIN EPROC2G.mni_movimento MM
			ON MM.cod_mni_movimento = EJ.cod_mni_movimento
		WHERE MM.cod_mni_movimento IN (972, 12256, 12319)
	) X
	WHERE rn = 1
), mov_julgamento_colegiado as (
	SELECT *
	FROM (
		SELECT
			PE.ID_PROCESSO,
			PE.dth_evento,
			EJ.id_evento_judicial,
			ej.des_evento,
			MM.cod_mni_movimento,
			ROW_NUMBER() OVER (PARTITION BY PE.id_processo ORDER BY PE.dth_evento DESC) AS rn
		FROM acervo a 
		join EPROC2G.processo_evento PE on a.cod_documento = pe.id_processo and PE.DTH_EVENTO < A.dth_referencia 
		JOIN EPROC2G.evento_judicial EJ
			ON PE.id_evento_judicial = EJ.id_evento_judicial
		JOIN EPROC2G.mni_movimento MM
			ON MM.cod_mni_movimento = EJ.cod_mni_movimento
		WHERE MM.cod_mni_movimento IN (219, 220, 221, 235, 235, 236, 236, 236, 237, 238, 239, 240, 241, 242, 442, 12252, 12253, 12254, 12257)
	) X
	WHERE rn = 1	
)
select 
	A.DTH_REFERENCIA,
	A.COD_DOCUMENTO,
	A.DTH_ULTIMA_DISTRIBUICAO,
	A.COD_ULTIMO_RELATOR,
	A.cod_ultimo_orgao_julgador,
	m.DTH_EVENTO as DTH_ULTIMO_MOVIMENTO,
	m.ID_EVENTO_JUDICIAL as COD_FASE,
	MSV.DTH_EVENTO as DTH_SUSPENSAO,
	CASE WHEN MSV.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS IND_SUSPENSO,
	MSOBV.dth_evento as DTH_SOBRESTAMENTO,
	CASE WHEN MSOBV.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS IND_SOBRESTAMENTO,
	mj.dth_Evento as DTH_JULGAMENTO,
	CASE WHEN MJ.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS IND_JULGAMENTO,
    MLF.DTH_EVENTO as DTH_ULTIMO_LOCAL, --> REMESSA_ORGAO OU COMPLEMENTO NA PROCESSO_EVENTO?
	MLF.ID_ORGAO_DESTINO as COD_ULTIMO_LOCAL, --> REMESSA_ORGAO OU COMPLEMENTO NA PROCESSO_EVENTO?
	null as dth_entrada_localizador, --DTH_ULTIMO_LOCAL_VIRTUAL,
	null as cod_ultimo_localizador, --COD_ULTIMO_LOCAL_VIRTUAL
	mc.dth_evento as DTH_ULTIMA_CONCLUSAO,
	CASE WHEN MC.dth_evento IS NOT NULL THEN 'S' ELSE 'N' END AS IND_CONCLUSAO,
	MJM.DTH_EVENTO as DTH_ULTIMO_JULG_MONOCRATICO,
	MJC.DTH_EVENTO as DTH_ULTIMO_JULG_COLEGIADO
from ACERVO A
left join movimento m on A.cod_documento = m.id_processo
left join mov_julgamento mj on a.cod_documento = mj.id_processo
left join mov_conclusao mc on A.COD_DOCUMENTO = mc.ID_PROCESSO
left join mov_suspensao_valida msv on a.cod_documento = msv.id_processo
left join mov_sobrestamento_valida msobv on a.cod_documento = msobv.id_processo
left join mov_localfisico MLF on a.cod_documento = mlf.id_processo
left join mov_julgamento_monocratico MJM on A.COD_DOCUMENTO = MJM.ID_PROCESSO
left join mov_julgamento_colegiado MJC on A.COD_DOCUMENTO = MJC.ID_PROCESSO;