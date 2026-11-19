WITH processo AS (
	select 
		p.ID_PROCESSO,
		LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 1, 7), 7, '0') || '-' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 8, 2), 2, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 10, 4), 4, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 14, 1), 1, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 15, 2), 2, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 17, 4), 4, '0') AS COD_CNJ,
	    case
	    	when P.ID_SIGILO = 5 then 'RESTRITO'
	    	else LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 1, 7), 7, '0') || '-' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 8, 2), 2, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 10, 4), 4, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 14, 1), 1, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 15, 2), 2, '0') || '.' ||
	    LPAD(SUBSTRING(LPAD(CAST(p.num_processo AS STRING), 20, '0') , 17, 4), 4, '0')
	    end as COD_CNJ_SIGILO,    
	    P.ID_SIGILO,
	    p.DTH_AUTUACAO,
		p.ID_CLASSE_JUDICIAL,
		cast(cj.COD_CLASSE as STRING) as COD_CLASSE,
		AJ.cod_mni_assunto_judicial,
		cj.DES_CLASSE,	
		P.COD_COMPETENCIA,
		CPT.DES_COMPETENCIA,
		CPT.SIN_CRIMINAL,
		CPT.SIN_CIVEL,
		p.ID_ASSUNTO_PRINCIPAL,
		AJ.COD_ASSUNTO,
		AJ.DES_ASSUNTO
	from eproc2g.processo P
	left join eproc2g.classe_judicial cj on p.id_classe_judicial = cj.id_classe_judicial
	left join eproc2g.competencia_judicial cpt on p.cod_competencia = cpt.cod_competencia
	left join eproc2g.assunto_judicial AJ on P.ID_ASSUNTO_PRINCIPAL = AJ.ID_ASSUNTO_JUDICIAL
), processo_origem as (
	select * from (
		select 
			p.id_processo, 
			p.num_processo, 
			pr.id_processo_relacionado,
			pr.NUM_PROCESSO_ORIGINARIO as NUM_PROCESSO_ORIGEM,
			TRIM(CAST(PR.cod_orgao_origem AS STRING)) as COD_ORGAO_ORIGEM,
			pr.sig_orgao_origem,
			O.DES_ORGAO as orgao_origem,
			CJ1.id_classe_judicial as id_classe_judicial_origem,
			pr.cod_classe_origem,
			cj1.des_classe as classe_origem, 
			pr.dth_autuacao_origem,
			'EPROC1G' as IND_SISTEMA_ORIGEM,
			CASE 
				when UPPER(CJ1.DES_CLASSE) like '%DESTITUIÇÃO DO PODER FAMILIAR%' then 'S'
				else 'N'
			end as IND_DESTITUICAO_PODER_FAMILIAR,
			ROW_NUMBER() OVER (PARTITION BY p.id_processo ORDER BY pr.dth_autuacao_origem ASC) AS rn 
		from eproc2g.processo p 
		join eproc2g.processo_relacionado pr on pr.id_processo = p.id_processo
		left join eproc1g.classe_judicial cj1 on pr.cod_classe_origem = cj1.cod_classe
		left join eproc1g.orgao O on TRIM(CAST(PR.cod_orgao_origem AS STRING)) = CAST(O.ID_ORGAO as STRING)
		where pr.sin_ativo = 'S' 			
	) x where rn = 1
), polo_ativo as (
	select
		pp.id_processo,
	    array_join(array_agg(CAST(pp.id_pessoa AS STRING)), ', ') AS lista_pessoa,
	    array_join(array_agg(pn.nome_pessoa), ', ') AS lista_nomepessoa
	FROM eproc2g.processo_parte pp
	JOIN eproc2g.pessoa_nome pn
	    ON pp.id_pessoa = pn.id_pessoa
	JOIN eproc2g.tipo_parte tp
	    ON pp.cod_tipo_parte = tp.cod_tipo_parte
	WHERE tp.cod_mni_modalidade_polo_processual = 1
	GROUP BY pp.id_processo
), polo_passivo as (
	select
		pp.id_processo,
	    array_join(array_agg(CAST(pp.id_pessoa AS STRING)), ', ') AS lista_pessoa,
	    array_join(array_agg(pn.nome_pessoa), ', ') AS lista_nomepessoa
	FROM eproc2g.processo_parte pp
	JOIN eproc2g.pessoa_nome pn
	    ON pp.id_pessoa = pn.id_pessoa
	JOIN eproc2g.tipo_parte tp
	    ON pp.cod_tipo_parte = tp.cod_tipo_parte
	WHERE tp.cod_mni_modalidade_polo_processual = 2
	GROUP BY pp.id_processo
), processo_ultima_ocorrencia AS (
	SELECT *
	FROM (
		SELECT 
			pdc.id_processo,
			dc.nome AS nome_campo,
			dcv.dado_comp_valor,
			pdc.id_dado_comp_valor,
			dcv.sin_exibe_rotulo,
			ROW_NUMBER() OVER (PARTITION BY pdc.id_processo, dc.nome ORDER BY pdc.dth_inclusao DESC) AS rn
		FROM eproc2g.processo_dado_complementar pdc
		JOIN eproc2g.dado_complementar_valor dcv
			ON pdc.id_dado_complementar = dcv.id_dado_complementar
			AND pdc.id_dado_comp_valor = dcv.id_dado_comp_valor
		JOIN eproc2g.dado_complementar dc
			ON dcv.id_dado_complementar = dc.id_dado_complementar
		WHERE dc.nome IN (
			'VISTA_MINISTERIO_PUBLICO',
			'REU_PRESO',
			'IDOSO',
			'CRIANCA_E_ADOLESCENTE',
			'PETICAO_URGENTE',
			'IDOSO_MAIOR_80',
			'DOENCA_GRAVE',
			'PESSOA_COM_DEFICIENCIA',
			'PESSOA_SITUACAO_RUA',
			'ADOLESC_INT'
		) and pdc.sin_ativo = 'S'
	) x
	WHERE rn = 1
), indicadores_processo as (
	SELECT
	    u.id_processo,
	    MAX(CASE WHEN u.nome_campo = 'VISTA_MINISTERIO_PUBLICO' AND u.rn = 1 and u.id_dado_comp_valor = 1 THEN 'S' ELSE 'N' END) AS VISTA_MP,
	    MAX(CASE WHEN u.nome_campo = 'REU_PRESO' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS REU_PRESO,
	    MAX(CASE WHEN u.nome_campo = 'IDOSO' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS IDOSOS,
	    MAX(CASE WHEN u.nome_campo = 'CRIANCA_E_ADOLESCENTE' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS CRIANCA_ADOLESCENTE,
	    MAX(CASE WHEN u.nome_campo = 'PETICAO_URGENTE' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS URGENTE,
	    MAX(CASE WHEN u.nome_campo = 'IDOSO_MAIOR_80' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS IDOSOS_80_MAIS,
	    MAX(CASE WHEN u.nome_campo = 'DOENCA_GRAVE' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS DOENCA_GRAVE,
	    MAX(CASE WHEN u.nome_campo = 'PESSOA_COM_DEFICIENCIA' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS PRIORIDADE_DEFICIENTE,
	    MAX(CASE WHEN u.nome_campo = 'PESSOA_SITUACAO_RUA' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS SITUACAO_RUA,
	    MAX(CASE WHEN u.nome_campo = 'ADOLESC_INT' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS ADOLESCENTE_INTERNADO
	FROM processo_ultima_ocorrencia u
	WHERE u.rn = 1
	GROUP BY u.id_processo
), indicador_funciona_mp as (
	select * from (
		select 
			pp.id_processo,
			pn.id_pessoa,
			pp.dth_inclusao,
			pp.dth_desativacao,
			pn.dth_inclusao,
			pn.dth_desativacao,
			ROW_NUMBER() OVER (PARTITION BY PP.id_processo ORDER BY pp.dth_inclusao DESC) AS rn	
		from eproc2g.processo_parte PP
		left JOIN eproc2g.pessoa_nome PN on PP.ID_PESSOA = PN.ID_PESSOA
		where UPPER(PN.nome_pessoa) like '%MINISTÉRIO%' and UPPER(PN.nome_pessoa) like '%PÚBLICO%'
		AND pp.dth_desativacao IS NULL
		AND pn.dth_desativacao IS NULL
	) x where rn = 1
), indicador_crime_adm_publico as (
	SELECT
	    p.id_processo,
	    CASE 
		    when PAP.ID_PROCESSO is null then 'N'
		    else 'S'
	    END AS IND_CRIME_ADM_PUBLICO
	FROM eproc2g.processo P 
	LEFT join (
	select PA.ID_PROCESSO from eproc2g.processo_assunto pa
	LEFT JOIN eproc2g.assunto_judicial aj 
	    ON pa.id_assunto_judicial = aj.id_assunto_judicial
	LEFT JOIN eproc2g.mni_assunto_judicial maj 
	    ON aj.cod_mni_assunto_judicial = maj.cod_mni_assunto_judicial
	WHERE maj.cod_mni_assunto_judicial IN 
	(3415,3430,3432,3523,3524,3527,3529,3530,3531,3533,3534,3535,3536,3539,3540,3541,
	3545,3547,3548,3550,3551,3552,3553,3554,3555,3556,3557,3558,3560,3561,3562,3563,
	3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,3574,3576,3577,3579,3580,3582,
	3585,3589,3592,3593,3594,3595,3596,3597,3598,3603,3604,3606,3612,3614,3628,3642,
	3650,3651,5571,5872,5873,5874,5875,5876,5877,5903,5904,5905,5906,5907,5908,5909,
	5910,9674,9689,9691,9704,9807,9808,9809,9811,9812,9813,9815,9816,9817,9818,9821,
	9822,9823,9826,9828,9829,9830,9831,9832,9833,9834,9835,9836,9837,9838,9839,9840,
	9841,9842,9843,9844,9845,9846,9847,9857,9872,9874,9888,9899,9903,9904,9917,9918,
	9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,9931,9932,9934,9935,
	9937,9938,9940,9943,9947,9950,9951,9952,9953,9955,9956,9957,9958,9959,9960,9961,
	9962,9963,10012,10013,10014,10982,10983,10984,10985,10986,10990,10991,10993,
	10994,10995,10998,11005,11006,11007,11068,11072,11073,11152,11166,11167,11797)
	group by PA.ID_PROCESSO) PAP on P.ID_PROCESSO = PAP.ID_PROCESSO
), INDICADOR_PODER_FAMILIAR as (
	select 
		P.ID_PROCESSO,
		CJ.id_classe_judicial
	from eproc1g.processo P
	left join eproc1g.classe_judicial CJ on P.ID_CLASSE_JUDICIAL = CJ.ID_CLASSE_JUDICIAL
	join eproc1g.relacao_classe_judicial_mni RCJ on CJ.id_classe_judicial = RCJ.ID_CLASSE_JUDICIAL
	join eproc1g.mni_classe_judicial mcj  on RCJ.cod_mni_classe_judicial = MCJ.cod_mni_classe_judicial 
	where UPPER(mcj.des_mni_classe_judicial) like '%DESTITUIÇÃO DO PODER FAMILIAR%'
)
select 
	--INDICADORES DO PROCESSO
	P.ID_PROCESSO,									-- CHAVE PRIMARIA DO PROCESSO PARA EPROC2G
	p.DTH_AUTUACAO,									-- DATA DE AUTUACAO DO PROCESSO
	p.COD_CNJ,										-- CODIGO PARA O CNJ DO PROCESSO
	p.COD_CNJ_SIGILO,								-- CODIGO CNJ DO PROCESSO MASCARADO POR SIGILO 5
	p.ID_SIGILO,									-- ID DE IDENTIFICACAO DO SIGILO
    --CLASSE
	p.ID_CLASSE_JUDICIAL,							--	CHAVE PRIMARIA DA CLASSE
	cast(p.COD_CLASSE as STRING) as COD_CLASSE, 	--	CODIGO DE CLASSE DE EXIBICAO DO SISTEMA
	p.DES_CLASSE,									--	DESCRICAO DA CLASSE JUDICIAL
	CAST(NULL AS STRING) AS COD_MNI_CLASSE,		--	CODIGO TPU DA CLASSE
	--COMPETENCIA
	p.COD_COMPETENCIA,								-- CODIGO DE COMPETENCIA DO SISTEMA
	p.DES_COMPETENCIA,								-- DESCRICAO DA COMPETENCIA DO SISTEMA
	p.SIN_CRIMINAL,									-- BOOLEANO PARA NATUREZA CRIMINAL
	p.SIN_CIVEL,									-- BOOLEANO PARA NATUREZA CIVEL
	--ASSUNTO
	p.ID_ASSUNTO_PRINCIPAL,							-- CHAVE PRIMARIA DO ASSUNTO
	p.COD_ASSUNTO,									-- CODIGO DO ASSUNTO NO SISTEMA
	p.DES_ASSUNTO,									-- DESCRICAO DO ASSUNTO
	P.COD_MNI_ASSUNTO_JUDICIAL,						-- CODIGO DO ASSUNTO NA TPU                             
	--INDICADORES DE ORIGEM
	CASE 
	    WHEN substr(p.COD_CNJ, -4) = '0000' THEN 'S'
	    ELSE 'N'
	END AS IND_ORIGINARIO,
	case
		when po.ID_PROCESSO is null then 'N'
		else 'S'
	end as IND_AUTOASSOCIADO_ORIGEM,
	po.ID_PROCESSO_RELACIONADO,
--	po.NUM_PROCESSO_ORIGEM,
	LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 1, 7), 7, '0') || '-' ||
    LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 8, 2), 2, '0') || '.' ||
    LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 10, 4), 4, '0') || '.' ||
    LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 14, 1), 1, '0') || '.' ||
    LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 15, 2), 2, '0') || '.' ||
    LPAD(SUBSTRING(LPAD(CAST(po.NUM_PROCESSO_ORIGEM AS STRING), 20, '0') , 17, 4), 4, '0') AS NUM_PROCESSO_ORIGEM,
    PO.IND_SISTEMA_ORIGEM,
	po.COD_ORGAO_ORIGEM,
	po.SIG_ORGAO_ORIGEM,
	po.ORGAO_ORIGEM,
	po.ID_CLASSE_JUDICIAL_ORIGEM,
	po.COD_CLASSE_ORIGEM,
	po.CLASSE_ORIGEM, 
	po.DTH_AUTUACAO_ORIGEM,
	pa.lista_nomepessoa || ' X ' || pp.lista_nomepessoa as PARTES,
	ip.VISTA_MP,
	case 
		when fmp.id_processo is not null then 'S'
		else 'N'
	end as FUNCIONA_MP,
	ip.REU_PRESO,
	ip.IDOSOS,
	ip.CRIANCA_ADOLESCENTE,
	ip.URGENTE,
	ip.IDOSOS_80_MAIS,
	ip.DOENCA_GRAVE,
	ip.PRIORIDADE_DEFICIENTE,
	ip.SITUACAO_RUA,
	ip.ADOLESCENTE_INTERNADO,
	PO.IND_DESTITUICAO_PODER_FAMILIAR						as IND_DESTITUICAO_PODER_FAMILIAR,
	ic.IND_CRIME_ADM_PUBLICO,
	case
		when ipf.id_processo is not null then 'S'
		else 'N'
	end as IND_PODER_FAMILIAR,
	'EPROC2G' as IND_SISTEMA,
	current_timestamp as DTH_ETL
from processo p 
left join processo_origem po on p.id_processo = po.id_processo
left join polo_ativo pa on p.id_processo = pa.id_processo
left join polo_passivo pp on p.id_processo = pp.id_processo
left join indicadores_processo ip on p.id_processo = ip.id_processo
left join indicador_funciona_mp fmp on p.id_processo = fmp.id_processo
LEFT join indicador_crime_adm_publico ic on p.id_processo = ic.id_processo
left join indicador_poder_familiar ipf on p.id_processo = ipf.id_processo;