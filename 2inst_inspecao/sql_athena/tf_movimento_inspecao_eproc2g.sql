WITH ORGAO_GABINETE_MAGISTRADO as ( 
	select 
		OS.ID_ORGAO, 
		O.ID_ORGAO as COD_ORGAO, 
		O.DES_ORGAO, 
		JM.COD_MAGISTRADO, 
		M.NOME_MAGISTRADO 
	from EPROC2G.ORGAO OS 
	join (select ID_ORGAO, COD_ORGAO_SECRETARIA, DES_ORGAO from EPROC2G.ORGAO where COD_ORGAO_SECRETARIA is not NULL) O
	ON cast(OS.ID_ORGAO as VARCHAR) = cast(O.COD_ORGAO_SECRETARIA as VARCHAR)
	join (select COD_MAGISTRADO, ID_ORGAO from EPROC2G.JUIZO_MAGISTRADO where COD_SITUACAO_MAGISTRADO_JUIZO = 1 and SIN_ATIVO = 'S' and DTH_FINAL_JUIZO >= CURRENT_TIMESTAMP) JM on cast(O.ID_ORGAO as VARCHAR)= cast(JM.ID_ORGAO as VARCHAR)
	join EPROC2G.MAGISTRADO M on cast(JM.COD_MAGISTRADO as VARCHAR) = cast(M.COD_MAGISTRADO as VARCHAR)
), mov_distribuido as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
--		PE.ID_ORGAO_EVENTO, nao tem essa informação na processo_evento
		hdj.cod_magistrado,
		hdj.id_orgao_destino as id_orgao_evento,
		null as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'DISTRIBUIDO_REDISTRIBUIDO' as TP_MOVIMENTO,
		'ITEM_11_1' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
    join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where cod_mni_movimento in (26,36)) mm on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    join eproc2g.historico_distribuicao_judicial hdj on pe.id_processo_evento = hdj.id_processo_evento
    WHERE ej.cod_mni_movimento in (26,36)
), mov_julgado_monocraticamente as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null as int) as cod_magistrado,
		PE.ID_ORGAO_EVENTO,
		case 
			when SJIM.id_sessao_julgamento_item is not null then 'S'
			else 'N'
		end as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'JULGAMENTO_MONOCRATICO' as TP_MOVIMENTO,
		'ITEM_11_2' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
    join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 193 > %') mm on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    left join EPROC2G.sessao_julgamento_item SJI on PE.id_processo_evento = SJI.id_processo_evento_resultado 
	left join (select distinct id_sessao_julgamento_item from eproc2g.sessao_julgamento_item_MAGISTRADO) SJIM on SJIM.ID_SESSAO_JULGAMENTO_ITEM = SJI.ID_SESSAO_JULGAMENTO_ITEM 
    where sjim.id_sessao_julgamento_item is null   
), mov_julgado_colegiado as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null as int) as cod_magistrado,
		PE.ID_ORGAO_EVENTO,
		case 
			when SJIM.id_sessao_julgamento_item is not null then 'S'
			else 'N'
		end as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'JULGAMENTO_COLEGIADO' as TP_MOVIMENTO,
		'ITEM_11_3' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
	join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 193 > %') mm 
    	on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    join EPROC2G.sessao_julgamento_item SJI on PE.id_processo_evento = SJI.id_processo_evento_resultado 
	join (select distinct id_sessao_julgamento_item from eproc2g.sessao_julgamento_item_MAGISTRADO) SJIM 
		on SJIM.ID_SESSAO_JULGAMENTO_ITEM = SJI.ID_SESSAO_JULGAMENTO_ITEM 
    where sjim.id_sessao_julgamento_item is not null 
), mov_baixado as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null as int) as COD_MAGISTRADO,
		PE.ID_ORGAO_EVENTO,
		null as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'BAIXA' as TP_MOVIMENTO,
		'ITEM_11_4' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
	join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 22 > %') MM
    	on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
), mov_despacho as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null  as int) as COD_MAGISTRADO,
		PE.ID_ORGAO_EVENTO,
		null as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'DESPACHO' as TP_MOVIMENTO,
		'ITEM_12_1' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
    join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento, caminho_hierarquia_txt from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 11009 > %') MM
    	on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
), mov_decisao_monocratica as (
    SELECT 
    	PE.ID_PROCESSO,
    	PE.DTH_EVENTO,
    	PE.ID_PROCESSO_EVENTO,
    	PE.ID_EVENTO_JUDICIAL,
    	EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null as int) as COD_MAGISTRADO,
		PE.ID_ORGAO_EVENTO,
		null as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'DECISAO_MONOCRATICA' as TP_MOVIMENTO,
		'ITEM_12_2' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
    join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
    join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento, caminho_hierarquia_txt 
    	from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 3 > %' and flg_monocratico = 'S') MM
    on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
    --WHERE EJ.COD_MNI_MOVIMENTO IN (972, 12256, 12319)
), mov_voto as (
	SELECT 
		PE.ID_PROCESSO,
		PE.DTH_EVENTO,
		PE.ID_PROCESSO_EVENTO,
		PE.ID_EVENTO_JUDICIAL,
		EJ.COD_EVENTO,
		EJ.DES_EVENTO,
		EJ.COD_MNI_MOVIMENTO,
		cast(null as int) as COD_MAGISTRADO,
		PE.ID_ORGAO_EVENTO,
		null as IND_SESSAO,
		MM.FLG_MONOCRATICO as IND_MONOCRATICO,
		MM.FLG_COLEGIADO as IND_COLEGIADO,
		'VOTO' as TP_MOVIMENTO,
		'ITEM_12_3' as ITEM,
		'EPROC2G' as IND_SISTEMA
    FROM eproc2g.processo_Evento pe 
    join eproc2g.evento_judicial EJ on pe.id_Evento_judicial = ej.id_evento_judicial
        join (select cod_mni_movimento, flg_monocratico, flg_colegiado, nome_movimento, caminho_hierarquia_txt 
    	from dw2inst.tf_arvore_mni_movimento where CONCAT(' > ', caminho_hierarquia_txt, ' > ') LIKE '% > 14092 > %') MM
    on EJ.COD_MNI_MOVIMENTO = MM.COD_MNI_MOVIMENTO
	--WHERE EJ.ID_EVENTO_JUDICIAL = 40400383
), movimentos as (
	select * from mov_distribuido
	union all
	select * from mov_julgado_monocraticamente
	union all
	select * from mov_julgado_colegiado
	union all
	select * from mov_baixado
	union all
	select * from mov_despacho
	union all
	select * from mov_decisao_monocratica
	union all
	select * from mov_voto
)
SELECT  
	CAST(m.ID_PROCESSO AS DECIMAL(30,0))													as ID_PROCESSO,
	m.DTH_EVENTO, 
	CAST(M.ID_PROCESSO_EVENTO AS DECIMAL(30,0))												as ID_PROCESSO_EVENTO,
	case 
		when m.item = 'ITEM_11_1' then CAST(id_orgao_evento AS INT)
		else CAST(OGM.COD_ORGAO AS INT)
	end 																					AS COD_ORGAO_GABINETE,
	case 
		when item = 'ITEM_11_1' then CAST(m.COD_MAGISTRADO AS VARCHAR(255))
		else CAST(ogm.cod_magistrado AS VARCHAR(255)) 
	end 																					AS COD_MAGISTRADO,	
    CAST(m.ID_EVENTO_JUDICIAL AS INT) 														AS ID_EVENTO_JUDICIAL,
    CAST(m.COD_EVENTO AS VARCHAR(255))														AS COD_EVENTO,
    CAST(m.COD_MNI_MOVIMENTO AS INT) 														AS COD_MNI_MOVIMENTO,
    CAST(m.DES_EVENTO AS VARCHAR(255)) 														AS DES_EVENTO,
    CAST(m.IND_SESSAO AS VARCHAR(1)) 														AS IND_SESSAO,
    CAST(M.IND_MONOCRATICO AS VARCHAR(1)) 													AS IND_MONOCRATICO,
	 CAST(M.IND_COLEGIADO AS VARCHAR(1)) 													AS IND_COLEGIADO,
    CAST(M.TP_MOVIMENTO AS VARCHAR(255))													as TP_MOVIMENTO,
    CAST(M.ITEM AS VARCHAR(255))															as ITEM,
    M.IND_SISTEMA,
    current_timestamp as DTH_ETL
FROM movimentos m
left join ORGAO_GABINETE_MAGISTRADO OGM on M.ID_ORGAO_EVENTO = OGM.ID_ORGAO;