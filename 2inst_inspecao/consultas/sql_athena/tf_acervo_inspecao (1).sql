WITH item_10_1 AS (
	--Total de processos em tramitação na unidade, incluindo suspensos, sobrestados e arquivados provisoriamente (acervo total)
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    1 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
), item_10_2 AS (
	--Total de processos suspensos, sobrestados ou arquivados provisoriamente
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    2 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_SUSPENSO = 'S'
), item_10_3 AS (
	--Total de processos em tramitação, excluídos suspensos, sobrestados ou arquivados provisoriamente (acervo ajustado)
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    3 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_SUSPENSO = 'N'
), item_10_4 AS (
	--Total de processos originários em tramitação sem decisão de mérito
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    4 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_SUSPENSO = 'N' AND A.IND_JULGAMENTO = 'N' AND P.IND_ORIGINARIO = 'S'
), item_10_5 AS (
	--Total de recursos em tramitação sem decisão de mérito
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    5 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_SUSPENSO = 'N' AND A.IND_JULGAMENTO = 'N' AND P.IND_ORIGINARIO = 'N'		
), item_10_6 AS (
--	Total de processos conclusos para o magistrado
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    6 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_CONCLUSAO = 'S' 
), item_10_7 AS (
	--	Total de processos conclusos para o magistrado há mais de 100 dias
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    7 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where A.IND_CONCLUSAO = 'S' and A.DTH_ULTIMO_MOVIMENTO < current_timestamp - INTERVAL '100' DAY
), item_10_8 AS (
--	Total de processos em secretaria
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    8 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where UPPER(dlf.localfisico) like '%SEC%'
), item_10_9 AS (
--	Total de processos paralisados em secretaria há mais de 100 dias
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    9 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	and A.DTH_ULTIMO_MOVIMENTO < current_timestamp - INTERVAL '100' day and UPPER(dlf.localfisico) like '%SEC%'
), item_10_10 AS (
--	Total de processos em diligência na origem
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    8 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
	where dle.indlocalext = 'S'	
), item_10_12 AS (
--	Total de processos com prioridade legal conclusos há mais de 100 dias
	SELECT 
		A.DTH_REFERENCIA,
	    a.COD_DOCUMENTO, 
	    p.COD_CNJ,
	    p.NM_CLASSE,
	    A.DTH_ULTIMO_MOVIMENTO,
	    A.COD_LOCAL_ULTIMO,
	    dlf.localfisico as local,
	    dm.CPF, 
	    10 as ITEM,
	    12 as SUBITEM,
	    'DADOS PROCESSUAIS' as TP_PERGUNTA,
	    a.ind_sistema
	FROM dw2inst.tb_acervo_indicadores a
	left join dw2inst.rel_associacao_magistrado ram on a.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
	left join dim2inst.dim_mag_cpf dm on ram.cpf = dm.cpf
	left join dim2inst.dim_processo p on a.cod_documento = p.cod_documento and a.ind_sistema = p.ind_sistema
	left join dw2inst.tb_associacao_localfisico rlf on A.COD_LOCAL_ULTIMO = rlf.codlocalsistema and a.ind_sistema = rlf.sistema
	left join dim2inst.dim_localfisico_parametrizado dlf on rlf.codlocalparametrizado = dlf.codlocal
	left join dim2inst.dim_localfisico_ejud dle on dlf.codlocal = dle.cod_local
), item_10 as (
	select * from item_10_1
	union all
	select * from item_10_2
	union all
	select * from item_10_3
	union all
	select * from item_10_4
	union all
	select * from item_10_5
	union all
	select * from item_10_6
	union all
	select * from item_10_7
	union all
	select * from item_10_8
	union all
	select * from item_10_9
	union all
	select * from item_10_10
	union all
	select * from item_10_12
)
SELECT  
	*	
from item_10;