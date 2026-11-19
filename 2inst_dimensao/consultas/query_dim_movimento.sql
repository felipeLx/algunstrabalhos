with ejud_movimento as (
select
	f.codfase as cod_movimento
	,'CODFASE' as nm_coluna
	,UPPER(f.descr) as nm_movimento
	,cast(fe.codfaseext as varchar(255)) as cod_movimento_sistema
	,'CODFASEEXT' as nm_coluna_sistema
	,'EJUD' as ind_sistema
	,case 
		when F.CODFASE in (67,118) 
		then 'N'
		else 'S'
	end IND_MOVIMENTO_CNJ
    ,CAST(NULL AS VARCHAR(1)) AS IND_REATIVACAO
from
	ejud.fase f
left join ejud.faseexterna fe on
	f.codfase = fe.codfase
), eproc2g_movimento as (
select
	id_evento_judicial as cod_movimento
	,'ID_EVENTO_JUDICIAL' as nm_coluna
	,UPPER(des_evento) as nm_movimento
	,cast(COD_EVENTO as varchar(255)) as cod_movimento_sistema
	,'COD_EVENTO' as nm_coluna_sistema
	,'EPROC2G' as ind_sistema
	,case 
		when CONCAT(' > ', arv.caminho_hierarquia_txt, ' > ') LIKE '% > 67 > %' then 'N'
		else 'S'
	end IND_MOVIMENTO_CNJ
	,case 
		when CONCAT(' > ', arv.caminho_hierarquia_txt, ' > ') LIKE '% > 849 > %' then 'S'
		else 'N'
	end IND_REATIVACAO
from
	eproc2g.evento_judicial ej
left join dw_2inst_ouro.tb_ft_arvore_mni_movimento arv on ej.cod_mni_movimento = arv.cod_mni_movimento
), dim_movimento as (
select * from ejud_movimento
union all
select * from eproc2g_movimento
) select m.*, cast(current_timestamp as timestamp) as DTH_ETL from dim_movimento m;
