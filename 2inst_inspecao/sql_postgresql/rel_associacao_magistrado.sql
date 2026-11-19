INSERT INTO insp2inst.rel_associacao_magistrado (
	COD_MAGISTRADO,
	nm_magistrado_sistema,
	id_cpf,
	ind_sistema
)

select 
    COD_MAGISTRADO,
    nm_magistrado_sistema,
    id_cpf,
    ind_sistema
from (
with MAG_EPROC2G as (
	select
		COD_MAGISTRADO,
		NOME,
		CPF,
		'COD_MAGISTRADO' as CAMPO,
		'EPROC2G' as SISTEMA
	from (
	select 
		distinct 
		mag.id_pessoa,
	 	mag.cod_magistrado,
	 	mag.nome_magistrado AS NOME,
	 	ident.ident_principal as CPF
	from eproc2g.magistrado mag
	join (select jm.cod_magistrado, jm.id_orgao, o.des_orgao, smj.* from eproc2g.juizo_magistrado jm   
		join eproc2g.orgao o on o.id_orgao = jm.id_orgao
		join eproc2g.situacao_magistrado_juizo smj on jm.cod_situacao_magistrado_juizo = smj.cod_situacao_magistrado_juizo
		where 1=1 and smj.cod_situacao_magistrado_juizo = 1 order by jm.cod_magistrado asc) om 
	on mag.cod_magistrado = om.cod_magistrado
	join (select id_pessoa, tipo_identificacao, ident_principal from eproc2g.pessoa_identificacao where tipo_identificacao = 'CPF') ident
	on mag.id_pessoa = ident.id_pessoa) X
), MAG_EJUD as (
	select  
		M.CODMAG as COD_MAGISTRADO, 
		P.NOME AS NOME,
		case 
			when sm.cpf is not null then sm.cpf
			else '0'
		end as cpf,
		'COD_MAG' as CAMPO,
		'EJUD' as sistema
	--traz todas as combinacoes de codmag, codcargo e codjustorig que um magistrado ja teve
	from (select codmag, id, codpess from ejud.magistrado) m
	--traz o nome da pessoa
	left join (select codpess, LTRIM(REPLACE(REPLACE(UPPER(TRIM(NOME)),'JDS.',''),'DES.','')) AS nome from ejud.pessoa) p on m.codpess = p.codpess
	--traz o CPF
	left join (select cod_usu, cargo, cpf_usu as cpf from seg.usuario where cargo = 'Magistrado') sm on m.id = sm.cod_usu
)
SELECT 
    CAST(COD_MAGISTRADO AS INT) AS COD_MAGISTRADO,
    NOME as nm_magistrado_sistema,
    CPF as id_cpf,
    SISTEMA as IND_SISTEMA
FROM MAG_EPROC2G
UNION ALL
SELECT 
    CAST(COD_MAGISTRADO AS INT) AS COD_MAGISTRADO,
    NOME as nm_magistrado_sistema,
    CPF as id_cpf,
    SISTEMA as IND_SISTEMA
FROM MAG_EJUD
) x

ON CONFLICT (COD_MAGISTRADO, IND_SISTEMA)
DO UPDATE SET
    nm_magistrado_sistema = EXCLUDED.nm_magistrado_sistema,
    id_cpf = EXCLUDED.id_cpf;