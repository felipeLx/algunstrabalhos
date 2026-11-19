WITH EPROC2G_MAG_CPF AS (
  SELECT 
        pi.cpf,
        m.nome_magistrado as nome,
		row_number() over (partition by pi.cpf order by pi.cpf desc) rn
    FROM EPROC2G.magistrado m
    JOIN (
      SELECT id_pessoa, ident_principal AS cpf
        FROM EPROC2G.pessoa_identificacao
       WHERE tipo_identificacao = 'CPF'
    ) pi ON m.id_pessoa = pi.id_pessoa
),
EJUD_MAG_CPF AS (
	SELECT distinct cpf, nome FROM dim_2inst_ouro.tb_dim_magistrado_ejud
),
UNIR_MAG AS (
  SELECT 
         coalesce(eproc.cpf, EJUD.CPF) AS CPF,
         coalesce(eproc.nome, EJUD.nome) AS NOME,
         CASE WHEN eproc.cpf IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc2g,
         CASE WHEN EJUD.cpf IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud
    FROM (select * from eproc2g_mag_cpf where rn = 1) eproc 
    FULL OUTER JOIN (select * from ejud_mag_cpf) ejud ON eproc.cpf = EJUD.cpf
)
select
	m.cpf,
	m.nome,
	m.ind_ejud,
	m.ind_eproc2g,
	current_timestamp as dth_etl
FROM unir_mag m
union all 
select 
	'0' as CPF,
	'NÃO ENCONTRADO' as nome,
	'N' as ind_ejud,
	'N' as ind_eproc2g,
	current_timestamp as dth_etl