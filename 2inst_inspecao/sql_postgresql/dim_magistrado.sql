INSERT INTO insp2inst.dim_magistrado_cpf (
	ID_cpf,
	nm_magistrado,
	ind_ejud,
	ind_eproc2g
)
select * from (
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
	SELECT 
         SEG.cpf,
         LTRIM(REPLACE(REPLACE(UPPER(TRIM(P.NOME)),'JDS.',''),'DES.','')) as nome,
         row_number() over (partition by SEG.cpf order by SEG.cpf desc) rn
    FROM EJUD.magistrado m
    JOIN (
      SELECT distinct cod_usu, cpf_usu AS cpf
        FROM SEG.usuario 
       WHERE cargo = 'Magistrado'
    ) seg ON m.id = SEG.cod_usu
    join EJUD.pessoa p on m.codpess = p.codpess
),
UNIR_MAG AS (
  SELECT 
         coalesce(eproc.cpf, EJUD.CPF) AS CPF,
         coalesce(eproc.nome, EJUD.nome) AS NOME,
         CASE WHEN eproc.cpf IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc2g,
         CASE WHEN EJUD.cpf IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud
    FROM (select * from eproc2g_mag_cpf where rn = 1) eproc 
    FULL OUTER JOIN (select * from ejud_mag_cpf where rn = 1) ejud ON eproc.cpf = EJUD.cpf
)
select
	m.cpf as id_cpf,
	m.nome AS nm_magistrado,
	m.ind_ejud,
	m.ind_eproc2g
FROM unir_mag m
union all 
select 
	'0' as ID_CPF,
	'NÃO ENCONTRADO' as nm_magistrado,
	'?' as ind_ejud,
	'?' as ind_eproc2g
) X

ON CONFLICT (ID_CPF)
DO UPDATE SET
    nm_magistrado = EXCLUDED.nm_magistrado,
    ind_ejud = EXCLUDED.ind_ejud,
    ind_eproc2g = EXCLUDED.ind_eproc2g;