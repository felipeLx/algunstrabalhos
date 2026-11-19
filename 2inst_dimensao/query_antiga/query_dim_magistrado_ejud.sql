WITH eventos AS (
  SELECT
      CAST(lp.dthralt AS STRING) AS d,
      lp.codpess,
      m.codmag,
      lp.indativo                 AS i
  FROM ejud.magistrado m
  JOIN ejud.logpessoa lp ON lp.codpess = m.codpess
  WHERE m.codjustorig = 2
),

-- 1) estado atual: Ãºltimo indativo do magistrado
estado_atual AS (
  SELECT codmag, i AS ind_atual
  FROM (
    SELECT
      codmag, i, d,
      ROW_NUMBER() OVER (PARTITION BY codmag ORDER BY d DESC) AS rn
    FROM eventos
  ) t
  WHERE rn = 1
),

-- 2) detectar transiÃ§Ãµes Sâ†’N via LAG
trans AS (
  SELECT
    codmag, d, i,
    LAG(i) OVER (PARTITION BY codmag ORDER BY d) AS prev_i
  FROM eventos
),

-- 3) maior data em que houve Sâ†’N
max_s2n AS (
  SELECT codmag, MAX(d) AS d_s2n
  FROM trans
  WHERE i = 'N' AND prev_i = 'S'
  GROUP BY codmag
),

-- 4) data final: somente se estado atual = 'N'
ultima_inatividade AS (
  SELECT
    e.codmag,
    CASE WHEN ea.ind_atual = 'N' THEN
      COALESCE(
        to_timestamp(ms.d_s2n, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
        to_timestamp(ms.d_s2n, 'yyyy-MM-dd HH:mm:ss.SSS'),
        to_timestamp(ms.d_s2n, 'yyyy-MM-dd HH:mm:ss')
      )
    END AS data_de_inatividade
  FROM (SELECT DISTINCT codmag FROM eventos) e
  LEFT JOIN estado_atual ea ON e.codmag = ea.codmag
  LEFT JOIN max_s2n      ms ON e.codmag = ms.codmag
),

-- sua base (inalterada)
magistrado_base AS (
  SELECT
      m.codmag,
      p.nome,
      m.numantig,
      m.dtassun,
      m.codjustorig,
      TRIM(CAST(m.id AS STRING)) AS usuario,
      m.codmagcnj,
      m.indquint AS ind_quinto,
      TRIM(CAST(m.descrmatr AS STRING)) AS descrmatr,
      CASE
        WHEN UPPER(p.nome) LIKE 'DES%' THEN 2
        WHEN UPPER(p.nome) LIKE 'JDS%' THEN 3
        ELSE 0
      END AS codcargo
  FROM ejud.magistrado m
  LEFT JOIN ejud.magistradogtda mg ON m.codmag = mg.codmag
  LEFT JOIN ejud.pessoa p          ON m.codpess= p.codpess
  WHERE m.codjustorig = 2
),

localfisico_filtered AS (
  SELECT CAST(codlocal AS BIGINT) AS codlocal,
         descr AS localfisico,
         CAST(codmag  AS BIGINT)  AS codmag
  FROM ejud.localfisico
  WHERE codlocal NOT IN (3142, 3144, 3148)
),

scol_orgao AS (
  SELECT CAST(cod_org AS BIGINT) AS cod_org,
         nome_dtl AS nome, dt_inativ, sigla,
         CAST(cod_org_cnj AS BIGINT) AS cod_org_cnj
  FROM scol.orgao
),

seg_usuario AS (
  SELECT
    TRIM(CAST(matr_usu AS STRING)) AS matr_usu,
    TRIM(CAST(cod_usu  AS STRING)) AS cod_usu,
    TRIM(nome_usu)                  AS nome_usu,
    cpf_usu AS cpf_usu
  FROM seg_usuario
  WHERE cargo = 'Magistrado'
),

magistrado_sanitizado AS (
  SELECT cpf,
         nome,
	nome_ejud
  FROM dw2inst.magistrados_cpf_sanitizado
),

funcionario AS (
  SELECT
    TRIM(CAST(matricula AS STRING)) AS matricula_func,
    TRIM(nome)                  AS nome_func,
    cpf AS cpf_func
  FROM rh.funcionario
)

SELECT
  m.codmag,
  COALESCE(NULLIF(ms.nome, ''),NULLIF(f.nome_func, ''),NULLIF(su.nome_usu, ''),NULLIF(u.nome_usu, ''),NULLIF(TRIM(m.nome), ''), '') AS nome,
  CASE WHEN ui.data_de_inatividade IS NOT NULL THEN 'Inativo' ELSE 'Ativo' END AS situacao,
  ui.data_de_inatividade,
  m.numantig,
  m.dtassun,
  m.codcargo,
  m.codjustorig,
  m.usuario,
  COALESCE(lf.codlocal, CAST(0 AS BIGINT)) AS codlocal,
  lf.localfisico,
  m.ind_quinto,
  so.cod_org_cnj,
  COALESCE(NULLIF(ms.cpf,''),NULLIF(BTRIM(f.cpf_func),''),NULLIF(BTRIM(su.cpf_usu), ''),NULLIF(BTRIM(u.cpf_usu), ''), '') AS cpf,
  m.codmagcnj
FROM magistrado_base m
LEFT JOIN magistrado_sanitizado ms ON m.nome = ms.nome_ejud
LEFT JOIN ultima_inatividade  ui ON m.codmag = ui.codmag
LEFT JOIN localfisico_filtered lf ON CAST(m.codmag AS BIGINT) = lf.codmag
LEFT JOIN scol_orgao          so ON so.cod_org  = lf.codlocal
LEFT JOIN funcionario          f ON m.descrmatr = f.matricula_func
LEFT JOIN seg_usuario         su ON m.descrmatr = su.matr_usu
left join seg_usuario          u on m.usuario   = u.cod_usu
WHERE m.codcargo IN (2,3)
ORDER BY m.codmag;

