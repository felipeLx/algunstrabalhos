WITH seg_usuario AS (
  SELECT DISTINCT
    cpf_usu
  FROM glue_catalog.seg.usuario
  WHERE cargo = 'Magistrado'
),

eproc_magistrado AS (
  SELECT
    mag.id_pessoa,
    mag.cod_magistrado,
    mag.num_antiguidade,
    mag.nome_magistrado,
    mag.dth_inclusao,
    mag.dth_desativacao,
    id.tipo_identificacao,
    id.ident_principal,
    o.id_orgao,
    o.des_orgao,
    CASE
      WHEN UPPER(o.des_orgao) LIKE 'DES%' THEN 2
      WHEN UPPER(o.des_orgao) LIKE 'JDS%' OR mag.cod_magistrado IN (7777874,7777875) THEN 3
      ELSE 0
    END AS codcargo,
    2 AS codjustorig,
    ROW_NUMBER() OVER (
      PARTITION BY mag.id_pessoa, 
                   CASE
                     WHEN UPPER(o.des_orgao) LIKE 'DES%' THEN 2
                     WHEN UPPER(o.des_orgao) LIKE 'JDS%' OR mag.cod_magistrado IN (7777874,7777875) THEN 3
                     ELSE 0
                   END
      ORDER BY mag.dth_inclusao DESC
    ) AS rn
  FROM glue_catalog.eproc2g.magistrado mag
  JOIN glue_catalog.eproc2g.juizo_magistrado jm
    ON jm.cod_magistrado = mag.cod_magistrado
  JOIN glue_catalog.eproc2g.orgao o
    ON o.id_orgao = jm.id_orgao
  JOIN (
    SELECT id_pessoa, tipo_identificacao, ident_principal
    FROM glue_catalog.eproc2g.pessoa_identificacao
    WHERE tipo_identificacao = 'CPF'
  ) id
    ON id.id_pessoa = mag.id_pessoa
  WHERE
    CASE
      WHEN UPPER(o.des_orgao) LIKE 'DES%' THEN 2
      WHEN UPPER(o.des_orgao) LIKE 'JDS%' OR mag.cod_magistrado IN (7777874,7777875) THEN 3
      ELSE 0
    END IN (2,3)
)

SELECT
  eproc.cod_magistrado,
  eproc.nome_magistrado,
  eproc.num_antiguidade,
  eproc.id_pessoa,
  eproc.ident_principal AS cpf,
  eproc.dth_inclusao,
  eproc.dth_desativacao,
  eproc.codcargo,
  eproc.codjustorig,
  eproc.rn
FROM eproc_magistrado eproc
LEFT JOIN seg_usuario su
  ON eproc.ident_principal = su.cpf_usu
WHERE eproc.rn = 1
;

