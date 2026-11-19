
WITH
-- EJUD normalizado
ejud AS (
  SELECT
    codmag                                  AS codmag_ejud,
    nome                                    AS nome_ejud,
    data_de_inatividade,
    situacao,
    numantig                                AS numantigo,
    dtassun                                 AS dataassuncao,
    codcargo                                AS codcargo_ejud,
    codjustorig                             AS codjustorig_ejud,
    usuario,
    codlocal                                AS cod_local,
    localfisico,
    ind_quinto,
    cod_org_cnj,
    cpf                                     AS cpf_ejud,
    codmagcnj,

    -- CPF normalizado (só dígitos, 11 chars, com zero à esquerda se precisar)
    CASE
      WHEN cpf IS NULL OR length(trim(cpf)) = 0 THEN NULL
      ELSE lpad(regexp_replace(trim(cpf), '[^0-9]', ''), 11, '0')
    END AS cpf_norm,

    CASE WHEN codcargo    = 0 THEN NULL ELSE CAST(codcargo    AS INT) END AS cargo_norm,
    CASE WHEN codjustorig = 0 THEN NULL ELSE CAST(codjustorig AS INT) END AS just_norm
  FROM dim_2inst_ouro.tb_dim_magistrado_ejud as ejud_src
),

-- EPROC normalizado
eproc AS (
  SELECT
    cod_magistrado                          AS codmag_eproc,
    nome_magistrado                         AS nome_eproc,
    codcargo                                AS codcargo_eproc,
    codjustorig                             AS codjustorig_eproc,
    cpf                                     AS cpf_eproc,

    CASE
      WHEN cpf IS NULL OR length(trim(cpf)) = 0 THEN NULL
      ELSE lpad(regexp_replace(trim(cpf), '[^0-9]', ''), 11, '0')
    END AS cpf_norm,

    CASE WHEN codcargo    = 0 THEN NULL ELSE CAST(codcargo    AS INT) END AS cargo_norm,
    CASE WHEN codjustorig = 0 THEN NULL ELSE CAST(codjustorig AS INT) END AS just_norm
  FROM dim_2inst_ouro.tb_dim_magistrado_eproc as eproc_src
)

SELECT
  a.codmag_ejud,
  a.nome_ejud,
  b.codmag_eproc,
  b.nome_eproc,
  a.data_de_inatividade,
  a.situacao,
  a.numantigo,
  a.dataassuncao,
  COALESCE(a.codcargo_ejud,    b.codcargo_eproc)    AS codcargo,
  COALESCE(a.codjustorig_ejud, b.codjustorig_eproc) AS codjustorig,
  a.usuario,
  a.cod_local,
  a.localfisico,
  a.ind_quinto,
  a.cod_org_cnj,
  COALESCE(a.cpf_ejud, b.cpf_eproc)                 AS cpf,
  a.codmagcnj
FROM ejud a
LEFT JOIN eproc b
  ON a.cpf_norm = b.cpf_norm
 AND COALESCE(a.cargo_norm, -1) = COALESCE(b.cargo_norm, -1)
 AND COALESCE(a.just_norm,  -1) = COALESCE(b.just_norm,  -1)
;