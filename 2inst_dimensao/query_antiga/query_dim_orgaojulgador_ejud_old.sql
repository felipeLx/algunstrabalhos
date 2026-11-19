WITH orgao_julgador AS (
    SELECT 
        oj.codorgjulg, 
        oj.codlocal,
        oj.descrsint, 
        oj.codpess, 
        oj.codorgjulgcnj,
        p.nome,
        oj.codcompt, 
        oj.nomebnmp, 
        oj.linkbalcaovirtual,
        oj.numordinal,
        oj.codmagpres,
        oj.codgraujuris,
        oj.idususecr,
        ojc.CODMAGPRESEXERC
    FROM ejud.orgaojulgador oj
    JOIN ejud.pessoa p ON CAST(oj.codpess AS BIGINT) = CAST(p.codpess AS BIGINT)
    LEFT JOIN ejud.orgaojulgadorconfig ojc 
           ON CAST(oj.codorgjulg AS BIGINT) = CAST(ojc.codorgjulg AS BIGINT)
),
de_para AS (
    SELECT 
        CAST(codorgjulgciv   AS BIGINT) AS codorgjulg, 
        CAST(codorgjulgespec AS BIGINT) AS de_para, 
        CAST(codcomptespec   AS BIGINT) AS codcompt 
    FROM ejud.camaracivelespecializada
),
local_fisico AS (
    SELECT 
        CAST(codlocal AS BIGINT) AS codlocal, 
        CAST(codorgjulg AS BIGINT) AS codorgjulg, 
        CAST(codpess AS BIGINT) AS codpess, 
        descr AS localfisico 
    FROM ejud.localfisico
),
pessoa_endereco_telefone AS (
    SELECT CAST(pe.codpess AS BIGINT) AS codpess, pe.codpessend, pec.descr
    FROM ejud.pessoaendereco pe
    JOIN ejud.pessoaenderecocontato pec 
      ON pe.codpessend = pec.codpessend
    WHERE pec.codtipcont = 1
),
pessoa_endereco_email AS (
    SELECT CAST(pe.codpess AS BIGINT) AS codpess, pe.codpessend, pec.descr
    FROM ejud.pessoaendereco pe
    JOIN ejud.pessoaenderecocontato pec 
      ON pe.codpessend = pec.codpessend
    WHERE pec.codtipcont = 3
),
pessoa_endereco AS (
    SELECT CAST(codpess AS BIGINT) AS codpess, nomelogr 
    FROM ejud.pessoaendereco
),
atividade AS (
    SELECT dt_instalacao_ejud, codpess
    FROM (
        SELECT 
            CAST(codpess AS BIGINT) AS codpess,
            CAST(dthralt AS TIMESTAMP) AS dt_instalacao_ejud,
            ROW_NUMBER() OVER (PARTITION BY codpess ORDER BY dthralt ASC) AS rn 
        FROM ejud.logpessoa 
        WHERE CAST(codpess AS BIGINT) IN (SELECT CAST(codpess AS BIGINT) FROM ejud.orgaojulgador)
    ) x 
    WHERE rn = 1
),
scol_orgao AS (
    SELECT 
        CAST(cod_org AS BIGINT) AS cod_org, 
        CAST(cod_org_cnj AS BIGINT) AS cod_org_cnj, 
        dt_registro, dt_inst, dt_inativ, ativo 
    FROM scol.orgao
)
SELECT 
  CAST(codorgjulg        AS BIGINT)   AS codorgjulg,
  CAST(de_para           AS BIGINT)   AS de_para,
  CAST(nome              AS STRING)   AS nome,
  CAST(nome_sint         AS STRING)   AS nome_sint,
  CAST(nome_bnmp         AS STRING)   AS nome_bnmp,
  CAST(CAST(cod_grau_jurisdicao AS DOUBLE) AS INT)    AS cod_grau_jurisdicao,
  CAST(codorgjulg_cnj    AS BIGINT)   AS codorgjulg_cnj,
  CAST(codcompt          AS BIGINT)   AS codcompt,
  CAST(codlocal          AS BIGINT)   AS codlocal,
  CAST(codmag_pres       AS BIGINT)   AS codmag_pres,
  CAST(idususecr         AS STRING)   AS idususecr,
  CAST(codmag_presexerc  AS BIGINT)   AS codmag_presexerc,
  CAST(balcao_virtual    AS STRING)   AS balcao_virtual,
  CAST(logradouro        AS STRING)   AS logradouro,
  CAST(telefone          AS STRING)   AS telefone,
  CAST(email             AS STRING)   AS email,
  CAST(dt_instalacao_ejud AS TIMESTAMP) AS dt_instalacao_ejud,
  CAST(dt_registro        AS TIMESTAMP) AS dt_registro,
  CAST(dt_instalacao      AS TIMESTAMP) AS dt_instalacao,
  CAST(dt_desativacao     AS TIMESTAMP) AS dt_desativacao,
  CAST(ativo             AS STRING)   AS ativo,
  'EJUD' AS sistema
FROM (
    SELECT 
           oj.codorgjulg,
           COALESCE(dp.de_para, oj.codorgjulg) AS de_para,
           oj.nome,
           oj.descrsint AS nome_sint,
           oj.nomebnmp AS nome_bnmp,
           oj.codgraujuris AS cod_grau_jurisdicao,
           COALESCE(so.cod_org_cnj, oj.codorgjulgcnj) AS codorgjulg_cnj,
           COALESCE(dp.codcompt, oj.codcompt) AS codcompt,
           oj.codlocal,
           oj.codmagpres AS codmag_pres,
           oj.idususecr,
           oj.CODMAGPRESEXERC AS codmag_presexerc,
           oj.linkbalcaovirtual AS balcao_virtual,
           pe.nomelogr AS logradouro,
           pet.descr AS telefone,
           pee.descr AS email,
           a.dt_instalacao_ejud, 
           so.dt_registro,
           so.dt_inst       AS dt_instalacao,
           so.dt_inativ     AS dt_desativacao,
           so.ativo,
           ROW_NUMBER() OVER (ORDER BY oj.codcompt ASC, oj.numordinal ASC, oj.codpess ASC) AS ordem
    FROM orgao_julgador oj
    LEFT JOIN de_para dp
           ON oj.codorgjulg = dp.codorgjulg
    /* trocado para LEFT JOIN para não anular tudo se faltar codlocal correspondente */
    LEFT JOIN local_fisico lf
           ON CAST(oj.codlocal AS BIGINT) = lf.codlocal
    LEFT JOIN pessoa_endereco_telefone pet 
           ON CAST(oj.codpess AS BIGINT) = pet.codpess
    LEFT JOIN pessoa_endereco_email   pee 
           ON CAST(oj.codpess AS BIGINT) = pee.codpess
    LEFT JOIN pessoa_endereco         pe  
           ON CAST(oj.codpess AS BIGINT) = pe.codpess
    LEFT JOIN atividade a             
           ON CAST(oj.codpess AS BIGINT) = a.codpess
    LEFT JOIN scol_orgao so           
           ON CAST(oj.codorgjulg AS BIGINT) = so.cod_org
) ejud
;

