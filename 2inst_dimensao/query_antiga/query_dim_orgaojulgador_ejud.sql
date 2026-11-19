WITH orgao_julgador AS (
    SELECT
        oj.codorgjulg,
        oj.codorgjulgcnj,
        p.nome,
        oj.nomebnmp,
        oj.descrsint,
        oj.codlocal,
        oj.codcompt,
        C.DESCR as COMPETENCIA,
        oj.linkbalcaovirtual,
        oj.codgraujuris,
        oj.codmagpres,
        oj.idususecr,
        ojc.CODMAGPRESEXERC,
        oj.numordinal,
        oj.codpess
    FROM ejud_orgaojulgador oj
    JOIN ejud_pessoa p ON CAST(oj.codpess AS BIGINT) = CAST(p.codpess AS BIGINT)
    LEFT JOIN ejud_orgaojulgadorconfig ojc ON CAST(oj.codorgjulg AS BIGINT) = CAST(ojc.codorgjulg AS BIGINT)
    left join EJUD_COMPETENCIA C on cast(OJ.CODCOMPT as BIGINT) = CAST(C.CODCOMPT as BIGINT)
), de_para AS (
    SELECT
        CAST(codorgjulgciv   AS BIGINT) AS codorgjulg,
        CAST(codorgjulgespec AS BIGINT) AS de_para,
        CAST(codcomptespec   AS BIGINT) AS codcompt
    FROM ejud_camaracivelespecializada
),
local_fisico AS (
    SELECT
        CAST(codlocal AS BIGINT) AS codlocal,
        CAST(codorgjulg AS BIGINT) AS codorgjulg,
        CAST(codpess AS BIGINT) AS codpess,
        descr AS localfisico
    FROM ejud_localfisico
),
pessoa_endereco_telefone AS (
    SELECT CAST(pe.codpess AS BIGINT) AS codpess, pe.codpessend, pec.descr
    FROM ejud_pessoaendereco pe
    JOIN ejud_pessoaenderecocontato pec
      ON pe.codpessend = pec.codpessend
    WHERE pec.codtipcont = 1
),
pessoa_endereco_email AS (
    SELECT CAST(pe.codpess AS BIGINT) AS codpess, pe.codpessend, pec.descr
    FROM ejud_pessoaendereco pe
    JOIN ejud_pessoaenderecocontato pec
      ON pe.codpessend = pec.codpessend
    WHERE pec.codtipcont = 3
),
pessoa_endereco AS (
    SELECT CAST(codpess AS BIGINT) AS codpess, nomelogr
    FROM ejud_pessoaendereco
),
atividade AS (
    SELECT dt_instalacao_ejud, codpess
    FROM (
        SELECT
            CAST(codpess AS BIGINT) AS codpess,
            CAST(dthralt AS TIMESTAMP) AS dt_instalacao_ejud,
            ROW_NUMBER() OVER (PARTITION BY codpess ORDER BY dthralt ASC) AS rn
        FROM ejud_logpessoa
        WHERE CAST(codpess AS BIGINT) IN (SELECT CAST(codpess AS BIGINT) FROM ejud.orgaojulgador)
    ) x
    WHERE rn = 1
)
SELECT
       CAST(OJ.codorgjulg AS INT),
       CAST(oj.codorgjulgcnj AS INT),
       cast(COALESCE(dp.de_para, oj.codorgjulg) as INT) AS de_para,
       cast(oj.nome as STRING),
       cast(oj.descrsint as STRING),
       cast(oj.nomebnmp as STRING),
       cast(oj.codlocal as INT),
       cast(lf.localfisico as STRING),
       cast(oj.codcompt as INT),
       cast(oj.competencia as STRING),
       cast(oj.codmagpres as INT) AS codmag_pres,
       cast(oj.idususecr as STRING),
       cast(oj.CODMAGPRESEXERC as INT),
       cast(oj.linkbalcaovirtual as STRING),
       cast(pe.nomelogr as STRING) AS logradouro,
       cast(pet.descr as STRING) AS telefone,
       cast(pee.descr as STRING) AS email,
       cast(a.dt_instalacao_ejud as TIMESTAMP),
       cast(oj.codgraujuris as INT),
       cast(ROW_NUMBER() OVER (ORDER BY oj.codcompt ASC, oj.numordinal ASC, oj.codpess ASC) as INT) AS ordem
FROM orgao_julgador oj
LEFT JOIN de_para dp                          ON oj.codorgjulg = dp.codorgjulg
LEFT JOIN local_fisico lf                     ON CAST(oj.codlocal AS BIGINT) = lf.codlocal
LEFT JOIN pessoa_endereco_telefone pet        ON CAST(oj.codpess AS BIGINT) = pet.codpess
LEFT JOIN pessoa_endereco_email   pee         ON CAST(oj.codpess AS BIGINT) = pee.codpess
LEFT JOIN pessoa_endereco         pe          ON CAST(oj.codpess AS BIGINT) = pe.codpess
LEFT JOIN atividade a                         ON CAST(oj.codpess AS BIGINT) = a.codpess
