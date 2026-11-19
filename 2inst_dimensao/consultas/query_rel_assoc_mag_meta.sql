WITH MAG_EPROC2G AS (
    SELECT
        COD_MAGISTRADO,
        NOME,
        COD_CARGO,
        CPF,
        'COD_MAGISTRADO' AS CAMPO,
        'EPROC2G'        AS SISTEMA
    FROM (
        SELECT DISTINCT
            mag.id_pessoa,
            mag.cod_magistrado,
            CASE
                WHEN upper(om.des_orgao) LIKE '%CORREGEDORIA%'  THEN 1
                WHEN upper(om.des_orgao) LIKE 'DES%'            THEN 2
                WHEN upper(om.des_orgao) LIKE 'JUIZ%'
                  OR upper(om.des_orgao) LIKE 'JUÍZ%'           THEN 3
                WHEN upper(om.des_orgao) LIKE '%PRESIDÊNCIA%'   THEN 4
                WHEN upper(om.des_orgao) LIKE '1ª VICE%'        THEN 5
                WHEN upper(om.des_orgao) LIKE '2ª VICE%'        THEN 6
                WHEN upper(om.des_orgao) LIKE '3ª VICE%'        THEN 7
                WHEN upper(om.des_orgao) LIKE 'JDS%'            THEN 9
                WHEN upper(om.des_orgao) LIKE 'PRESIDENTE%'     THEN 10
                ELSE 0
            END AS COD_CARGO,
            mag.nome_magistrado AS NOME,
            ident.ident_principal AS CPF
        FROM eproc2g.magistrado mag
        JOIN (
            SELECT jm.cod_magistrado, jm.id_orgao, o.des_orgao, smj.*
            FROM eproc2g.juizo_magistrado jm
            JOIN eproc2g.orgao o
              ON o.id_orgao = jm.id_orgao
            JOIN eproc2g.situacao_magistrado_juizo smj
              ON jm.cod_situacao_magistrado_juizo = smj.cod_situacao_magistrado_juizo
            WHERE smj.cod_situacao_magistrado_juizo = 1
        ) om ON mag.cod_magistrado = om.cod_magistrado
        JOIN (
            SELECT id_pessoa, tipo_identificacao, ident_principal
            FROM eproc2g.pessoa_identificacao
            WHERE tipo_identificacao = 'CPF'
        ) ident ON mag.id_pessoa = ident.id_pessoa
    ) x
),
MAG_EJUD AS (
    SELECT DISTINCT
        lm.codmag AS COD_MAGISTRADO,
        p.nome    AS NOME,
        CASE
            WHEN lm.codcargo = 2 AND lm.codjustorig = 2 THEN 2
            WHEN lm.codcargo = 3 AND lm.codjustorig = 2 THEN 9
            ELSE lm.codcargo
        END AS COD_CARGO,
        CASE
            WHEN sm.cpf IS NULL THEN '0'
            ELSE sm.cpf
        END AS CPF,
        'COD_MAG' AS CAMPO,
        'EJUD'    AS SISTEMA
    FROM (
        SELECT DISTINCT codmag, codcargo, codjustorig
        FROM (
            SELECT DISTINCT codmag, codcargo, codjustorig
            FROM ejud.logmagistrado
            WHERE codcargo IS NOT NULL
              AND codjustorig IS NOT NULL
              AND CAST(codjustorig AS INT) IN (1, 2)
            UNION ALL
            SELECT DISTINCT codmag, codcargo, codjustorig
            FROM ejud.magistrado
            WHERE CAST(codjustorig AS INT) IN (1, 2)
        ) x
        WHERE codcargo IS NOT NULL
          AND codjustorig IS NOT NULL
          AND NOT (codcargo = 2 AND codjustorig = 1)
    ) lm
    LEFT JOIN (
        SELECT codmag, id, codpess FROM ejud.magistrado
    ) m ON lm.codmag = m.codmag
    LEFT JOIN (
        SELECT codpess,
               LTRIM(REPLACE(REPLACE(UPPER(TRIM(nome)), 'JDS.', ''), 'DES.', '')) AS nome
        FROM ejud.pessoa
    ) p ON m.codpess = p.codpess
    LEFT JOIN (
        SELECT codmag,
               CASE WHEN cpf IS NULL THEN '0' ELSE cpf END AS cpf
        FROM dim_2inst_ouro.tb_dim_magistrado_ejud
    ) sm ON m.codmag = sm.codmag
)
SELECT
    CAST(COD_MAGISTRADO AS INT) AS COD_MAGISTRADO,
    NOME,
    CAST(COD_CARGO AS INT)      AS COD_CARGO,
    CPF,
    CAMPO,
    SISTEMA
FROM MAG_EPROC2G
UNION ALL
SELECT
    CAST(COD_MAGISTRADO AS INT) AS COD_MAGISTRADO,
    NOME,
    CAST(COD_CARGO AS INT)      AS COD_CARGO,
    CASE WHEN CPF = '' THEN '0' ELSE CPF END AS CPF,
    CAMPO,
    SISTEMA
FROM MAG_EJUD
