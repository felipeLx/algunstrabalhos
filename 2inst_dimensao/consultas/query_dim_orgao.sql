WITH orgao_ejud AS (
    SELECT 
        oj.codorgjulg AS codorg,
        CASE 
            WHEN mr.codorgjulg IS NOT NULL THEN 'S' ELSE 'N'
        END AS ind_distr,
        oj.codorgjulgcnj AS codorgcnj
    FROM ejud.orgaojulgador oj
    LEFT JOIN ejud.camaracivelespecializada c ON oj.codorgjulg = c.codorgjulgciv
    JOIN scol.orgao so ON oj.codorgjulg = so.cod_org 
    LEFT JOIN (SELECT DISTINCT codorgjulg FROM ejud.movimentorelator) mr ON oj.codorgjulg = mr.codorgjulg
), 
ORGAO_EPROC AS (
    SELECT 
        id_orgao AS codorg,
        CASE 
            WHEN hdj.id_orgao_colegiado IS NOT NULL THEN 'S' ELSE 'N'
        END AS ind_distr,
        eo.id_orgao_cnj AS codorgcnj
    FROM eproc2g.orgao eo
    JOIN scol.orgao so ON eo.id_orgao = so.cod_org
    LEFT JOIN (
        SELECT DISTINCT id_orgao_colegiado FROM (
            SELECT DISTINCT id_orgao_colegiado FROM eproc2g.log_processo WHERE id_orgao_colegiado IS NOT NULL 
            UNION ALL
            SELECT DISTINCT id_orgao_colegiado FROM eproc2g.processo WHERE id_orgao_colegiado IS NOT NULL 
        ) x
    ) hdj ON hdj.id_orgao_colegiado = eo.id_orgao
), 
ORGAO_UNIDOS AS (
    SELECT DISTINCT
        COALESCE(ejud.codorg, eproc.codorg) AS codorg,
        COALESCE(ejud.codorgcnj, eproc.codorgcnj) AS codorgcnj,
        CASE 
            WHEN ejud.ind_distr = 'S' OR eproc.ind_distr = 'S' THEN 'S' 
            ELSE 'N' 
        END AS ind_distr,
        CASE 
            WHEN ejud.codorg IS NOT NULL THEN 'S' ELSE 'N' 
        END AS ind_ejud,
        CASE 
            WHEN eproc.codorg IS NOT NULL THEN 'S' ELSE 'N' 
        END AS ind_eproc
    FROM orgao_ejud ejud
    FULL OUTER JOIN orgao_eproc eproc ON ejud.codorg = eproc.codorg
), 
ORGAO_SCOL AS (
    SELECT 
        SC.cod_org, 
        COALESCE(SC.cod_org_cnj, ou.codorgcnj) AS codorgcnj, 
        SC.nome, 
        SC.nome_shs,
        OU.ind_ejud,
        OU.ind_eproc,
        OU.IND_DISTR
    FROM SCOL.orgao SC
    JOIN ORGAO_UNIDOS OU ON OU.codorg = SC.cod_org
), 
EJUD_COMPETENCIA AS (
    SELECT 
        oj.codorgjulg,
        OJ.nomebnmp,
        p.nome AS orgaojulgador,
        c.codcompt AS codcompt,
        oj.codmagpres,
        c.descr AS competencia,
        OJ.CODPESS
    FROM ejud.orgaojulgador oj
    LEFT JOIN ejud.competencia c ON oj.codcompt = c.codcompt
    JOIN ejud.pessoa p ON oj.codpess = p.codpess
), 
ORGAO_SCOL_NOMESIGLA AS (
    SELECT
        SC.cod_org, 
        SC.codorgcnj, 
        CAST(C.CODCOMPT AS INTEGER) as CODCOMPT,
        C.COMPETENCIA,
        SC.ind_ejud,
        SC.ind_eproc,
        SC.IND_DISTR,
        SC.NOME,
        C.CODPESS,
        CASE 
            WHEN regexp_like(SC.nome, '[0-9]+') THEN LPAD(regexp_extract(SC.nome, '([0-9]+)', 1), 2, '0')
            ELSE NULL
        END AS sigla_numeral,
        CASE
            WHEN c.codcompt = 1 THEN 'SETOE'
            WHEN c.codcompt = 2 THEN 'DECON'
            WHEN c.codcompt = 3 THEN 'SECIV'
            WHEN c.codcompt = 4 THEN 'SECRI'
            WHEN c.codcompt = 5 THEN 'GCCIV'
            WHEN c.codcompt = 6 THEN 'GCCRI'
            WHEN c.codcompt = 7 THEN 'CCIV'
            WHEN c.codcompt = 8 THEN 'CCRI'
            WHEN c.codcompt = 9 THEN '3VP'
            WHEN c.codcompt = 10 THEN 'TRCRI'
            WHEN c.codcompt = 11 THEN 'TRCIV'
            WHEN c.codcompt = 12 THEN '2VP'
            WHEN c.codcompt = 14 AND SC.COD_ORG IN (6739,6740) THEN 'TRFAZ'
            WHEN c.codcompt = 14 AND SC.COD_ORG = 9816 THEN 'TRFAZEXT'
            WHEN c.codcompt = 17 AND SC.cod_org = 9377 THEN 'SECIV'
            WHEN c.codcompt = 17 AND SC.cod_org = 9378 THEN 'SECON'
            WHEN c.codcompt = 19 AND SC.cod_org = 9525 THEN 'TRUCIV'
            WHEN c.codcompt = 20 AND SC.cod_org = 9940 THEN 'TRUFAZ'
            WHEN c.codcompt = 21 THEN '2VP'
            WHEN c.codcompt = 23 THEN 'CPRI'
            WHEN c.codcompt = 24 THEN 'CPUB'
            WHEN c.codcompt = 25 THEN 'CEMP'
            WHEN c.codcompt = 26 THEN 'SEPUB'
            WHEN c.codcompt = 27 THEN 'SEPRI'
            WHEN c.codcompt = 28 THEN 'CEREU'
            WHEN c.codcompt = 29 THEN 'NUDIG'
        END AS sigla,
        CASE 
            WHEN regexp_like(SC.nome, '[0-9]+') AND c.codcompt != 7 THEN regexp_extract(SC.nome, '([0-9]+)', 1)
            WHEN regexp_like(C.nomebnmp, '[0-9]+') AND c.codcompt = 7 THEN regexp_extract(C.nomebnmp, '([0-9]+)', 1)
            ELSE NULL
        END AS nome_numeral, 
        CASE
            WHEN C.CODCOMPT IN (5,6) THEN 'º'
            ELSE 'ª'
        END AS ARTIGO,
        CASE
            WHEN C.CODCOMPT = 1 THEN 'Orgão Especial'
            WHEN C.CODCOMPT = 5 THEN 'Secretaria do X Grupo de Câmaras Cíveis'
            WHEN C.CODCOMPT = 6 AND c.codmagpres IS NOT NULL THEN 'X Grupo de Câmara Criminal'
            WHEN C.CODCOMPT = 6 AND c.codmagpres IS NULL THEN 'Secretaria do X Grupo de Câmara Criminais'
            WHEN C.CODCOMPT = 7 THEN 'X Câmara Cível'
            WHEN C.CODCOMPT = 8 THEN 'X Câmara Criminal'
            WHEN C.CODCOMPT = 10 THEN 'X Turma Recursal Criminal'
            WHEN C.CODCOMPT = 11 THEN 'X Turma Recursal Cível'
            WHEN C.CODCOMPT = 14 THEN 'X Turma Recursal Fazendária'
            WHEN C.CODCOMPT = 23 THEN 'X Câmara de Direito Privado'
            WHEN C.CODCOMPT = 24 THEN 'X Câmara de Direito Público'
            WHEN C.CODCOMPT = 25 THEN 'X Câmara de Direito Empresarial'
            WHEN C.CODCOMPT = 26 THEN 'Seção de Direito Público'
            WHEN C.CODCOMPT = 27 THEN 'Seção de Direito Privado'
            ELSE SC.nome
        END AS NOME_PADRONIZADO
    FROM ORGAO_SCOL SC
    LEFT JOIN EJUD_COMPETENCIA C ON SC.cod_org = C.CODORGJULG
)
-- SELECT FINAL SEM INSERT OU CONFLICT
SELECT
    COD_ORG AS cod_orgao,
    CODORGCNJ AS cod_orgao_cnj,
    NOME AS nm_orgao_scol,
    CASE 
        WHEN NOME_NUMERAL IS NOT NULL THEN REPLACE(NOME_PADRONIZADO, 'X', NOME_NUMERAL || ARTIGO) 
        ELSE NOME 
    END AS nm_orgao,
    CODCOMPT AS cod_competencia,
    COMPETENCIA AS nm_competencia,
    CASE
        WHEN SIGLA_NUMERAL IS NULL THEN SIGLA
        WHEN CODCOMPT = 9 THEN SIGLA
        WHEN CODCOMPT = 21 THEN SIGLA
        WHEN COD_ORG = 522 THEN 'CM'
        WHEN CODCOMPT IN (12,21) THEN '2VP'
        WHEN CODCOMPT IN (29) THEN 'NUDIG'
        ELSE SIGLA_NUMERAL || SIGLA
    END AS nm_sigla,
    IND_EJUD AS ind_ejud,
    IND_EPROC AS ind_eproc2g,
    IND_DISTR AS ind_distribuicao,
    CASE
        WHEN CODCOMPT IN (6,21) THEN ROW_NUMBER() OVER (PARTITION BY CODCOMPT ORDER BY CAST(CODPESS AS INTEGER) ASC)
        WHEN CODCOMPT != 6 THEN ROW_NUMBER() OVER (PARTITION BY CODCOMPT ORDER BY CAST(NOME_NUMERAL AS INTEGER) ASC)
        ELSE NULL
    END AS NUM_ORDINAL
FROM ORGAO_SCOL_NOMESIGLA

UNION ALL

SELECT 
    0 AS cod_orgao,
    NULL AS cod_orgao_cnj,
    'NÃO ENCONTRADO' AS nm_orgao_scol,
    'NÃO ENCONTRADO' AS nm_orgao,
    NULL AS cod_competencia,
    NULL AS nm_competencia,
    NULL AS nm_sigla,
    '?' AS ind_ejud,
    '?' AS ind_eproc2g,
    '?' AS ind_distribuicao,
    NULL AS num_ordinal;