-- ============================================================
-- EJUD — Per-process diagnostics
-- Use these queries to investigate why a specific process
-- appears in ouro but not prata (or vice-versa).
-- Replace :nrproc with the actual process number.
-- ============================================================

-- ── 1. Check process in prata and ouro ──────────────────────────────────────

-- In prata?
SELECT *
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE id_processo = :nrproc
  AND meta_ano = '2026'
  AND cod_sistema = 1;

-- In ouro?
SELECT *
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE id_processo = :nrproc
  AND meta_ano = 2026;

-- ── 2. Process base info ─────────────────────────────────────────────────────

SELECT
    p.nrproc,
    p.codtipproc,
    tp.codextcnj,
    tp.desctipproc,
    tp.codnat,
    p.dtdistribuicao,
    p.dtsituacao,
    p.codsituacao
FROM ejud_bronze.processo p
JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
WHERE p.nrproc = :nrproc;

-- ── 3. Eligible coddoc check (conhecimento filter) ──────────────────────────
-- A process is eligible if its codtipproc has codextcnj IN CLASSE_CONHECIMENTO
-- AND (codnat = 1 OR codnat IS NULL).

SELECT
    p.nrproc,
    tp.codextcnj,
    tp.codnat,
    CASE
        WHEN cast(trim(tp.codnat) AS int) = 1  THEN 'eligible (codnat=1)'
        WHEN tp.codnat IS NULL                 THEN 'eligible (codnat=NULL turma)'
        WHEN cast(trim(tp.codnat) AS int) = 2  THEN 'EXCLUDED (codnat=2 criminal)'
        ELSE 'check manually'
    END AS eligibility
FROM ejud_bronze.processo p
JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
WHERE p.nrproc = :nrproc;

-- ── 4. Distribuicao events (coddoc) for the process ─────────────────────────
-- Used by m01 and mNN to identify which coddoc to count.

SELECT
    d.nrproc,
    d.coddoc,
    d.dtdoc,
    d.codnatureza,
    d.tipodoc
FROM ejud_bronze.documento d
WHERE d.nrproc = :nrproc
ORDER BY d.dtdoc;

-- ── 5. Situacao movements for the process ────────────────────────────────────
-- Check if process has a valid baixa (situacao=2) movement.

SELECT
    m.nrproc,
    m.dtmov,
    m.codfase,
    i.codfasenivel2cnj,
    i.codcompl1nivel3cnj,
    i.descrfasenivel2cnj
FROM ejud_bronze.movimento m
JOIN ejud_bronze.integracaofasecompl_ejud_cnj i ON i.codfase = m.codfase
WHERE m.nrproc = :nrproc
ORDER BY m.dtmov DESC;
