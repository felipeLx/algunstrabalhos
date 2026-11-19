-- ============================================================
-- DIAGNOSTIC — tb_tf_situacao_eproc layout
-- Run these BEFORE the eproc_metacnj_m01_emr.py script to
-- understand what values are actually in the table and verify
-- that the filter logic in the script will produce results.
-- ============================================================

-- ── 1. Total rows and year distribution ──────────────────────────────────────
-- Confirm data exists and which years are present.
SELECT YEAR(dth_inicio) AS yr, count(*) AS n_rows
FROM dw_2inst_ouro.tb_tf_situacao_eproc
GROUP BY YEAR(dth_inicio)
ORDER BY yr;


-- ── 2. Distribution situacao codes (DIST_CODES = 61,24,9,65,81) ──────────────
-- Confirm at least one of these codes exists in the table.
-- If this returns 0 rows, cnm01 will always be 0.
SELECT situacao, count(*) AS n
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE situacao IN (61, 24, 9, 65, 81)
GROUP BY situacao
ORDER BY situacao;


-- ── 3. mni_classe_judicial — natureza distribution ───────────────────────────
-- The 'natureza' column drives the CONHECIMENTO filter (replaces classes.xlsx).
-- Only 'Conhecimento' processes are eligible for Meta 01.
SELECT natureza, count(*) AS n_classes
FROM eproc2g.mni_classe_judicial
GROUP BY natureza
ORDER BY n_classes DESC;

-- How many CNJ class codes are 'Conhecimento'?
SELECT count(DISTINCT cod_mni_classe_judicial) AS n_conhecimento_cnj_codes
FROM eproc2g.mni_classe_judicial
WHERE natureza = 'Conhecimento';

-- How many internal id_classe_judicial map to Conhecimento?
SELECT count(DISTINCT r.id_classe_judicial) AS n_eligible_classes
FROM eproc2g.relacao_classe_judicial_mni r
JOIN eproc2g.mni_classe_judicial m
    ON r.cod_mni_classe_judicial = m.cod_mni_classe_judicial
WHERE m.natureza = 'Conhecimento';

-- Competencia distribution in processo (for exclusion list validation)
-- Excluded: cod_competencia IN (1, 2, 9, 2026)
SELECT cod_competencia, count(*) AS n_processos
FROM eproc2g.processo
GROUP BY cod_competencia
ORDER BY n_processos DESC;

-- id_orgao_colegiado sample (orgão julgador — NOT used for eligibility)
SELECT id_orgao_colegiado, count(*) AS n
FROM eproc2g.processo
GROUP BY id_orgao_colegiado
ORDER BY n DESC
LIMIT 20;


-- ── 4. NULL check on key filter columns ──────────────────────────────────────
-- How many rows have NULL situacao or NULL dth_inicio?
-- These rows would be silently excluded by the filters.
SELECT
    sum(CASE WHEN situacao    IS NULL THEN 1 ELSE 0 END) AS n_null_situacao,
    sum(CASE WHEN dth_inicio  IS NULL THEN 1 ELSE 0 END) AS n_null_dth_inicio,
    sum(CASE WHEN id_processo IS NULL THEN 1 ELSE 0 END) AS n_null_id_processo,
    count(*)                                              AS n_total
FROM dw_2inst_ouro.tb_tf_situacao_eproc;


-- ── 5. Eligible processes (mirrors _build_eligible logic exactly) ────────────
-- Step 1: Conhecimento class + competencia exclusion
-- Step 2: has a distribution event up to period_end
-- Expected: > 0 rows.

WITH conhecimento_classes AS (
    SELECT DISTINCT r.id_classe_judicial
    FROM eproc2g.mni_classe_judicial m
    JOIN eproc2g.relacao_classe_judicial_mni r
        ON m.cod_mni_classe_judicial = r.cod_mni_classe_judicial
    WHERE m.natureza = 'Conhecimento'
),
eligible_proc AS (
    SELECT DISTINCT p.id_processo
    FROM eproc2g.processo p
    JOIN conhecimento_classes c ON p.id_classe_judicial = c.id_classe_judicial
    WHERE p.cod_competencia NOT IN (1, 2, 9, 2026)
),
has_dist AS (
    SELECT DISTINCT id_processo
    FROM dw_2inst_ouro.tb_tf_situacao_eproc
    WHERE situacao   IN (61, 24, 9, 65, 81)
      AND dth_inicio <= TIMESTAMP '2026-12-31 23:59:59'
)
SELECT count(*) AS n_eligible_processes
FROM eligible_proc ep
JOIN has_dist hd ON ep.id_processo = hd.id_processo;


-- ── 6. cnm01 candidate processes (distribution in 2026, eligible competencia) ─
-- Expected: > 0 rows (should match n_prata.cnm01 after the run).
-- These are processes whose FIRST EVER distribution is in 2026
-- AND whose cod_competencia is not excluded.

WITH conhecimento_classes AS (
    SELECT DISTINCT r.id_classe_judicial
    FROM eproc2g.mni_classe_judicial m
    JOIN eproc2g.relacao_classe_judicial_mni r
        ON m.cod_mni_classe_judicial = r.cod_mni_classe_judicial
    WHERE m.natureza = 'Conhecimento'
),
eligible AS (
    SELECT DISTINCT p.id_processo
    FROM eproc2g.processo p
    JOIN conhecimento_classes c ON p.id_classe_judicial = c.id_classe_judicial
    WHERE p.cod_competencia NOT IN (1, 2, 9, 2026)
),
first_dist AS (
    SELECT
        s.id_processo,
        min(s.dth_inicio) AS first_dth
    FROM dw_2inst_ouro.tb_tf_situacao_eproc s
    JOIN eligible e ON s.id_processo = e.id_processo
    WHERE s.situacao IN (61, 24, 9, 65, 81)
    GROUP BY s.id_processo
)
SELECT count(*) AS n_cnm01_expected
FROM first_dist
WHERE first_dth >= TIMESTAMP '2026-01-01 00:00:00'
  AND first_dth <= TIMESTAMP '2026-12-31 23:59:59';


-- ── 7. Schema check ──────────────────────────────────────────────────────────
-- Confirm column types. situacao should be INT, dth_inicio should be TIMESTAMP.
DESCRIBE dw_2inst_ouro.tb_tf_situacao_eproc;
