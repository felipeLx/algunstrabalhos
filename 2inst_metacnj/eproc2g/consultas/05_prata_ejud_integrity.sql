-- ============================================================
-- EPROC2G — Verify ejud rows (cod_sistema=1) were NOT deleted
-- after running eproc_metacnj_m01_emr.py (DELETE+APPEND strategy).
--
-- CRITICAL: ejud uses overwritePartitions() for the same (meta_ano, variavel)
-- partition. eproc2g must use DELETE cod_sistema=2 + APPEND.
-- If ejud runs AFTER eproc2g it will overwrite and delete eproc2g rows —
-- this is expected, the orchestration order is: ejud FIRST, eproc2g SECOND.
-- ============================================================

-- ── 1. Both systems present in each partition ─────────────────────────────
-- After both ejud and eproc2g m01 have run, every (meta_ano, variavel)
-- partition should contain rows for BOTH cod_sistema=1 AND cod_sistema=2.

SELECT
    meta_ano,
    variavel,
    sum(CASE WHEN cod_sistema = 1 THEN 1 ELSE 0 END) AS n_ejud,
    sum(CASE WHEN cod_sistema = 2 THEN 1 ELSE 0 END) AS n_eproc
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cd_meta  = '01'
GROUP BY meta_ano, variavel
ORDER BY variavel;


-- ── 2. No cod_sistema other than 1 or 2 ──────────────────────────────────────
-- Sanity: only two systems write to this table.

SELECT cod_sistema, count(*) AS n
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cd_meta  = '01'
GROUP BY cod_sistema
ORDER BY cod_sistema;


-- ── 3. ejud row count before vs after eproc2g run ────────────────────────────
-- Run BEFORE the eproc2g script to capture a baseline, compare AFTER.
-- Store the result manually; the numbers should be identical.
--
-- BEFORE snapshot (run before eproc2g m01):
SELECT variavel, count(*) AS n_ejud_before
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 1
GROUP BY variavel
ORDER BY variavel;

-- AFTER snapshot (run after eproc2g m01):
-- n_ejud_before == n_ejud_after  → DELETE+APPEND worked correctly
SELECT variavel, count(*) AS n_ejud_after
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 1
GROUP BY variavel
ORDER BY variavel;


-- ── 4. Duplicate id_processo per system per variable ─────────────────────────
-- Each process should appear at most once per (cod_sistema, meta_ano, variavel).
-- Any n_dupes > 0 means _save_eproc ran more than once without the DELETE firing.

SELECT
    cod_sistema,
    variavel,
    count(*) AS n_total_rows,
    count(DISTINCT id_processo) AS n_unique,
    count(*) - count(DISTINCT id_processo) AS n_dupes   -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cd_meta  = '01'
GROUP BY cod_sistema, variavel
ORDER BY cod_sistema, variavel;
