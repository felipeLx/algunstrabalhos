-- ============================================================
-- VALIDATE — eproc_metacnj_m01_emr.py
--
-- Script: eproc2g/scripts/eproc_metacnj_m01_emr.py
-- Source: dw_2inst_ouro.tb_tf_situacao_eproc
-- Target: dw_2inst_prata.tb_2inst_metacnj (cod_sistema=2, cd_meta='01')
--
-- CRITICAL: ejud m01 must run BEFORE eproc m01.
-- Write strategy: DELETE WHERE cod_sistema=2 AND meta_ano=X AND variavel=Y → APPEND
-- (cannot overwrite — would delete ejud cod_sistema=1 rows from same partition)
--
-- Variables written (cod_sistema=2):
--   cnm01        cod_indicador=1  First sit-88 in period (trigger: sit IN 24,9,65,81)
--   julgadom01   cod_indicador=2  All eligible judged in period (not only cnm01 pool)
--   susm01       cod_indicador=3  Suspended at period_end (snapshot)
--   desm01       cod_indicador=4  Unsuspended during period (flow)
--   PendJulg01TG cod_indicador=5  cnm01 LEFT ANTI JOIN julgadom01
--
-- NOTE: sit-61 does NOT exist in eproc2g — only sit-24 used as trigger for cnm01.
--
-- Run after: eproc_metacnj_m01_emr.py --year 2026
--            --period-start 2026-01-01 --period-end 2026-12-31
-- ============================================================


-- ── 1. Count per variable ───────────────────────────────────────────────────
-- Expected: 5 rows (cnm01/julgadom01/susm01/desm01/PendJulg01TG), all n > 0
SELECT variavel, cod_indicador, count(*) AS n
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cd_meta  = '01'
  AND cod_sistema = 2
GROUP BY variavel, cod_indicador
ORDER BY cod_indicador;


-- ── 2. PendJulg01TG = cnm01 - julgadom01 sanity ────────────────────────────
-- PendJulg01TG is cnm01 ANTI JOIN julgadom01 → count(pend) = count(cnm01) - count(julg)
-- Note: julgadom01 covers ALL eligible (not just cnm01 pool), so diff may be > count(pend)
WITH counts AS (
    SELECT cod_indicador, count(*) AS n
    FROM dw_2inst_prata.tb_2inst_metacnj
    WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 2
    GROUP BY cod_indicador
)
SELECT
    max(CASE WHEN cod_indicador = 1 THEN n END) AS n_cnm01,
    max(CASE WHEN cod_indicador = 2 THEN n END) AS n_julgadom01,
    max(CASE WHEN cod_indicador = 3 THEN n END) AS n_susm01,
    max(CASE WHEN cod_indicador = 4 THEN n END) AS n_desm01,
    max(CASE WHEN cod_indicador = 5 THEN n END) AS n_pendJulg01TG,
    max(CASE WHEN cod_indicador = 2 THEN n END) <=
        max(CASE WHEN cod_indicador = 1 THEN n END) AS julg_pool_lte_cnm01,
    max(CASE WHEN cod_indicador = 5 THEN n END) <=
        max(CASE WHEN cod_indicador = 1 THEN n END) AS pend_lte_cnm01
FROM counts;


-- ── 3. No cod_sistema=2 rows remain from previous run (clean delete+append) ──
-- If script ran correctly, only one batch of cod_sistema=2 rows exists for this meta_ano
SELECT meta_ano, cd_meta, cod_sistema, count(*) AS n_total
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026' AND cd_meta = '01'
GROUP BY meta_ano, cd_meta, cod_sistema
ORDER BY cod_sistema;


-- ── 4. dth_evento_indicador range — must be within [2026-01-01, 2026-12-31] ─
SELECT cod_indicador, variavel,
       min(dth_evento_indicador) AS earliest,
       max(dth_evento_indicador) AS latest
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 2
GROUP BY cod_indicador, variavel
ORDER BY cod_indicador;


-- ── 5. cnm01 source: sit-24 events in tb_tf_situacao_eproc ─────────────────
-- Verify sit-24 (Distribuído) exists and had events in period
SELECT situacao, count(*) AS n_events, count(DISTINCT id_processo) AS n_processos
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE situacao IN (24, 9, 65, 81)
  AND year_month >= '2026-01'
  AND year_month <= '2026-12'
GROUP BY situacao
ORDER BY situacao;


-- ── 6. Duplicate check per variable ────────────────────────────────────────
SELECT variavel, count(*) AS n_total, count(DISTINCT id_processo) AS n_distinct,
       count(*) - count(DISTINCT id_processo) AS n_duplicates
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 2
GROUP BY variavel
ORDER BY variavel;


-- ── 7. Schema ──────────────────────────────────────────────────────────────
DESCRIBE dw_2inst_prata.tb_2inst_metacnj;
