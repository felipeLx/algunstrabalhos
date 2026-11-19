-- ============================================================
-- VALIDATE — ejud_metacnj_m01_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_m01_emr.py
-- Target: dw_2inst_ouro.tb_ft_meta_analitico (cod_sistema=1, cd_meta='01')
--
-- Variables written:
--   cnm01        cod_indicador=1  Casos novos (first sit-88 in period)
--   julgadom01   cod_indicador=2  Judged from cnm01 pool in period
--   susm01       cod_indicador=3  Suspended at period_end (snapshot)
--   desm01       cod_indicador=4  Unsuspended during period (flow)
--
-- Run after: ejud_metacnj_m01_emr.py --year 2026
--            --period-start 2026-01-01 --period-end 2026-12-31
-- ============================================================


-- ── 1. Count per variable — quick sanity check ─────────────────────────────
-- Expected: 4 rows (cnm01/julgadom01/susm01/desm01), all n > 0
SELECT cd_variavel, cod_indicador, count(*) AS n
FROM dw_2inst_ouro.tb_ft_meta_analitico m
join dim_2inst_ouro.tb_dim_variavel v on m.cod_indicador = v.id_variavel
WHERE meta_ano = 2025
  AND cod_indicador  = 1
  AND cod_sistema = 1
GROUP BY cd_variavel, cod_indicador;


-- ── 2. julgadom01 <= cnm01 sanity ─────────────────────────────────────────
-- julgadom01 is subset of cnm01 pool → count(julg) must be <= count(cnm01)
WITH counts AS (
    SELECT cod_indicador, count(*) AS n
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 1
    GROUP BY cod_indicador
)
SELECT
    max(CASE WHEN cod_indicador = 1 THEN n END) AS n_cnm01,
    max(CASE WHEN cod_indicador = 2 THEN n END) AS n_julgadom01,
    max(CASE WHEN cod_indicador = 3 THEN n END) AS n_susm01,
    max(CASE WHEN cod_indicador = 4 THEN n END) AS n_desm01,
    max(CASE WHEN cod_indicador = 2 THEN n END) <=
        max(CASE WHEN cod_indicador = 1 THEN n END) AS julg_lte_cnm01
FROM counts;


-- ── 3. dth_evento_indicador range check ────────────────────────────────────
-- All events must fall within the script period [2026-01-01, 2026-12-31]
SELECT cod_indicador, variavel,
       min(dth_evento_indicador) AS earliest,
       max(dth_evento_indicador) AS latest
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 1
GROUP BY cod_indicador, variavel
ORDER BY cod_indicador;


-- ── 4. Spot check: cnm01 source events ────────────────────────────────────
-- sit-88 events that triggered cnm01 — must have trigger sit IN (61,24,9,65,81)
-- and first sit-88 dthrmov in period.
SELECT situacao, count(*) AS n
FROM dw_2inst_ouro.tb_tf_situacao_ejud
WHERE situacao = 88
  AND year_month >= '2026-01'
  AND year_month <= '2026-12'
GROUP BY situacao;


-- ── 5. Duplicate id_processo check per variable ─────────────────────────────
-- Each variable should have at most 1 row per id_processo per meta_ano
SELECT variavel, count(*) AS n_total, count(DISTINCT id_processo) AS n_distinct,
       count(*) - count(DISTINCT id_processo) AS n_duplicates
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 1
GROUP BY variavel
ORDER BY variavel;


-- ── 6. Schema ──────────────────────────────────────────────────────────────
DESCRIBE dw_2inst_ouro.tb_ft_meta_analitico;
