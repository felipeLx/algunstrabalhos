-- ============================================================
-- VALIDATE — ejud_metacnj_relatorio_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_relatorio_emr.py
-- Source: dw_2inst_ouro.tb_ft_meta_analitico
-- Targets:
--   dw_2inst_ouro.tb_ft_meta_por_oj         (grouped by cod_ult_oj)
--   dw_2inst_ouro.tb_ft_meta_por_magistrado  (grouped by cod_ult_magistrado)
--
-- One row per (cod_ult_oj | cod_ult_magistrado, meta_ano, nm_meta, segmento).
-- nm_meta: M01, M02, M02d, M04, M06, M07a, M07b, M07c, M08a, M08b, M10, M11
-- segmento: '2inst' | 'turmas' | NULL
--
-- Run after: ejud_metacnj_relatorio_emr.py --year 2026
-- ============================================================


-- ── 1. Row count per nm_meta and segmento ──────────────────────────────────
SELECT nm_meta, segmento, count(*) AS n_oj_rows
FROM dw_2inst_ouro.tb_ft_meta_por_oj
WHERE meta_ano = 2026
GROUP BY nm_meta, segmento
ORDER BY nm_meta, segmento;

SELECT nm_meta, segmento, count(*) AS n_mag_rows
FROM dw_2inst_ouro.tb_ft_meta_por_magistrado
WHERE meta_ano = 2026
GROUP BY nm_meta, segmento
ORDER BY nm_meta, segmento;


-- ── 2. pct NULL check — denominator <= 0 rows ──────────────────────────────
-- pct = NULL when n_denom <= 0 (expected for some OJs with no stock)
SELECT nm_meta, segmento,
       count(*) AS n_total,
       sum(CASE WHEN pct IS NULL THEN 1 ELSE 0 END) AS n_null_pct,
       sum(CASE WHEN n_denom <= 0 THEN 1 ELSE 0 END) AS n_zero_denom
FROM dw_2inst_ouro.tb_ft_meta_por_oj
WHERE meta_ano = 2026
GROUP BY nm_meta, segmento
ORDER BY nm_meta, segmento;


-- ── 3. pct range sanity ────────────────────────────────────────────────────
-- Some metas allow pct > 100 (M02 factor = 1000/9 ≈ 111.1); others capped at 100
SELECT nm_meta,
       min(pct) AS min_pct,
       max(pct) AS max_pct,
       avg(pct) AS avg_pct
FROM dw_2inst_ouro.tb_ft_meta_por_oj
WHERE meta_ano = 2026 AND pct IS NOT NULL
GROUP BY nm_meta
ORDER BY nm_meta;


-- ── 4. M01 formula spot check: pct = julg / (cnm + des - sus) * 100 ────────
-- Pick top 5 OJs for M01 to verify formula manually
SELECT cod_ult_oj, nm_meta, segmento, n_julg, n_denom, pct
FROM dw_2inst_ouro.tb_ft_meta_por_oj
WHERE meta_ano = 2026 AND nm_meta = 'M01'
ORDER BY n_denom DESC
LIMIT 10;


-- ── 5. Turmas segmento check ────────────────────────────────────────────────
-- TURMAS = cod_ult_oj IN (9,10,11,14,15,19,20,22) — expect segmento='turmas'
-- M02 turmas factor = 1000/9.5 (differs from 2inst 1000/9)
SELECT DISTINCT segmento, nm_meta
FROM dw_2inst_ouro.tb_ft_meta_por_oj
WHERE meta_ano = 2026 AND cod_ult_oj IN (9, 10, 11, 14, 15, 19, 20, 22)
ORDER BY nm_meta, segmento;


-- ── 6. Symmetry: same nm_meta in both oj and magistrado tables ─────────────
SELECT
    oj.nm_meta,
    oj.n_oj,
    mag.n_mag
FROM (
    SELECT nm_meta, count(*) AS n_oj
    FROM dw_2inst_ouro.tb_ft_meta_por_oj
    WHERE meta_ano = 2026 GROUP BY nm_meta
) oj
FULL OUTER JOIN (
    SELECT nm_meta, count(*) AS n_mag
    FROM dw_2inst_ouro.tb_ft_meta_por_magistrado
    WHERE meta_ano = 2026 GROUP BY nm_meta
) mag ON oj.nm_meta = mag.nm_meta
ORDER BY oj.nm_meta;
