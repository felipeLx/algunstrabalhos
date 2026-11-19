-- ============================================================
-- VALIDATE — ejud_metacnj_relatorio_prim_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_relatorio_prim_emr.py
-- Sources: dw_2inst_ouro.tb_ft_meta_analitico
--          dw_2inst_ouro.tb_ft_meta_aux  (cod_prim_oj / cod_prim_magistrado)
-- Targets:
--   dw_2inst_ouro.tb_ft_meta_por_prim_oj         (grouped by cod_prim_oj)
--   dw_2inst_ouro.tb_ft_meta_por_prim_magistrado  (grouped by cod_prim_magistrado)
--
-- Same formulas as relatorio_emr.py but attributed to FIRST OJ/magistrado
-- (dth_prim_distribuicao) instead of last (cod_ult_oj).
--
-- Run after: ejud_metacnj_relatorio_prim_emr.py --year 2026
--            Requires tb_ft_meta_aux to be populated first (run 06 script first).
-- ============================================================


-- ── 1. Row count per nm_meta — compare with relatorio (ult) ────────────────
-- prim tables may differ from ult tables when redistribuicao changes OJ/magistrado
SELECT
    p.nm_meta,
    p.n_prim_oj,
    u.n_ult_oj,
    p.n_prim_oj - u.n_ult_oj AS diff_oj
FROM (
    SELECT nm_meta, count(*) AS n_prim_oj
    FROM dw_2inst_ouro.tb_ft_meta_por_prim_oj
    WHERE meta_ano = 2026 GROUP BY nm_meta
) p
FULL OUTER JOIN (
    SELECT nm_meta, count(*) AS n_ult_oj
    FROM dw_2inst_ouro.tb_ft_meta_por_oj
    WHERE meta_ano = 2026 GROUP BY nm_meta
) u ON p.nm_meta = u.nm_meta
ORDER BY p.nm_meta;


-- ── 2. pct spot check for M01 (prim) ───────────────────────────────────────
SELECT cod_prim_oj, nm_meta, segmento, n_julg, n_denom, pct
FROM dw_2inst_ouro.tb_ft_meta_por_prim_oj
WHERE meta_ano = 2026 AND nm_meta = 'M01'
ORDER BY n_denom DESC
LIMIT 10;


-- ── 3. NULL pct count ──────────────────────────────────────────────────────
SELECT nm_meta,
       count(*) AS n_total,
       sum(CASE WHEN pct IS NULL THEN 1 ELSE 0 END) AS n_null_pct
FROM dw_2inst_ouro.tb_ft_meta_por_prim_oj
WHERE meta_ano = 2026
GROUP BY nm_meta
ORDER BY nm_meta;


-- ── 4. cod_prim_oj not in tb_ft_meta_aux — should be zero ──────────────────
-- Every OJ in prim report must exist in aux table
SELECT count(*) AS n_prim_oj_not_in_aux
FROM dw_2inst_ouro.tb_ft_meta_por_prim_oj p
WHERE meta_ano = 2026
  AND NOT EXISTS (
      SELECT 1 FROM dw_2inst_ouro.tb_ft_meta_aux a
      WHERE a.meta_ano = '2026' AND a.cod_prim_oj = p.cod_prim_oj
  );


-- ── 5. Symmetry: prim_oj vs prim_magistrado tables ─────────────────────────
SELECT
    oj.nm_meta,
    oj.n_prim_oj,
    mag.n_prim_mag
FROM (
    SELECT nm_meta, count(*) AS n_prim_oj
    FROM dw_2inst_ouro.tb_ft_meta_por_prim_oj
    WHERE meta_ano = 2026 GROUP BY nm_meta
) oj
FULL OUTER JOIN (
    SELECT nm_meta, count(*) AS n_prim_mag
    FROM dw_2inst_ouro.tb_ft_meta_por_prim_magistrado
    WHERE meta_ano = 2026 GROUP BY nm_meta
) mag ON oj.nm_meta = mag.nm_meta
ORDER BY oj.nm_meta;
