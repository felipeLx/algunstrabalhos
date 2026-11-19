-- ============================================================
-- VALIDATE — ejud_metacnj_relatorio_resp_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_relatorio_resp_emr.py
-- Sources: dw_2inst_ouro.tb_ft_meta_analitico
--          dw_2inst_ouro.tb_ft_meta_aux
--          ejud.movjulg    (codmagrel — responsible magistrado for judgments)
--          ejud.movdespa   (codmag    — fallback magistrado)
--          ejud.movcancedistr (cancelled distribution safety filter)
-- Targets:
--   dw_2inst_ouro.tb_ft_meta_por_mag_resp  (by responsible magistrado)
--   dw_2inst_ouro.tb_ft_meta_por_oj_resp   (by responsible OJ)
--
-- Attribution logic:
--   Judgment cod_indicadores (2,7,10,14,21,28,35,42,49) →
--       codmagrel from movjulg, fallback codmag from movdespa,
--       fallback cod_prim_magistrado from aux
--   All other indicators → cod_prim_magistrado / cod_prim_oj from aux
--
-- Run after: ejud_metacnj_relatorio_resp_emr.py --year 2026
--            Requires tb_ft_meta_aux populated.
-- ============================================================


-- ── 1. Row count per nm_meta ────────────────────────────────────────────────
SELECT nm_meta, count(*) AS n_mag_resp
FROM dw_2inst_ouro.tb_ft_meta_por_mag_resp
WHERE meta_ano = 2026
GROUP BY nm_meta ORDER BY nm_meta;

SELECT nm_meta, count(*) AS n_oj_resp
FROM dw_2inst_ouro.tb_ft_meta_por_oj_resp
WHERE meta_ano = 2026
GROUP BY nm_meta ORDER BY nm_meta;


-- ── 2. Compare resp vs prim totals ─────────────────────────────────────────
-- n_julg aggregate should be same across resp and prim groupings
SELECT
    r.nm_meta,
    r.n_julg_resp,
    p.n_julg_prim,
    r.n_julg_resp - p.n_julg_prim AS diff
FROM (
    SELECT nm_meta, sum(n_julg) AS n_julg_resp
    FROM dw_2inst_ouro.tb_ft_meta_por_mag_resp
    WHERE meta_ano = 2026 GROUP BY nm_meta
) r
FULL OUTER JOIN (
    SELECT nm_meta, sum(n_julg) AS n_julg_prim
    FROM dw_2inst_ouro.tb_ft_meta_por_prim_magistrado
    WHERE meta_ano = 2026 GROUP BY nm_meta
) p ON r.nm_meta = p.nm_meta
ORDER BY r.nm_meta;


-- ── 3. Check movjulg join coverage for judgment indicators ─────────────────
-- How many distinct judgment events have a movjulg match
SELECT count(*) AS n_julg_events_with_movjulg
FROM dw_2inst_ouro.tb_ft_meta_analitico a
JOIN ejud.movjulg j
    ON a.id_processo = j.coddoc
   AND CAST(a.dth_evento_indicador AS DATE) = CAST(j.dthrmov AS DATE)
WHERE a.meta_ano = '2026'
  AND a.cod_indicador IN (2, 7, 10, 14, 21, 28, 35, 42, 49);


-- ── 4. NULL cod_magistrado_resp check ───────────────────────────────────────
-- Should be 0 — all events should resolve a responsible magistrado
SELECT nm_meta, count(*) AS n_null_mag
FROM dw_2inst_ouro.tb_ft_meta_por_mag_resp
WHERE meta_ano = 2026 AND cod_magistrado_resp IS NULL
GROUP BY nm_meta;


-- ── 5. pct range spot check ────────────────────────────────────────────────
SELECT nm_meta, min(pct) AS min_pct, max(pct) AS max_pct
FROM dw_2inst_ouro.tb_ft_meta_por_mag_resp
WHERE meta_ano = 2026 AND pct IS NOT NULL
GROUP BY nm_meta
ORDER BY nm_meta;


-- ── 6. Symmetry: resp_mag vs resp_oj tables ────────────────────────────────
SELECT
    m.nm_meta,
    m.n_mag_resp,
    o.n_oj_resp
FROM (
    SELECT nm_meta, count(*) AS n_mag_resp
    FROM dw_2inst_ouro.tb_ft_meta_por_mag_resp
    WHERE meta_ano = 2026 GROUP BY nm_meta
) m
FULL OUTER JOIN (
    SELECT nm_meta, count(*) AS n_oj_resp
    FROM dw_2inst_ouro.tb_ft_meta_por_oj_resp
    WHERE meta_ano = 2026 GROUP BY nm_meta
) o ON m.nm_meta = o.nm_meta
ORDER BY m.nm_meta;
