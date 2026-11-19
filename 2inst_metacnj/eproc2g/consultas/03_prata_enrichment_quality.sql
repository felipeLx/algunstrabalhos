-- ============================================================
-- EPROC2G — Enrichment quality checks in prata
-- Verifies that cod_classe_cnj, id_assunto_principal, cod_competencia,
-- cod_natureza, cod_ult_oj are populated correctly for cod_sistema=2.
-- Run on cnm01 (indicator=1) — the base variable; all others share
-- the same enrichment since they are subsets of cnm01.
-- ============================================================

-- ── 1. NULL rates per enriched column ───────────────────────────────────────
-- cod_ult_magistrado is expected NULL (not available without log_processo join).
-- All others should be near 0% NULL for a healthy run.

SELECT
    count(*)                                        AS n_total,
    -- situacao columns (always present)
    sum(CASE WHEN id_processo         IS NULL THEN 1 ELSE 0 END) AS null_id_processo,
    sum(CASE WHEN num_processo        IS NULL THEN 1 ELSE 0 END) AS null_num_processo,
    sum(CASE WHEN dth_evento_indicador IS NULL THEN 1 ELSE 0 END) AS null_dth_evento,
    -- class / subject
    sum(CASE WHEN cod_classe_cnj      IS NULL THEN 1 ELSE 0 END) AS null_cod_classe_cnj,
    sum(CASE WHEN id_assunto_principal IS NULL THEN 1 ELSE 0 END) AS null_assunto,
    -- enrichment
    sum(CASE WHEN cod_competencia     IS NULL THEN 1 ELSE 0 END) AS null_cod_competencia,
    sum(CASE WHEN cod_natureza        IS NULL THEN 1 ELSE 0 END) AS null_cod_natureza,
    sum(CASE WHEN cod_ult_oj          IS NULL THEN 1 ELSE 0 END) AS null_cod_ult_oj,
    -- expected NULL
    sum(CASE WHEN cod_ult_magistrado  IS NULL THEN 1 ELSE 0 END) AS null_magistrado  -- expected = n_total
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01';


-- ── 2. cod_natureza distribution ─────────────────────────────────────────────
-- 1 = civel, 2 = criminal, NULL = unknown competencia.
-- Both civel and criminal processes are expected; verify no unexpected values.

SELECT
    cod_natureza,
    count(*) AS n,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01'
GROUP BY cod_natureza
ORDER BY cod_natureza;


-- ── 3. cod_classe_cnj top 20 ─────────────────────────────────────────────────
-- Sanity check: known 2nd-instance CNJ class codes (e.g. 941=Acórdão, 220=Agravo, etc.)
-- No class with cod_classe_cnj < 1 should appear.

SELECT
    cod_classe_cnj,
    count(*) AS n
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01'
GROUP BY cod_classe_cnj
ORDER BY n DESC
LIMIT 20;


-- ── 4. id_assunto_principal — check it is a CNJ code (not an internal eproc2g ID)
-- Internal eproc2g IDs are typically < 1000 and sequential.
-- CNJ codes are codified 4–6 digit numbers (e.g. 10174, 10020, etc.).
-- After enrichment, the stored value should be the CNJ code (> 1000 expected for most).
-- NOTE: the column is named id_assunto_principal but after enrichment it holds
--       assunto_judicial.cod_mni_assunto_judicial — a 4-5 digit CNJ code.

SELECT
    count(*) AS n_total,
    sum(CASE WHEN id_assunto_principal < 1000 THEN 1 ELSE 0 END) AS n_possibly_internal_id,
    sum(CASE WHEN id_assunto_principal >= 1000 THEN 1 ELSE 0 END) AS n_cnj_code,
    min(id_assunto_principal)   AS min_val,
    max(id_assunto_principal)   AS max_val,
    approx_percentile(CAST(id_assunto_principal AS double), 0.5) AS median_val
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01'
  AND id_assunto_principal IS NOT NULL;


-- ── 5. cod_ult_oj — verify only eligible 2nd-instance courts ─────────────────
-- All id_orgao_colegiado values present should be in COMPETENCIA_COLEGIADO list.
-- [1..29] minus {9,12,13,21} (approximately) — verify no unexpected courts appear.

SELECT
    cod_ult_oj,
    count(*) AS n
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01'
GROUP BY cod_ult_oj
ORDER BY cod_ult_oj;
