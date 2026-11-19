-- ============================================================
-- VALIDATE — ejud_metacnj_meta_aux_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_meta_aux_emr.py
-- Source: dw_2inst_ouro.tb_ft_meta_analitico (all cod_sistema)
-- Target: dw_2inst_ouro.tb_ft_meta_aux
--
-- One row per (id_processo, meta_ano) across all systems.
-- Deduplicates distribution/judgment dates and resolves oj/magistrado.
--
-- Run after: ALL m01 + m02 indicator scripts for the target meta_ano.
-- Run after: ejud_metacnj_meta_aux_emr.py --year 2026
-- ============================================================


-- ── 1. Row count per meta_ano ───────────────────────────────────────────────
-- Should match total distinct id_processo in tb_ft_meta_analitico for same year
SELECT meta_ano, count(*) AS n_rows, count(DISTINCT id_processo) AS n_distinct
FROM dw_2inst_ouro.tb_ft_meta_aux
WHERE meta_ano = '2026'
GROUP BY meta_ano;


-- ── 2. Cross-check aux vs analitico distinct processes ─────────────────────
-- n_aux should equal n_analitico
SELECT
    (SELECT count(DISTINCT id_processo)
     FROM dw_2inst_ouro.tb_ft_meta_analitico
     WHERE meta_ano = '2026') AS n_analitico_distinct,
    (SELECT count(DISTINCT id_processo)
     FROM dw_2inst_ouro.tb_ft_meta_aux
     WHERE meta_ano = '2026') AS n_aux_distinct;


-- ── 3. NULL check on distribution date columns ─────────────────────────────
-- dth_prim_distribuicao should never be NULL (every process has at least one dist)
SELECT
    count(*) AS n_total,
    sum(CASE WHEN dth_prim_distribuicao IS NULL THEN 1 ELSE 0 END) AS n_null_prim_dist,
    sum(CASE WHEN dth_ult_distribuicao  IS NULL THEN 1 ELSE 0 END) AS n_null_ult_dist,
    sum(CASE WHEN cod_prim_oj          IS NULL THEN 1 ELSE 0 END) AS n_null_prim_oj,
    sum(CASE WHEN cod_ult_oj           IS NULL THEN 1 ELSE 0 END) AS n_null_ult_oj,
    sum(CASE WHEN ind_julgado = 'S'    THEN 1 ELSE 0 END)         AS n_julgados
FROM dw_2inst_ouro.tb_ft_meta_aux
WHERE meta_ano = '2026';


-- ── 4. Logical check: prim_dist <= ult_dist ─────────────────────────────────
SELECT count(*) AS n_prim_after_ult
FROM dw_2inst_ouro.tb_ft_meta_aux
WHERE meta_ano = '2026'
  AND dth_prim_distribuicao > dth_ult_distribuicao;


-- ── 5. ind_julgado consistency ──────────────────────────────────────────────
-- Processes with ind_julgado='S' must have a julgadom* row in analitico
WITH julgados_aux AS (
    SELECT id_processo FROM dw_2inst_ouro.tb_ft_meta_aux
    WHERE meta_ano = '2026' AND ind_julgado = 'S'
),
julgados_analitico AS (
    SELECT DISTINCT id_processo FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND variavel LIKE 'julgado%'
)
SELECT
    (SELECT count(*) FROM julgados_aux)       AS n_ind_julgado_s,
    (SELECT count(*) FROM julgados_analitico) AS n_julgadom_in_analitico,
    (SELECT count(*) FROM julgados_aux j
     LEFT JOIN julgados_analitico a ON j.id_processo = a.id_processo
     WHERE a.id_processo IS NULL)             AS n_missing_in_analitico;


-- ── 6. Sample rows ─────────────────────────────────────────────────────────
SELECT *
FROM dw_2inst_ouro.tb_ft_meta_aux
WHERE meta_ano = '2026'
LIMIT 20;
