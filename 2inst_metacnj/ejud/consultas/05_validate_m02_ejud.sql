-- ============================================================
-- VALIDATE — ejud_metacnj_m02_emr.py
--
-- Script: ejud/scripts/ejud_metacnj_m02_emr.py
-- Target: dw_2inst_ouro.tb_ft_meta_analitico (cod_sistema=1, cd_meta='02')
--
-- Variables written (all in same target table):
--   dism02           Stock  — sit-88 before cutoff, not judged
--   dism_ant02       Stock  — same, dist <= 15 years before stock year
--   primeirasentm02  Flow   — first sentence after cutoff (dism02 pool)
--   baixm02          Flow   — first baixa after cutoff (dism02 pool)
--   mpum02           Flow   — first MPU decision after cutoff (dism02 pool)
--   julgadom02       Flow   — union primeirasentm02 + baixm02 + mpum02
--   julgadom02_ant   Flow   — first judgment (dism_ant02 pool)
--   susm02           Snap   — first suspension after cutoff (dism02 pool)
--   susm02_ant       Snap   — first suspension (dism_ant02 pool)
--   desm_ant02       Snap   — first levantamento (dism_ant02 pool)
--
-- Run after: ejud_metacnj_m02_emr.py --year 2026 --cutoff-date 2023-12-31
-- ============================================================


-- ── 1. Count per variable ───────────────────────────────────────────────────
-- Expected: 10 rows, all n > 0
SELECT variavel, cod_indicador, count(*) AS n
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026'
  AND cd_meta  = '02'
  AND cod_sistema = 1
GROUP BY variavel, cod_indicador
ORDER BY cod_indicador;


-- ── 2. Stock vs flow sanity ─────────────────────────────────────────────────
-- julgadom02 count <= dism02 count (judged is subset of eligible stock)
-- susm02 count <= dism02 count - julgadom02 count (suspended from non-judged)
WITH counts AS (
    SELECT variavel, count(*) AS n
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
    GROUP BY variavel
)
SELECT
    max(CASE WHEN variavel = 'dism02'       THEN n END) AS n_dism02,
    max(CASE WHEN variavel = 'dism_ant02'   THEN n END) AS n_dism_ant02,
    max(CASE WHEN variavel = 'julgadom02'   THEN n END) AS n_julgadom02,
    max(CASE WHEN variavel = 'susm02'       THEN n END) AS n_susm02,
    max(CASE WHEN variavel = 'desm_ant02'   THEN n END) AS n_desm_ant02,
    max(CASE WHEN variavel = 'julgadom02'   THEN n END) <=
        max(CASE WHEN variavel = 'dism02'   THEN n END) AS julg_lte_dism02,
    max(CASE WHEN variavel = 'julgadom02_ant' THEN n END) <=
        max(CASE WHEN variavel = 'dism_ant02'  THEN n END) AS julg_ant_lte_dism_ant02
FROM counts;


-- ── 3. dism02 date range — all events must be < cutoff (2023-12-31) ─────────
-- sit-88 first event must pre-date the cutoff
SELECT variavel,
       min(dth_evento_indicador) AS earliest_88,
       max(dth_evento_indicador) AS latest_88
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
  AND variavel IN ('dism02', 'dism_ant02')
GROUP BY variavel;


-- ── 4. Flow events must be AFTER cutoff (2023-12-31) ───────────────────────
SELECT variavel,
       min(dth_evento_indicador) AS earliest_flow,
       max(dth_evento_indicador) AS latest_flow
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
  AND variavel IN ('primeirasentm02', 'baixm02', 'mpum02',
                   'julgadom02', 'julgadom02_ant')
GROUP BY variavel
ORDER BY variavel;


-- ── 5. julgadom02 = primeirasentm02 ∪ baixm02 ∪ mpum02 union check ─────────
-- id_processo in julgadom02 should equal union of the three sub-flows
WITH julg_ids AS (
    SELECT DISTINCT id_processo FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
      AND variavel = 'julgadom02'
),
sub_ids AS (
    SELECT DISTINCT id_processo FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
      AND variavel IN ('primeirasentm02', 'baixm02', 'mpum02')
)
SELECT
    (SELECT count(*) FROM julg_ids)  AS n_julgadom02,
    (SELECT count(*) FROM sub_ids)   AS n_union_subflows,
    (SELECT count(*) FROM julg_ids) -
    (SELECT count(*) FROM sub_ids)   AS diff;


-- ── 6. Duplicate check per variable ────────────────────────────────────────
SELECT variavel, count(*) AS n_total, count(DISTINCT id_processo) AS n_distinct,
       count(*) - count(DISTINCT id_processo) AS n_duplicates
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = '2026' AND cd_meta = '02' AND cod_sistema = 1
GROUP BY variavel
ORDER BY variavel;
