-- ============================================================
-- EPROC2G — Meta 01 variable counts in prata after test run
-- Run after eproc_metacnj_m01_emr.py to confirm all 5 variables
-- were written correctly for cod_sistema=2.
-- ============================================================

-- ── 1. Row counts per variable (quick sanity check) ─────────────────────────
-- Expected: 5 rows (cnm01, julgadom01, susm01, desm01, PendJulg01TG).
-- PendJulg01TG must be <= cnm01 (every pending process is a new case).
-- julgadom01 must be <= cnm01 (can only judge new cases in same period).

SELECT
    variavel,
    cod_indicador,
    count(*)                    AS n_processos,
    count(DISTINCT id_processo) AS n_unique_processos,
    min(dth_evento_indicador)   AS dth_min,
    max(dth_evento_indicador)   AS dth_max
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano   = '2026'
  AND cd_meta    = '01'
  AND cod_sistema = 2
GROUP BY variavel, cod_indicador
ORDER BY cod_indicador;


-- ── 2. PendJulg01TG consistency check ───────────────────────────────────────
-- PendJulg01TG MUST equal cnm01 MINUS julgadom01 (no overlap, no gap).
-- diff_pendente = cnm01 - julgadom01 - PendJulg01TG  → must be 0.

WITH counts AS (
    SELECT
        variavel,
        count(*) AS n
    FROM dw_2inst_prata.tb_2inst_metacnj
    WHERE meta_ano    = '2026'
      AND cd_meta     = '01'
      AND cod_sistema = 2
    GROUP BY variavel
)
SELECT
    cnm01.n                             AS n_cnm01,
    julg.n                              AS n_julgadom01,
    pend.n                              AS n_PendJulg01TG,
    cnm01.n - julg.n                    AS cnm01_minus_julgadom01,
    cnm01.n - julg.n - pend.n          AS diff_pendente   -- must be 0
FROM       counts cnm01 CROSS JOIN counts julg CROSS JOIN counts pend
WHERE cnm01.variavel = 'cnm01'
  AND julg.variavel  = 'julgadom01'
  AND pend.variavel  = 'PendJulg01TG';


-- ── 3. PendJulg01TG ∩ julgadom01 must be empty ──────────────────────────────
-- A process cannot be both judged and pending at the same time.

SELECT count(*) AS n_overlap   -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj pend
JOIN dw_2inst_prata.tb_2inst_metacnj julg
    ON  pend.id_processo = julg.id_processo
    AND pend.meta_ano    = julg.meta_ano
WHERE pend.meta_ano     = '2026'
  AND pend.cod_sistema  = 2
  AND pend.variavel     = 'PendJulg01TG'
  AND julg.cod_sistema  = 2
  AND julg.variavel     = 'julgadom01';


-- ── 4. julgadom01 ⊆ cnm01 check ─────────────────────────────────────────────
-- Every judged process must also appear in cnm01 (judged = subset of new).

SELECT count(*) AS n_julg_not_in_cnm01   -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj julg
LEFT JOIN dw_2inst_prata.tb_2inst_metacnj cnm
    ON  julg.id_processo = cnm.id_processo
    AND julg.meta_ano    = cnm.meta_ano
    AND cnm.cod_sistema  = 2
    AND cnm.variavel     = 'cnm01'
WHERE julg.meta_ano    = '2026'
  AND julg.cod_sistema = 2
  AND julg.variavel    = 'julgadom01'
  AND cnm.id_processo  IS NULL;
