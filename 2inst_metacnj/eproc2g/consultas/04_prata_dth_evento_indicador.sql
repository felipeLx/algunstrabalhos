-- ============================================================
-- EPROC2G — dth_evento_indicador correctness per variable
--
-- Expected event times per variable:
--   cnm01        → dth_inicio of distribution event (situacao IN [61,24,9,65,81])
--   julgadom01   → dth_inicio of first judgment event in period
--   susm01       → dth_inicio of distribution event (same as cnm01 — snapshot variable)
--   desm01       → dth_last_susp (when suspension was REMOVED)
--   PendJulg01TG → dth_inicio of distribution event (same as cnm01)
--
-- All event dates must fall within the analysis period [2026-01-01, 2026-12-31]
-- EXCEPT susm01 (snapshot at period_end — distribution may be before period_start).
-- ============================================================

-- ── 1. dth_evento_indicador range per variable ───────────────────────────────
-- Highlight anomalies: NULL dates or dates outside expected window.

SELECT
    variavel,
    cod_indicador,
    count(*)                       AS n,
    sum(CASE WHEN dth_evento_indicador IS NULL              THEN 1 ELSE 0 END) AS n_null_dth,
    sum(CASE WHEN dth_evento_indicador < TIMESTAMP '2026-01-01 00:00:00' THEN 1 ELSE 0 END) AS n_before_period,
    sum(CASE WHEN dth_evento_indicador > TIMESTAMP '2026-12-31 23:59:59' THEN 1 ELSE 0 END) AS n_after_period,
    min(dth_evento_indicador)      AS dth_min,
    max(dth_evento_indicador)      AS dth_max
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
GROUP BY variavel, cod_indicador
ORDER BY cod_indicador;


-- ── 2. cnm01 vs susm01 dth comparison ───────────────────────────────────────
-- For processes in BOTH cnm01 AND susm01, their dth_evento_indicador must be EQUAL
-- (susm01 inherits the distribution event time from cnm01).

SELECT
    count(*) AS n_in_both,
    sum(CASE WHEN cnm.dth_evento_indicador = sus.dth_evento_indicador THEN 1 ELSE 0 END) AS n_dth_match,
    sum(CASE WHEN cnm.dth_evento_indicador != sus.dth_evento_indicador THEN 1 ELSE 0 END) AS n_dth_mismatch  -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj cnm
JOIN dw_2inst_prata.tb_2inst_metacnj sus
    ON  cnm.id_processo = sus.id_processo
    AND cnm.meta_ano    = sus.meta_ano
WHERE cnm.meta_ano    = '2026'
  AND cnm.cod_sistema = 2
  AND cnm.variavel    = 'cnm01'
  AND sus.cod_sistema = 2
  AND sus.variavel    = 'susm01';


-- ── 3. cnm01 vs PendJulg01TG dth comparison ─────────────────────────────────
-- PendJulg01TG also inherits the distribution event time from cnm01.

SELECT
    count(*) AS n_in_both,
    sum(CASE WHEN cnm.dth_evento_indicador = pend.dth_evento_indicador THEN 1 ELSE 0 END) AS n_dth_match,
    sum(CASE WHEN cnm.dth_evento_indicador != pend.dth_evento_indicador THEN 1 ELSE 0 END) AS n_dth_mismatch  -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj cnm
JOIN dw_2inst_prata.tb_2inst_metacnj pend
    ON  cnm.id_processo = pend.id_processo
    AND cnm.meta_ano    = pend.meta_ano
WHERE cnm.meta_ano    = '2026'
  AND cnm.cod_sistema = 2
  AND cnm.variavel    = 'cnm01'
  AND pend.cod_sistema = 2
  AND pend.variavel    = 'PendJulg01TG';


-- ── 4. julgadom01 dth must be >= cnm01 dth ───────────────────────────────────
-- A process cannot be judged BEFORE its first distribution.

SELECT count(*) AS n_judged_before_distributed   -- must be 0
FROM dw_2inst_prata.tb_2inst_metacnj julg
JOIN dw_2inst_prata.tb_2inst_metacnj cnm
    ON  julg.id_processo = cnm.id_processo
    AND julg.meta_ano    = cnm.meta_ano
WHERE julg.meta_ano    = '2026'
  AND julg.cod_sistema = 2
  AND julg.variavel    = 'julgadom01'
  AND cnm.cod_sistema  = 2
  AND cnm.variavel     = 'cnm01'
  AND julg.dth_evento_indicador < cnm.dth_evento_indicador;


-- ── 5. desm01 dth must be inside the period ──────────────────────────────────
-- desm01 uses dth_last_susp (when suspension was REMOVED).
-- The removal event must be within [period_start, period_end].

SELECT
    count(*) AS n_total,
    sum(CASE WHEN dth_evento_indicador BETWEEN TIMESTAMP '2026-01-01 00:00:00'
                                           AND TIMESTAMP '2026-12-31 23:59:59'
             THEN 1 ELSE 0 END)        AS n_in_period,
    sum(CASE WHEN dth_evento_indicador < TIMESTAMP '2026-01-01 00:00:00' THEN 1 ELSE 0 END) AS n_before_period,
    sum(CASE WHEN dth_evento_indicador > TIMESTAMP '2026-12-31 23:59:59' THEN 1 ELSE 0 END) AS n_after_period
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'desm01';
