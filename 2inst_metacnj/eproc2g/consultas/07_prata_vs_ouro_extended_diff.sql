-- ============================================================
-- EPROC2G — Extended diff: Linux ouro vs Iceberg prata (cod_sistema=2)
-- Extends 01_diff_ouro_vs_prata.sql with per-variable process lists
-- to identify which specific processes differ between systems.
-- ============================================================

-- ── 1. Per-variable count summary with % difference ──────────────────────────

SELECT
    COALESCE(o.variavel, p.variavel) AS variavel,
    COALESCE(o.n_ouro,  0)           AS n_ouro,
    COALESCE(p.n_prata, 0)           AS n_prata,
    COALESCE(o.n_ouro,  0) - COALESCE(p.n_prata, 0) AS diff_abs,
    round(
        100.0 * (COALESCE(o.n_ouro, 0) - COALESCE(p.n_prata, 0))
               / NULLIF(COALESCE(o.n_ouro, 0), 0),
        2
    ) AS diff_pct_ouro_base
FROM (
    SELECT variavel, count(*) AS n_ouro
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano   = 2026
      AND cod_sistema = 2
    GROUP BY variavel
) o
FULL OUTER JOIN (
    SELECT variavel, count(*) AS n_prata
    FROM dw_2inst_prata.tb_2inst_metacnj
    WHERE meta_ano   = '2026'
      AND cd_meta    = '01'
      AND cod_sistema = 2
    GROUP BY variavel
) p ON o.variavel = p.variavel
ORDER BY variavel;


-- ── 2. Processes in ouro NOT in prata (cnm01) — first 50 ─────────────────────
-- These are cases the new script is missing. Investigate distribution events.

SELECT
    o.id_processo,
    o.dth_evento_indicador,
    o.cod_classe_cnj,
    o.cod_competencia,
    o.cod_natureza
FROM dw_2inst_ouro.tb_ft_meta_analitico o
LEFT JOIN dw_2inst_prata.tb_2inst_metacnj p
    ON  o.id_processo  = p.id_processo
    AND p.meta_ano     = cast(o.meta_ano AS varchar)
    AND p.variavel     = o.variavel
    AND p.cod_sistema  = 2
WHERE o.meta_ano    = 2026
  AND o.cod_sistema = 2
  AND o.variavel    = 'cnm01'
  AND p.id_processo IS NULL
LIMIT 50;


-- ── 3. Processes in prata NOT in ouro (cnm01) — first 50 ─────────────────────
-- These are cases the new script adds. May be legitimate fixes.

SELECT
    p.id_processo,
    p.dth_evento_indicador,
    p.cod_classe_cnj,
    p.cod_competencia,
    p.cod_natureza
FROM dw_2inst_prata.tb_2inst_metacnj p
LEFT JOIN dw_2inst_ouro.tb_ft_meta_analitico o
    ON  p.id_processo  = o.id_processo
    AND p.meta_ano     = cast(o.meta_ano AS varchar)
    AND p.variavel     = o.variavel
    AND o.cod_sistema  = 2
WHERE p.meta_ano    = '2026'
  AND p.cd_meta     = '01'
  AND p.cod_sistema = 2
  AND p.variavel    = 'cnm01'
  AND o.id_processo IS NULL
LIMIT 50;


-- ── 4. dth_evento_indicador agreement for matched processes (cnm01) ──────────
-- For processes present in BOTH, verify event times are close.
-- Small differences (< 1 day) may be due to timestamp rounding.
-- Large differences indicate the wrong event was picked.

SELECT
    count(*)  AS n_matched,
    sum(CASE WHEN abs(date_diff('day',
                      cast(o.dth_evento_indicador AS date),
                      cast(p.dth_evento_indicador AS date))) = 0
             THEN 1 ELSE 0 END) AS n_exact_match,
    sum(CASE WHEN abs(date_diff('day',
                      cast(o.dth_evento_indicador AS date),
                      cast(p.dth_evento_indicador AS date))) BETWEEN 1 AND 30
             THEN 1 ELSE 0 END) AS n_within_30d,
    sum(CASE WHEN abs(date_diff('day',
                      cast(o.dth_evento_indicador AS date),
                      cast(p.dth_evento_indicador AS date))) > 30
             THEN 1 ELSE 0 END) AS n_large_diff
FROM dw_2inst_ouro.tb_ft_meta_analitico o
JOIN dw_2inst_prata.tb_2inst_metacnj p
    ON  o.id_processo  = p.id_processo
    AND p.meta_ano     = cast(o.meta_ano AS varchar)
    AND p.variavel     = o.variavel
    AND p.cod_sistema  = 2
WHERE o.meta_ano    = 2026
  AND o.cod_sistema = 2
  AND o.variavel    = 'cnm01';


-- ── 5. Sample of large dth_evento_indicador mismatches ───────────────────────
-- Investigate the specific processes with large date differences.

SELECT
    o.id_processo,
    cast(o.dth_evento_indicador AS date)  AS dth_ouro,
    cast(p.dth_evento_indicador AS date)  AS dth_prata,
    date_diff('day',
              cast(o.dth_evento_indicador AS date),
              cast(p.dth_evento_indicador AS date)) AS diff_days
FROM dw_2inst_ouro.tb_ft_meta_analitico o
JOIN dw_2inst_prata.tb_2inst_metacnj p
    ON  o.id_processo  = p.id_processo
    AND p.meta_ano     = cast(o.meta_ano AS varchar)
    AND p.variavel     = o.variavel
    AND p.cod_sistema  = 2
WHERE o.meta_ano    = 2026
  AND o.cod_sistema = 2
  AND o.variavel    = 'cnm01'
  AND abs(date_diff('day',
              cast(o.dth_evento_indicador AS date),
              cast(p.dth_evento_indicador AS date))) > 30
ORDER BY abs(date_diff('day',
              cast(o.dth_evento_indicador AS date),
              cast(p.dth_evento_indicador AS date))) DESC
LIMIT 20;
