-- ============================================================
-- EJUD — Comparison: Linux ouro vs Iceberg prata (staging)
-- tb_ft_meta_analitico (ouro/production) vs tb_2inst_metacnj (prata/test)
-- Note: meta_ano is INT in ouro, STRING in prata
-- ============================================================

-- ── 1. Summary counts per cod_indicador in each table ───────────────────────

-- Iceberg prata counts
SELECT cod_indicador, count(*) AS n_prata
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cod_sistema = 1   -- ejud only
GROUP BY cod_indicador
ORDER BY cod_indicador;

-- Linux ouro counts
SELECT cod_indicador, count(*) AS n_ouro
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = 2026
  AND cod_indicador NOT IN (1, 2, 3, 4, 5)  -- skip Meta 01 (eproc2g shared)
GROUP BY cod_indicador
ORDER BY cod_indicador;

-- ── 2. In Linux ouro but NOT in Iceberg prata ───────────────────────────────
-- These are processes that Linux has but Iceberg is missing.
-- Expected: 0 after all fixes are applied.

SELECT
    m.cod_indicador,
    count(m.id_processo) AS n
FROM dw_2inst_ouro.tb_ft_meta_analitico m
LEFT JOIN dw_2inst_prata.tb_2inst_metacnj mm
    ON  m.id_processo          = mm.id_processo
    AND cast(m.meta_ano AS varchar) = mm.meta_ano
    AND m.cod_indicador        = mm.cod_indicador
WHERE m.meta_ano = 2026
  AND m.cod_indicador NOT IN (1, 2, 3, 4, 5)
  AND mm.id_processo IS NULL
GROUP BY m.cod_indicador
ORDER BY m.cod_indicador;

-- ── 3. In Iceberg prata but NOT in Linux ouro ───────────────────────────────
-- These are processes that Iceberg has but Linux doesn't.
-- Expected: small or 0 (Iceberg may correctly include processes Linux missed).

SELECT
    mm.cod_indicador,
    count(mm.id_processo) AS n
FROM dw_2inst_prata.tb_2inst_metacnj mm
LEFT JOIN dw_2inst_ouro.tb_ft_meta_analitico m
    ON  mm.id_processo  = m.id_processo
    AND mm.meta_ano     = cast(m.meta_ano AS varchar)
    AND mm.cod_indicador = m.cod_indicador
WHERE mm.meta_ano = '2026'
  AND mm.cod_indicador NOT IN (1, 2, 3, 4, 5)
  AND m.id_processo IS NULL
GROUP BY mm.cod_indicador
ORDER BY mm.cod_indicador;

-- ── 4. Full diff side-by-side ────────────────────────────────────────────────
-- Shows n_ouro, n_prata, and difference for every cod_indicador.

SELECT
    COALESCE(o.cod_indicador, p.cod_indicador) AS cod_indicador,
    COALESCE(o.n_ouro,  0) AS n_ouro,
    COALESCE(p.n_prata, 0) AS n_prata,
    COALESCE(o.n_ouro, 0) - COALESCE(p.n_prata, 0) AS diff
FROM (
    SELECT cod_indicador, count(*) AS n_ouro
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = 2026
      AND cod_indicador NOT IN (1, 2, 3, 4, 5)
    GROUP BY cod_indicador
) o
FULL OUTER JOIN (
    SELECT cod_indicador, count(*) AS n_prata
    FROM dw_2inst_prata.tb_2inst_metacnj
    WHERE meta_ano = '2026'
      AND cod_indicador NOT IN (1, 2, 3, 4, 5)
    GROUP BY cod_indicador
) p ON o.cod_indicador = p.cod_indicador
ORDER BY cod_indicador;
