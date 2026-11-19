-- ============================================================
-- EPROC2G — Spot-check individual processes in prata
-- Use to verify a known process end-to-end:
--   1. Confirm it is in the situacao source
--   2. Confirm it appears in the correct variable(s)
--   3. Verify enriched columns match the bronze tables
-- Replace '???' with a real id_processo from your test dataset.
-- ============================================================

-- ── 0. Find candidate processes to spot-check ────────────────────────────────
-- Pick 5 recent cnm01 processes to use in the checks below.

SELECT
    id_processo,
    num_processo,
    dth_evento_indicador,
    cod_classe_cnj,
    id_assunto_principal,
    cod_competencia,
    cod_natureza,
    cod_ult_oj
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'cnm01'
ORDER BY dth_evento_indicador DESC
LIMIT 10;


-- ── 1. All variables for a single id_processo ────────────────────────────────
-- Replace '???' with the id_processo from step 0 above.
-- Should show which variables the process appears in and its event times.

SELECT
    variavel,
    cod_indicador,
    dth_evento_indicador,
    cod_classe_cnj,
    id_assunto_principal,
    cod_competencia,
    cod_natureza,
    cod_ult_oj,
    id_sigilo
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND id_processo = '???'
ORDER BY cod_indicador;


-- ── 2. Cross-check against situacao source ───────────────────────────────────
-- For the same id_processo, verify the distribution event in situacao.
-- The dth_inicio from situacao should match dth_evento_indicador in prata (cnm01).

SELECT
    id_processo,
    situacao,
    dth_inicio,
    cod_mni_movimento,
    id_orgao_colegiado,
    id_classe_judicial,
    id_assunto_principal AS assunto_raw_int  -- raw internal ID before enrichment
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE id_processo = '???'
  AND situacao IN (61, 24, 9, 65, 81)       -- distribution codes
ORDER BY dth_inicio;


-- ── 3. Verify id_assunto_principal enrichment via bronze ─────────────────────
-- The prata row should have cod_mni_assunto_judicial, not the raw internal ID.
-- Compare prata value vs bronze resolution.

SELECT
    a.id_assunto_judicial       AS id_assunto_interno,
    a.cod_mni_assunto_judicial  AS cod_cnj_esperado
FROM eproc2g.assunto_judicial a
WHERE a.id_assunto_judicial = (
    SELECT DISTINCT id_assunto_principal   -- raw ID from situacao source
    FROM dw_2inst_ouro.tb_tf_situacao_eproc
    WHERE id_processo = '???'
    LIMIT 1
);
-- Expected: prata.id_assunto_principal == cod_mni_assunto_judicial above


-- ── 4. Verify cod_natureza enrichment ────────────────────────────────────────
-- Look up the competencia for the process and confirm sin_civel / sin_criminal.

SELECT
    p.id_processo,
    p.cod_competencia,
    cj.sin_civel,
    cj.sin_criminal,
    CASE
        WHEN cj.sin_civel     = 'S' THEN 1
        WHEN cj.sin_criminal  = 'S' THEN 2
        ELSE NULL
    END AS cod_natureza_esperado
FROM eproc2g.processo p
JOIN eproc2g.competencia_judicial cj
    ON cj.cod_competencia = p.cod_competencia
WHERE p.id_processo = '???';
-- Expected: cod_natureza_esperado == prata.cod_natureza for this process


-- ── 5. Validate a julgadom01 process has judgment event in situacao ───────────
-- Pick a process from julgadom01 and confirm the judgment situacao exists.

SELECT
    id_processo,
    dth_evento_indicador AS dth_julgamento_prata
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano    = '2026'
  AND cd_meta     = '01'
  AND cod_sistema = 2
  AND variavel    = 'julgadom01'
ORDER BY dth_evento_indicador DESC
LIMIT 5;

-- For each id_processo above, confirm in situacao:
SELECT
    id_processo,
    situacao,
    dth_inicio
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE id_processo = '???'
  AND situacao IN (27, 28, 62, 18, 29, 90, 129,   -- primeirasentm
                   2, 10, 23, 41, 134, 154, 132,   -- baixm
                   17, 34)                          -- mpum
  AND dth_inicio BETWEEN TIMESTAMP '2026-01-01 00:00:00'
                     AND TIMESTAMP '2026-12-31 23:59:59'
ORDER BY dth_inicio;
