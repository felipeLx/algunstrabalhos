-- ============================================================
-- DIAGNOSTIC — Situacao 024 (Distribuído) — EPROC
--
-- Simulates what eproc_situacao_emr.py does for situacao 024.
-- Test with the movement codes from situacoes.json:
--   situacao 24 → cod_mni_movimento: 26
--
-- situacoes_start_end.json defines 024 as instantaneous:
--   start_with: [24], end_with: [24] → dth_fim = dth_inicio
-- ============================================================


-- ── 1. Raw movement events matching situacao 024 ────────────────────────────
-- The cod_mni_movimento value(s) below come from:
--   eproc2g/json/situacoes.json → { "situacao": 24, "movimentos": [...] }
--
-- For situacao 024 the JSON defines:
--   cod_mni_movimento = 26  (no complemento filters)
SELECT
    pe.id_processo,
    pe.id_processo_evento,
    p.num_processo,
    pe.dth_evento        AS dth_inicio,
    ej.cod_mni_movimento,
    ejct.cod_mni_complemento_tabelado,
    mct.cod_mni_complemento
FROM eproc2g.processo_evento pe
JOIN eproc2g.evento_judicial ej
    ON pe.id_evento_judicial = ej.id_evento_judicial
LEFT JOIN eproc2g.evento_judicial_mni_complemento_tabelado ejct
    ON ej.id_evento_judicial = ejct.id_evento_judicial
LEFT JOIN eproc2g.mni_complemento_tabelado mct
    ON ejct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
JOIN eproc2g.processo p
    ON pe.id_processo = p.id_processo
WHERE pe.dth_desativacao IS NULL
  AND pe.id_evento_judicial != 40400400
  -- ↓ cod_mni_movimento from situacoes.json for situacao 24
  AND ej.cod_mni_movimento = 26
  AND YEAR(pe.dth_evento) = 2026
  AND MONTH(pe.dth_evento) = 1
ORDER BY pe.dth_evento
LIMIT 50;


-- ── 2. Verify mapped result in Iceberg output table ────────────────────────
-- After running the script, situacao=24 rows should exist here.
SELECT situacao, count(*) AS n_rows
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE situacao = 24
  AND year_month = '2026-01'
GROUP BY situacao;


-- ── 3. Spot-check: compare raw events vs written rows ──────────────────────
-- Pick a sample processo and verify 1:1 match between raw events
-- and the Iceberg output for situacao 024.
WITH raw_events AS (
    SELECT
        pe.id_processo,
        pe.id_processo_evento,
        pe.dth_evento AS dth_inicio
    FROM eproc2g.processo_evento pe
    JOIN eproc2g.evento_judicial ej
        ON pe.id_evento_judicial = ej.id_evento_judicial
    WHERE pe.dth_desativacao IS NULL
      AND pe.id_evento_judicial != 40400400
      AND ej.cod_mni_movimento = 26
      AND YEAR(pe.dth_evento) = 2026
      AND MONTH(pe.dth_evento) = 1
),
written AS (
    SELECT id_processo, dth_inicio, dth_fim, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_eproc
    WHERE situacao = 24
      AND year_month = '2026-01'
)
SELECT
    r.id_processo,
    r.dth_inicio,
    w.dth_fim,
    w.situacao,
    CASE WHEN w.id_processo IS NOT NULL THEN 'MATCH' ELSE 'MISSING' END AS status
FROM raw_events r
LEFT JOIN written w
    ON r.id_processo = CAST(w.id_processo AS BIGINT)
   AND r.dth_inicio = w.dth_inicio
ORDER BY r.id_processo, r.dth_inicio
LIMIT 50;


-- ── 4. dth_fim validation for 024 (instantaneous) ──────────────────────────
-- situacoes_start_end.json: start_with=[24], end_with=[24]
-- → instantaneous: dth_fim MUST equal dth_inicio for every row.
SELECT
    count(*)                                                  AS n_total,
    sum(CASE WHEN dth_fim = dth_inicio THEN 1 ELSE 0 END)    AS n_instantaneous,
    sum(CASE WHEN dth_fim != dth_inicio THEN 1 ELSE 0 END)   AS n_wrong_dth_fim,
    sum(CASE WHEN dth_fim IS NULL THEN 1 ELSE 0 END)          AS n_null_dth_fim
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE situacao = 24
  AND year_month = '2026-01';
