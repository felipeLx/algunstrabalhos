-- ============================================================
-- DIAGNOSTIC — Situacao 004 (Arquivado provisoriamente) — EPROC
--
-- Example of end_with pattern: starts with sit 004,
-- closes when next situacao per processo is in end_with list.
--
-- From situacoes.json → situacao 4:
--   cod_mni_movimento IN (245, 14997, 14998)   ← no complemento filters
--
-- From situacoes_start_end.json → situacao 4:
--   start_with: [4]
--   end_with:   [2, 4, 159, 10, 81, 160, 90, 18, 9, 82, 23, 24, 26,
--                65, 62, 27, 28, 29, 91, 72, 37, 154, 41, 132, 129]
--
-- dth_fim = dth_inicio of the NEXT situacao event per processo
-- when that next situacao IN end_with list; else NULL (still open).
-- ============================================================


-- ── 1. Raw movement events mapping to situacao 004 ─────────────────────────
-- cod_mni_movimento values come from:
--   eproc2g/json/situacoes.json → { "situacao": 4, "movimentos": [...] }
SELECT
    pe.id_processo,
    p.num_processo,
    pe.dth_evento             AS dth_inicio,
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
  -- ↓ cod_mni_movimento from situacoes.json for situacao 4
  AND ej.cod_mni_movimento IN (245, 14997, 14998)
  AND YEAR(pe.dth_evento) = 2026
  AND MONTH(pe.dth_evento) = 1
ORDER BY pe.dth_evento
LIMIT 50;


-- ── 2. Simulate compute_dth_fim for situacao 004 ───────────────────────────
-- Mirrors the end_with broadcast-join logic in compute_dth_fim().
-- For each sit=4 row, look at next situacao per id_processo (lead).
-- If next situacao IN end_with → dth_fim = next.dth_inicio; else NULL.
--
-- end_with codes come from:
--   eproc2g/json/situacoes_start_end.json → { "situacao": 4, "end_with": [...] }
WITH sit4_rows AS (
    SELECT id_processo, dth_inicio, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_eproc
    WHERE situacao = 4
      AND year_month = '2026-01'
),
all_sits AS (
    SELECT id_processo, dth_inicio, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_eproc
    WHERE YEAR(dth_inicio) = 2026
),
with_next AS (
    SELECT
        s.id_processo,
        s.dth_inicio,
        s.situacao,
        LEAD(a.situacao)   OVER (PARTITION BY s.id_processo ORDER BY a.dth_inicio) AS next_situacao,
        LEAD(a.dth_inicio) OVER (PARTITION BY s.id_processo ORDER BY a.dth_inicio) AS next_dth
    FROM sit4_rows s
    JOIN all_sits a ON s.id_processo = a.id_processo
    WHERE a.dth_inicio >= s.dth_inicio
)
SELECT
    id_processo,
    dth_inicio,
    situacao,
    next_situacao,
    next_dth,
    -- ↓ end_with list from situacoes_start_end.json for situacao 4
    CASE
        WHEN next_situacao IN (2, 4, 159, 10, 81, 160, 90, 18, 9, 82,
                               23, 24, 26, 65, 62, 27, 28, 29, 91, 72,
                               37, 154, 41, 132, 129)
        THEN next_dth
        ELSE NULL
    END AS dth_fim_expected
FROM with_next
ORDER BY id_processo, dth_inicio
LIMIT 50;


-- ── 3. Verify written rows in Iceberg ──────────────────────────────────────
SELECT situacao, count(*) AS n_rows,
       sum(CASE WHEN dth_fim IS NOT NULL THEN 1 ELSE 0 END) AS n_closed,
       sum(CASE WHEN dth_fim IS NULL     THEN 1 ELSE 0 END) AS n_open
FROM dw_2inst_ouro.tb_tf_situacao_eproc
WHERE situacao = 4
  AND year_month = '2026-01'
GROUP BY situacao;


-- ── 4. Schema check ────────────────────────────────────────────────────────
DESCRIBE dw_2inst_ouro.tb_tf_situacao_eproc;
