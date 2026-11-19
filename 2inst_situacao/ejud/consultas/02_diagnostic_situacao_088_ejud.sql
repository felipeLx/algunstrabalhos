-- ============================================================
-- DIAGNÓSTICO — Situação 088 (Concluso para julgamento) — EJUD
--
-- A situação 088 NÃO é mapeada de movimentos no situacoes.json.
-- Ela é GERADA pelo process_special_situacoes() no script.
-- EJUD particiona por coddoc (eproc usa id_processo).
--
-- Lógica (reproduz exatamente o script):
--   1. Buscar linhas onde situacao IN start_with
--   2. Para cada linha, verificar próxima linha por coddoc (lead por dthrmov)
--   3. Se próxima situacao IN (start_with ∪ end_with) → dth_fim = prox.dthrmov
--      senão dth_fim = NULL
--   4. Filtrar: manter apenas linhas onde dth_fim IS NULL OU dth_fim > dthrmov
--   5. Forçar situacao = 88 em todas as linhas mantidas
--
-- Códigos start_with e end_with vêm de:
--   ejud/json/situacoes_start_end.json → { "situacao": 88, ... }
-- ============================================================


-- ── 1. Verificar se linhas da situação 088 existem na saída ────────────────
SELECT situacao, count(*) AS n_linhas,
       sum(CASE WHEN dth_fim IS NOT NULL THEN 1 ELSE 0 END) AS n_fechadas,
       sum(CASE WHEN dth_fim IS NULL     THEN 1 ELSE 0 END) AS n_abertas
FROM dw_2inst_ouro.tb_tf_situacao_ejud
WHERE situacao = 88
  AND year_month = '2026-01'
GROUP BY situacao;


-- ── 2. Simular process_special_situacoes() para 088 ───────────────────────
-- start_with e all_ints vêm de:
--   ejud/json/situacoes_start_end.json → situação 88
--
-- start_with: [159, 81, 160, 90, 18, 9, 82, 24, 26, 65, 62, 27, 28, 29,
--              138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102,
--              104, 137, 91, 72, 37, 131, 120, 119, 61, 40, 130, 118, 153, 129]
WITH base AS (
    SELECT
        coddoc,
        dthrmov,
        situacao,
        LEAD(situacao)   OVER (PARTITION BY coddoc ORDER BY dthrmov) AS prox_situacao,
        LEAD(dthrmov)    OVER (PARTITION BY coddoc ORDER BY dthrmov) AS prox_dth
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE YEAR(dthrmov) = 2026
      AND MONTH(dthrmov) = 1
),
candidatos AS (
    SELECT
        coddoc,
        dthrmov,
        situacao,
        prox_situacao,
        prox_dth,
        -- ↓ all_ints: start_with ∪ end_with do situacoes_start_end.json para 088
        CASE
            WHEN prox_situacao IN (
                159, 81, 160, 90, 18, 9, 82, 24, 26, 65, 62, 27, 28, 29,
                138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102,
                104, 137, 91, 72, 37, 131, 120, 119, 61, 40, 130, 118, 153, 129,
                2, 10, 23, 155
            )
            THEN prox_dth
            ELSE NULL
        END AS dth_fim_calculado
    FROM base
    -- ↓ filtro start_with do situacoes_start_end.json para 088
    WHERE situacao IN (
        159, 81, 160, 90, 18, 9, 82, 24, 26, 65, 62, 27, 28, 29,
        138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102,
        104, 137, 91, 72, 37, 131, 120, 119, 61, 40, 130, 118, 153, 129
    )
)
SELECT
    coddoc,
    dthrmov,
    situacao             AS situacao_gatilho,
    88                   AS situacao_gerada,
    prox_situacao        AS situacao_fechamento,
    prox_dth,
    dth_fim_calculado,
    -- script mantém linha apenas quando dth_fim NULL ou dth_fim > dthrmov
    CASE
        WHEN dth_fim_calculado IS NULL OR dth_fim_calculado > dthrmov THEN 'MANTER'
        ELSE 'DESCARTAR'
    END AS decisao
FROM candidatos
ORDER BY coddoc, dthrmov
LIMIT 50;


-- ── 3. Conferência cruzada: contagem simulada vs escrita ──────────────────
WITH base AS (
    SELECT
        coddoc,
        dthrmov,
        situacao,
        LEAD(situacao)   OVER (PARTITION BY coddoc ORDER BY dthrmov) AS prox_situacao,
        LEAD(dthrmov)    OVER (PARTITION BY coddoc ORDER BY dthrmov) AS prox_dth
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE YEAR(dthrmov) = 2026
      AND MONTH(dthrmov) = 1
),
simulado AS (
    SELECT
        coddoc,
        dthrmov,
        CASE
            WHEN prox_situacao IN (
                159, 81, 160, 90, 18, 9, 82, 24, 26, 65, 62, 27, 28, 29,
                138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102,
                104, 137, 91, 72, 37, 131, 120, 119, 61, 40, 130, 118, 153, 129,
                2, 10, 23, 155
            )
            THEN prox_dth
            ELSE NULL
        END AS dth_fim_calculado
    FROM base
    WHERE situacao IN (
        159, 81, 160, 90, 18, 9, 82, 24, 26, 65, 62, 27, 28, 29,
        138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102,
        104, 137, 91, 72, 37, 131, 120, 119, 61, 40, 130, 118, 153, 129
    )
),
mantidos AS (
    SELECT count(*) AS n_simulado
    FROM simulado
    WHERE dth_fim_calculado IS NULL OR dth_fim_calculado > dthrmov
),
escritos AS (
    SELECT count(*) AS n_escrito
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao = 88
      AND year_month = '2026-01'
)
SELECT k.n_simulado, w.n_escrito,
       k.n_simulado - w.n_escrito AS diferenca
FROM mantidos k, escritos w;
