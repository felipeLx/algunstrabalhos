-- ============================================================
-- DIAGNÓSTICO — Situação 004 (Arquivado provisoriamente) — EJUD
--
-- Exemplo do padrão end_with: inicia com sit 004,
-- fecha quando a próxima situação do processo está na lista end_with.
--
-- De situacoes.json → situação 4:
--   cod_mni_movimento IN (245, 14997, 14998)   ← sem filtro de complemento
--
-- De situacoes_start_end.json → situação 4:
--   start_with: [4]
--   end_with:   [2, 4, 159, 10, 81, 160, 90, 18, 9, 82, 23, 24, 26,
--                65, 62, 27, 28, 29, 91, 72, 37, 154, 41, 132, 129]
--
-- dth_fim = dthrmov do PRÓXIMO evento de situação por coddoc
-- quando essa próxima situação está na lista end_with; senão NULL (ainda aberta).
--
-- Resolução de movimentos EJUD:
--   movimento.codfase → faseexterna.codfaseext → integracaofasecompl_ejud_cnj.codfasenivel2
--   cod_mni_movimento_int = COALESCE(codcompl1nivel3cnj, codfasenivel2cnj)
-- ============================================================


-- ── 1. Movimentos brutos que mapeiam para a situação 004 ───────────────────
-- Valores de cod_mni_movimento vêm de:
--   ejud/json/situacoes.json → { "situacao": 4, "movimentos": [...] }
SELECT
    m.coddoc,
    m.dthrmov              AS dth_inicio,
    m.codfase,
    fe.codfaseext,
    i.codfasenivel2cnj,
    i.codcompl1nivel3cnj,
    COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) AS cod_mni_movimento_int,
    tp.codextcnj           AS cod_classe
FROM ejud.movimento m
LEFT JOIN ejud.faseexterna fe
    ON m.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
   AND (i.ultimonivelcnj < 4 OR m.codcompl2 = i.codcompl2nivel4)
   AND (i.ultimonivelcnj < 5 OR m.codcompl3 = i.codcompl3nivel5)
LEFT JOIN ejud.t_processo p ON m.coddoc = p.coddoc
LEFT JOIN ejud.tipoprocesso tp ON p.codtipproc = tp.codtipproc
WHERE m.coddoc IS NOT NULL
  -- ↓ cod_mni_movimento do situacoes.json para a situação 4
  AND COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) IN (245, 14997, 14998)
  AND YEAR(m.dthrmov) = 2026
  AND MONTH(m.dthrmov) = 1
ORDER BY m.dthrmov
LIMIT 50;


-- ── 2. Simular compute_dth_fim para a situação 004 ────────────────────────
-- Reproduz a lógica de broadcast-join do compute_dth_fim().
-- Códigos end_with vêm de:
--   ejud/json/situacoes_start_end.json → { "situacao": 4, "end_with": [...] }
WITH sit4_rows AS (
    SELECT coddoc, dthrmov, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao = 4
      AND year_month = '2026-01'
),
todas_sits AS (
    SELECT coddoc, dthrmov, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE YEAR(dthrmov) = 2026
),
com_proximo AS (
    SELECT
        s.coddoc,
        s.dthrmov,
        s.situacao,
        LEAD(a.situacao)   OVER (PARTITION BY s.coddoc ORDER BY a.dthrmov) AS prox_situacao,
        LEAD(a.dthrmov)    OVER (PARTITION BY s.coddoc ORDER BY a.dthrmov) AS prox_dth
    FROM sit4_rows s
    JOIN todas_sits a ON s.coddoc = a.coddoc
    WHERE a.dthrmov >= s.dthrmov
)
SELECT
    coddoc,
    dthrmov,
    situacao,
    prox_situacao,
    prox_dth,
    -- ↓ lista end_with do situacoes_start_end.json para a situação 4
    CASE
        WHEN prox_situacao IN (2, 4, 159, 10, 81, 160, 90, 18, 9, 82,
                               23, 24, 26, 65, 62, 27, 28, 29, 91, 72,
                               37, 154, 41, 132, 129)
        THEN prox_dth
        ELSE NULL
    END AS dth_fim_esperado
FROM com_proximo
ORDER BY coddoc, dthrmov
LIMIT 50;


-- ── 3. Verificar linhas escritas no Iceberg ────────────────────────────────
SELECT situacao, count(*) AS n_linhas,
       sum(CASE WHEN dth_fim IS NOT NULL THEN 1 ELSE 0 END) AS n_fechadas,
       sum(CASE WHEN dth_fim IS NULL     THEN 1 ELSE 0 END) AS n_abertas
FROM dw_2inst_ouro.tb_tf_situacao_ejud
WHERE situacao = 4
  AND year_month = '2026-01'
GROUP BY situacao;


-- ── 4. Verificar schema ───────────────────────────────────────────────────
DESCRIBE dw_2inst_ouro.tb_tf_situacao_ejud;
