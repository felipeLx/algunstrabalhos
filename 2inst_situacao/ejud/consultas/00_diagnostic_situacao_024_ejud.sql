-- ============================================================
-- DIAGNÓSTICO — Situação 024 (Distribuído) — EJUD
--
-- Simula o que o ejud_situacao_emr.py faz para a situação 024.
-- Códigos de movimento vindos do situacoes.json:
--   situacao 24 → cod_mni_movimento: 26
--
-- situacoes_start_end.json define 024 como instantânea:
--   start_with: [24], end_with: [24] → dth_fim = dth_inicio
--
-- NOTA: O script Spark também implementa fallback de dois passos:
-- se o código CNJ específico (codcompl1nivel3cnj) não tem mapeamento
-- em situacoes.json, ele recai para o código base (codfasenivel2cnj).
-- Este fallback não aparece aqui pois a sit 24 mapeia um código
-- simples (26) sem complementos. Ver ejud_situacao_emr.py.
-- ============================================================


-- ── 1. Movimentos brutos que mapeiam para a situação 024 ───────────────────
-- Valores de cod_mni_movimento vêm de:
--   ejud/json/situacoes.json → { "situacao": 24, "movimentos": [...] }
--
-- Para a situação 024, o JSON define:
--   cod_mni_movimento = 26  (sem filtro de complemento)
--
-- Caminho de resolução EJUD:
--   movimento.codfase → faseexterna.codfaseext → integracaofasecompl_ejud_cnj.codfasenivel2
--   O código CNJ resultante (cod_mni_movimento_int) deve ser igual a 26.
SELECT
    m.coddoc,
    m.dthrmov          AS dth_inicio,
    m.codfase,
    fe.codfaseext,
    i.codfasenivel2cnj,
    i.codcompl1nivel3cnj,
    COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) AS cod_mni_movimento_int,
    tp.codextcnj       AS cod_classe
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
  -- ↓ cod_mni_movimento do situacoes.json para a situação 24
  AND COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) = 26
  AND YEAR(m.dthrmov) = 2026
  AND MONTH(m.dthrmov) = 1
ORDER BY m.dthrmov
LIMIT 50;


-- ── 2. Verificar resultado mapeado na tabela Iceberg ───────────────────────
-- Após rodar o script, linhas com situacao=24 devem existir aqui.
SELECT situacao, count(*) AS n_rows
FROM dw_2inst_ouro.tb_tf_situacao_ejud
WHERE situacao = 24
  AND year_month = '2026-01'
GROUP BY situacao;


-- ── 3. Conferência: comparar eventos brutos vs linhas escritas ─────────────
WITH eventos_brutos AS (
    SELECT
        m.coddoc,
        m.dthrmov AS dth_inicio
    FROM ejud.movimento m
    LEFT JOIN ejud.faseexterna fe
        ON m.codfase = fe.codfase
    LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
        ON fe.codfaseext = i.codfasenivel2
       AND i.indativo = 'S'
       AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
       AND (i.ultimonivelcnj < 4 OR m.codcompl2 = i.codcompl2nivel4)
       AND (i.ultimonivelcnj < 5 OR m.codcompl3 = i.codcompl3nivel5)
    WHERE m.coddoc IS NOT NULL
      AND COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) = 26
      AND YEAR(m.dthrmov) = 2026
      AND MONTH(m.dthrmov) = 1
),
escritos AS (
    SELECT coddoc, dthrmov, dth_fim, situacao
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao = 24
      AND year_month = '2026-01'
)
SELECT
    r.coddoc,
    r.dth_inicio,
    w.dth_fim,
    w.situacao,
    CASE WHEN w.coddoc IS NOT NULL THEN 'OK' ELSE 'FALTANDO' END AS status
FROM eventos_brutos r
LEFT JOIN escritos w
    ON r.coddoc = w.coddoc
   AND r.dth_inicio = w.dthrmov
ORDER BY r.coddoc, r.dth_inicio
LIMIT 50;


-- ── 4. Validação de dth_fim para 024 (instantânea) ────────────────────────
-- situacoes_start_end.json: start_with=[24], end_with=[24]
-- → instantânea: dth_fim DEVE ser igual a dth_inicio em todas as linhas.
SELECT
    count(*)                                                  AS n_total,
    sum(CASE WHEN dth_fim = dthrmov THEN 1 ELSE 0 END)       AS n_instantanea,
    sum(CASE WHEN dth_fim != dthrmov THEN 1 ELSE 0 END)      AS n_dth_fim_errado,
    sum(CASE WHEN dth_fim IS NULL THEN 1 ELSE 0 END)          AS n_dth_fim_nulo
FROM dw_2inst_ouro.tb_tf_situacao_ejud
WHERE situacao = 24
  AND year_month = '2026-01';
