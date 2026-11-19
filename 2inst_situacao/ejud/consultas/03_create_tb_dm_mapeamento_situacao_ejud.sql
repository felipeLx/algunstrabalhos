-- ============================================================
-- CRIAR tabela dimensão: dw_2inst_prata.tb_dm_mapeamento_situacao_ejud
--
-- Mapeia códigos de situação para seus códigos de movimento CNJ
-- com descrições das tabelas ejud.fase e ejud.complemento.
--
-- Fonte: situacoes.json (broadcast no Spark como mapping_df)
-- Enriquecimento: ejud.fase, ejud.complemento (3x para cada nível)
--
-- NOTA: A criação real é feita via Spark (create_dim_mapeamento.py)
-- porque o situacoes.json é carregado do S3 como DataFrame.
-- As queries abaixo servem para consulta e validação no Athena.
-- ============================================================


-- ── Opção A: Query Athena para visualizar o mapeamento ─────────────────────
-- Assume que a tabela já foi criada pelo script Spark.
-- O join com integracaofasecompl usa códigos CNJ (cod_mni_movimento),
-- NÃO o codfase interno — por isso não precisa de faseexterna aqui.
SELECT DISTINCT
    m.situacao,
    m.explanation,
    m.cod_mni_movimento,
    m.complemento1,
    m.complemento2,
    m.complemento3,
    i.codfasenivel2         AS codfase,
    f.descr                 AS descr_fase,
    i.codcompl1nivel3       AS codcompl1,
    c1.descr                AS descr_compl1,
    i.codcompl2nivel4       AS codcompl2,
    c2.descr                AS descr_compl2,
    i.codcompl3nivel5       AS codcompl3,
    c3.descr                AS descr_compl3
FROM dw_2inst_prata.tb_dm_mapeamento_situacao_ejud_raw m
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) = m.cod_mni_movimento
   AND i.indativo = 'S'
   AND (m.complemento1 IS NULL OR i.codcompl2nivel4cnj = m.complemento1)
   AND (m.complemento2 IS NULL OR i.codcompl3nivel5cnj = m.complemento2)
LEFT JOIN ejud.fase f
    ON i.codfasenivel2 = f.codfase
LEFT JOIN ejud.complemento c1
    ON i.codcompl1nivel3 = c1.codcompl
LEFT JOIN ejud.complemento c2
    ON i.codcompl2nivel4 = c2.codcompl
LEFT JOIN ejud.complemento c3
    ON i.codcompl3nivel5 = c3.codcompl
ORDER BY m.situacao, m.cod_mni_movimento;


-- ── Opção B: Script Spark para criar como tabela Iceberg ───────────────────
-- Ver: ejud/scripts/create_dim_mapeamento.py
