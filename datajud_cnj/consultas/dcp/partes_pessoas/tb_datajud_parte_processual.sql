-- PENDENTE: gerada a partir de docs/der-processojudicial-xsd.md.
-- Origem prevista: dcp. Validar mapeamento e joins antes de promover o arquivo.
select
       CAST(NULL AS DECIMAL(20, 0)) AS nr_processo,
       CAST(NULL AS INT) AS cd_sist_origem,
       CAST(NULL AS INT) AS cd_tb_origem,
       CAST(NULL AS STRING) AS cd_seq_tb_origem,
       CAST(NULL AS STRING) AS tipo_polo,
       CAST(NULL AS BOOLEAN) AS assistencia_judiciaria,
       CAST(NULL AS DATE) AS assistencia_deferimento,
       CAST(NULL AS DATE) AS assistencia_revogacao,
       CAST(NULL AS INT) AS intimacao_pendente,
       CAST(NULL AS STRING) AS relacionamento_processual,
       CAST(NULL AS STRING) AS interesse_publico
 where 1 = 0;
