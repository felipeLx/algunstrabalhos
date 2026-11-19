-- PENDENTE: gerada a partir de docs/der-processojudicial-xsd.md.
-- Origem prevista: pje. Validar mapeamento e joins antes de promover o arquivo.
select
       CAST(NULL AS STRING) AS id_documento,
       CAST(NULL AS DECIMAL(20, 0)) AS nr_processo,
       CAST(NULL AS STRING) AS id_documento_vinculado,
       CAST(NULL AS STRING) AS tipo_documento,
       CAST(NULL AS TIMESTAMP) AS data_hora,
       CAST(NULL AS STRING) AS mimetype,
       CAST(NULL AS INT) AS nivel_sigilo,
       CAST(NULL AS INT) AS evento,
       CAST(NULL AS STRING) AS hash,
       CAST(NULL AS STRING) AS descricao,
       CAST(NULL AS STRING) AS tipo_documento_local,
       CAST(NULL AS BINARY) AS conteudo
 where 1 = 0;
