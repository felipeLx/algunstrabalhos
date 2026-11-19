-- PENDENTE: gerada a partir de docs/der-processojudicial-xsd.md.
-- Origem prevista: dcp. Validar mapeamento e joins antes de promover o arquivo.
select
       CAST(NULL AS INT) AS cd_sist_origem,
       CAST(NULL AS INT) AS cd_tb_origem,
       CAST(NULL AS STRING) AS cd_seq_tb_origem,
       CAST(NULL AS STRING) AS nome,
       CAST(NULL AS STRING) AS sexo,
       CAST(NULL AS STRING) AS nome_genitor,
       CAST(NULL AS STRING) AS nome_genitora,
       CAST(NULL AS DATE) AS data_nascimento,
       CAST(NULL AS DATE) AS data_obito,
       CAST(NULL AS STRING) AS numero_documento_principal,
       CAST(NULL AS STRING) AS tipo_pessoa,
       CAST(NULL AS STRING) AS raca_cor,
       CAST(NULL AS STRING) AS cidade_natural,
       CAST(NULL AS STRING) AS estado_natural,
       CAST(NULL AS STRING) AS nacionalidade,
       CAST(NULL AS STRING) AS email,
       CAST(NULL AS BOOLEAN) AS populacao_de_rua
 where 1 = 0;
