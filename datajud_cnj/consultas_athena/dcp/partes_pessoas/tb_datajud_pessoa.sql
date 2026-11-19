-- PENDENTE: gerada a partir de docs/der-processojudicial-xsd.md.
-- Origem prevista: dcp. Validar mapeamento e joins antes de promover o arquivo.
select
       CAST(NULL AS INT) AS cd_sist_origem,
       CAST(NULL AS INT) AS cd_tb_origem,
       CAST(NULL AS VARCHAR) AS cd_seq_tb_origem,
       CAST(NULL AS VARCHAR) AS nome,
       CAST(NULL AS VARCHAR) AS sexo,
       CAST(NULL AS VARCHAR) AS nome_genitor,
       CAST(NULL AS VARCHAR) AS nome_genitora,
       CAST(NULL AS DATE) AS data_nascimento,
       CAST(NULL AS DATE) AS data_obito,
       CAST(NULL AS VARCHAR) AS numero_documento_principal,
       CAST(NULL AS VARCHAR) AS tipo_pessoa,
       CAST(NULL AS VARCHAR) AS raca_cor,
       CAST(NULL AS VARCHAR) AS cidade_natural,
       CAST(NULL AS VARCHAR) AS estado_natural,
       CAST(NULL AS VARCHAR) AS nacionalidade,
       CAST(NULL AS VARCHAR) AS email,
       CAST(NULL AS BOOLEAN) AS populacao_de_rua
 where 1 = 0;
