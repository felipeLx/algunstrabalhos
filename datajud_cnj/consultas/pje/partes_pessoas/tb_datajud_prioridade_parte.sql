-- Sem fonte confiavel no Athena nem no PJE Postgres original para prioridade
-- transacional vinculada a parte/pessoa.
-- Tabelas encontradas:
-- - pje/client.tb_proc_prioridde_processo: prioridade em nivel de processo.
-- - pje/client.tb_item_prioridade: prioridade em nivel de processo no Postgres original.
-- - pje/client.tb_prioridade_processo: dominio da prioridade.
-- - client.tb_aplicacao_cmptnca_prrde e client.tb_tipo_cl_prioridade_proc:
--   tabelas de configuracao/regra, nao registros aplicados a uma parte.
-- Nao replicar prioridade do processo para todas as partes sem confirmacao
-- de negocio, pois isso cria informacao sem granularidade registrada na origem.
select
       CAST(NULL AS DECIMAL(20, 0)) AS nr_processo,
       CAST(NULL AS INT) AS cd_sist_origem,
       CAST(NULL AS INT) AS cd_tb_origem,
       CAST(NULL AS STRING) AS cd_seq_tb_origem,
       CAST(NULL AS STRING) AS tipo_prioridade,
       CAST(NULL AS DATE) AS data_concessao,
       CAST(NULL AS DATE) AS data_fim
 where 1 = 0;
