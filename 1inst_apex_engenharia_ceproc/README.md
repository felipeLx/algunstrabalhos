# 1inst_apex_ceproc

Repositorio de projetos de dados do CEPROC no APEX (1a instancia).

## Objetivo

Este repositorio centraliza os artefatos tecnicos dos projetos de dados do CEPROC:

- consultas SQL (Spark/Athena),
- scripts de carga para Iceberg,
- scripts de replica para Oracle,
- documentacao operacional.

## Arquitetura Padrao

Fluxo padrao de cada tabela:

1. SQL (Spark) executa via `1_load_tables.py`.
2. Resultado grava em Iceberg no schema `dw_apex`.
3. `2_replication_iceberg_on_apex_ext.py` replica para Oracle APEX.
4. A orquestracao ocorre pela DAG `dag_apex_ceproc` (repositorio de DAGs separado).

## Projeto Atual: Localizadores CEPROC

Arquivos principais deste projeto:

- `tb_localizador_processo_1inst.sql`: fato principal (Spark SQL).
- `tb_localizador_processo_2inst.sql`: fato principal (Spark SQL).
- `tb_dim_localizador.sql`: dimensao comum (snapshot atual).
- `tb_dim_historico_localizador.sql`: dimensao historica (append-only).
- `1_load_tables.py`: build de tabelas Iceberg.
- `2_replication_iceberg_on_apex_ext.py`: replica Iceberg -> Oracle.
- `dag_apex_ceproc_1inst.py`: referencia local da DAG deste projeto.

## Scripts Base

### `1_load_tables.py`

Responsavel por:

- ler SQL no S3,
- executar via Spark,
- gravar no Iceberg (`dw_apex`).

Parametros mais importantes:

- `--table`: roda somente uma tabela (`nome_arquivo.sql` sem extensao).
- `--schema-destino`: schema Iceberg (ex.: `dw_apex`).
- `--consultas-base-dir`: diretorio base de SQLs no S3.
- `--write-mode`: `overwrite` ou `append`.

Regras:

- SQL deve ser `SELECT` puro (sem `DROP/CREATE/INSERT`).
- Historico deve usar `--write-mode append`.

### `2_replication_iceberg_on_apex_ext.py`

Responsavel por:

- ler tabela Iceberg,
- gravar no Oracle via JDBC.

Parametros mais importantes:

- `--iceberg-table`: origem no data lake.
- `--oracle-table`: destino Oracle.
- `--truncate-first`: `1` trunca antes de gravar, `0` nao trunca.
- `--append-only-new`: quando `truncate-first=0`, envia somente registros novos.

## DAG do Projeto

- Nome da DAG: `dag_apex_ceproc`.
- Referencia local: `dag_apex_ceproc_1inst.py`.
- Publicacao oficial: repositorio separado de DAGs do Airflow.

Observacao:

- Este repositorio versiona a logica e a configuracao.
- A DAG efetiva de producao deve ser atualizada no repositorio de DAGs.

### Execucao com cluster existente (modo teste/operacao)

A DAG aceita `cluster_id` via `dag_run.conf`.

Com isso:

- nao cria cluster novo;
- usa o cluster informado para os steps;
- nao encerra o cluster ao final.

Exemplo de `conf` no trigger manual:

```json
{
  "cluster_id": "j-XXXXXXXXXXXXX"
}
```

Observacao:

- se `cluster_id` nao for informado, a DAG segue o modo padrao (cria e encerra cluster).

## Links e Caminhos Oficiais

- Repositorio de DAGs (Airflow): `https://dev.azure.com/tjerj/Sala-Iris/_git/airflow_1inst_jobs`
- Script build no S3: `s3://sgdai-projetos/1instancia/1inst_apex_engenharia_ceproc/scripts/1_load_tables.py`
- Script replica no S3: `s3://sgdai-projetos/1instancia/1inst_apex_engenharia_ceproc/scripts/2_replication_iceberg_on_apex_ext.py`
- Base de consultas SQL no S3: `s3://sgdai-projetos/1instancia/1inst_apex_engenharia_ceproc/consultas`

## Como Adicionar Nova Tabela na DAG (Passo a Passo)

1. Criar SQL da tabela no padrao Spark (`SELECT` puro).
2. Salvar o arquivo com nome final da tabela (ex.: `tb_nova_tabela.sql`).
3. Se for historica, garantir que o SQL retorne apenas o delta (novos registros).
4. Se for historica e primeira carga, criar tabela Iceberg no Athena antes da DAG.
5. Subir SQL (e scripts, se mudaram) para o S3 de consultas/scripts.
6. No arquivo da DAG (no repositorio de DAGs), adicionar step `type="build"`:
   - `--table tb_nova_tabela`
   - `--write-mode overwrite` (comum) ou `append` (historica)
7. Se precisar Oracle, adicionar step `type="oracle"`:
   - comum: `--truncate-first 1`
   - historica: `--truncate-first 0 --append-only-new 1`
8. Publicar a DAG no repositorio de DAGs.
9. Executar run manual e validar:
   - tabela criada/atualizada em `dw_apex`,
   - dados no Oracle,
   - logs sem erro.
10. Somente depois agendar execucao recorrente.

## Matriz de Modo de Carga

| Tipo de tabela | Build Iceberg (`1_load_tables.py`) | Replica Oracle (`2_replication`) |
|---|---|---|
| Dimensao comum / snapshot | `--write-mode overwrite` | `--truncate-first 1` |
| Dimensao historica | `--write-mode append` | `--truncate-first 0 --append-only-new 1` |

## Bootstrap Historico (Athena)

Exemplo para primeira criacao de tabela historica:

```sql
CREATE TABLE IF NOT EXISTS dw_apex.tb_dim_historico_localizador (
  id_localizador string,
  des_localizador string,
  sig_localizador string
)
LOCATION 's3://sgdai-data-lake/ouro/dw_apex/tb_dim_historico_localizador/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);
```

## Checklist de Deploy

- SQLs atualizados no S3.
- Scripts atualizados no S3.
- DAG atualizada no repositorio de DAGs.
- Tabelas historicas bootstrapadas no Athena (quando aplicavel).
- DAG despausada e schedule configurado.

## Agendamento atual da DAG local

No arquivo `dag_apex_ceproc_1inst.py` o parametro atual e:

- `schedule="0 2 * * *"`

Ou seja: execucao diaria as 02:00 (America/Sao_Paulo), sem retroprocessamento (`catchup=False`).
