# 2inst_turmas_acervo

Acervo diário das Turmas (codlocal=3763) do EJUD.
Migração para Iceberg: leitura de bronze Glue, escrita em `dw_2inst_ouro.tb_ft_turmas_acervo`.

---

## 1. Resumo

| Item | Valor |
|---|---|
| Sistema | EJUD |
| Escopo | Processos fisicamente no local 3763 (Turmas) |
| Granularidade | 1 linha por `(coddoc, dth_referencia)` |
| Partição Iceberg | `dth_referencia` (DATE) |
| Frequência | Diária |
| Target Iceberg | `glue_catalog.dw_2inst_ouro.tb_ft_turmas_acervo` |
| Target legacy (PG) | `dw2inst.turmas_relatorio_final` |
| id_projeto (controle_carga) | 17 |

---

## 2. Critério de Elegibilidade (acervo bruto)

Um processo entra no acervo se **todas** as condições forem verdadeiras na `dth_referencia`:

1. **Tipo de processo ≠ 288** — `t_processo.codtipproc != 288`
2. **Competência turma** — `t_processo.codcompt IN (10, 11, 12, 14, 15, 19, 20, 22)`
3. **Último órgão julgador = turmas** — `movimentorelator.codorgjulg` mais recente (por coddoc) está em `(681, 682, 3553, 3798, 3799, 3800, 6676, 6739, 6740, 9525, 9816, 9940)`

> **Nota:** `movimentofisico` (codlocal) **não** é critério de elegibilidade.
> É retornado como LEFT JOIN para fins de relatório (`local_fisico`, `cod_ult_local`, `ind_excl_local_saida`).

**Última distribuição** (`codfase IN (26, 36, 60012)`) é adicionada via LEFT JOIN, restrita a movimentos onde `movimentorelator.codorgjulg` é um dos OJs das turmas — processos sem distribuição qualificada são elegíveis com `dth_ultima_distribuicao = NULL`.

---

## 3. Flags de Exclusão (acervo bruto → líquido)

Calculadas como `ind_excl_*` (S/N). Evento deve ocorrer **após** a última distribuição (codfase 26/36) e **antes** da `dth_referencia`.

| Flag | Regra |
|---|---|
| `ind_excl_fase_simples` | `codfase IN (861, 870, 22, 50011, 50014)` |
| `ind_excl_findo` | `codfase=50002` e `codcompl1 IN (50117,50118,60435)` ou `codcompl2=50118` |
| `ind_excl_expedicao` | `codfase=60`, `codcompl1=50007`, `codcompl2=50119` |
| `ind_excl_remessa` | `codfase IN (50123,123)` com complementos de remessa |
| `ind_excl_remessa_esp` | `codfase=123`, `codcompl1=66653`, `codcompl2=66652` |
| `ind_excl_local_saida` | movimento físico para ~50 locais de saída (arquivo, STJ, STF…) |
| `ind_excl_suspensao` | suspensão (codfase 3/11009 + compl 25/11025) sem reativação (fase 849) |
| `ind_acervo_liquido` | 'S' se nenhum `ind_excl_*` = 'S' |

---

## 4. Colunas de Saída (95 total)

### 4.1 Identificação e Distribuição
| Coluna | Origem |
|---|---|
| `dth_referencia` | parâmetro `--dateref` (DATE) |
| `coddoc` | `t_processo.coddoc` |
| `dth_ultima_distribuicao` | data do codfase 26/36 mais recente |
| `dth_distribuicao` | data do **primeiro** `movimentorelator` (distribuição original) |

### 4.2 Relator / Órgão Julgador
| Coluna | Origem |
|---|---|
| `ultimo_relator` | nome do magistrado no movimentorelator mais recente |
| `ultimo_orgao_julgador` | nome do órgão no movimentorelator mais recente |
| `cod_ultimo_relator` | `movimentorelator.codmagrel` |
| `cod_ultimo_orgao_julgador` | `movimentorelator.codorgjulg` |
| `relator` | nome no **primeiro** movimentorelator |
| `orgao_julgador` | nome do órgão no primeiro movimentorelator |

### 4.3 Indicadores do Processo
`ind_eletronico`, `cod_tipo_processo`, `cod_competencia`, `urgente`, `pedido_liminar`,
`defensoria_publica`, `justica_gratuita`, `crianca_adolescente`, `funciona_mp`,
`segredo_justica`, `idosos`, `idosos_80_mais`, `doenca_grave`, `prioridade_deficiente`,
`reu_preso`, `situacao_rua`, `classe`, `nivel_sigilo`

Fonte: `logprocesso` (tipoper != 'E', dthralt <= dth_referencia, rn=1 por dthralt DESC)

### 4.4 Número CNJ
| Coluna | Origem |
|---|---|
| `num_processo` | `NNNNNNN-DD.AAAA.J.TT.OOOO` (RESTRITO se nivel_sigilo=5) |
| `numsequencial`, `dv`, `numano`, `codjustica`, `codregi`, `codlocregi` | componentes |

### 4.5 Movimentos
| Coluna | Descrição |
|---|---|
| `dth_ultimo_movimento` | data do movimento mais recente |
| `cod_ultimo_movimento` | `faseexterna.codfaseext` do último movimento |
| `ultimo_movimento` | descrição da fase |
| `comp1/2/3_ultimo_movimento` | descrições dos complementos |
| `usuario_inclusao` | `movimento.usuincl` |
| `dth_ultimo_mov_cnj` (_d01) | igual ao acima mas excluindo codfase=67 |
| `cod/comp*_ultimo_movimento_d01` | equivalentes CNJ |

### 4.6 Julgamento
`dth_ultimo_julgamento`, `ultimo_julgamento`, `comp1/2/3_ultimo_julgamento`, `ind_julgado`

Fonte: `movimento` onde `faseexterna.codfaseext IN (193, 50193)`

### 4.7 Local
| Coluna | Origem |
|---|---|
| `local_virtual` | `localvirtual.descr` do localvirtualdocumento mais recente |
| `dth_entrada_local_virtual` | data do localvirtualdocumento mais recente |
| `local_fisico` | `localfisico.descr` do movimentofisico mais recente |
| `dth_entrada_local_fisico` | data do movimentofisico mais recente |
| `cod_ult_local` | `movimentofisico.codlocal` mais recente |

### 4.8 Partes
`partes` — "Polo1 x Polo2", construído com `collect_set` ordenado. RESTRITO se nivel_sigilo=5.

### 4.9 Assunto / Vara
`cod_assunto`, `assunto` — todos os assuntos concatenados por `;`
`cod_vara`, `vara` — origem da vara via `autoassociado.numseq=1`

### 4.10 Suspensão e Sobrestamento
| Coluna | Descrição |
|---|---|
| `ind_suspenso` | S se suspensão ativa sem reativação |
| `dth_suspensao` | data da suspensão |
| `movimento_suspensao`, `comp*_ultima_suspensao` | fase/complementos |
| `ind_sobrestado` | S se sobrestamento ativo (codfase=50040 sem término) |
| `dth_sobrestamento`, `dth_repercussao` | datas |
| `dthrterminosobre` | data de término do sobrestamento |
| `movimento_sobrestamento`, `comp*_ultimo_sobrestamento` | detalhes |

### 4.11 Decisão
`dth_ultima_decisao`, `ultima_decisao`, `comp1/2/3_ultima_decisao`, `ind_decisao`

Fonte: `movimento.codfase IN (3, 20)`, mais recente por coddoc.

### 4.12 Flags de Exclusão (somente Iceberg)
`ind_excl_fase_simples`, `ind_excl_findo`, `ind_excl_expedicao`, `ind_excl_remessa`,
`ind_excl_remessa_esp`, `ind_excl_local_saida`, `ind_excl_suspensao`, `ind_acervo_liquido`

> Estas 8 colunas existem no Iceberg mas **não** são carregadas para o PostgreSQL legacy.

---

## 5. Arquitetura

### 5.1 EMR / Iceberg (novo)

```
ejud_turmas_acervo_emr.py          ← self-contained EMR script
  └─ TurmasAcervoCalculator
       ├─ load_bronze_tables()      ← glue_catalog.ejud.* → TempViews
       ├─ build_turmas_base()       ← stage 1: turmas_base TempView
       └─ build_staged()            ← steps 0–15: SQL staged materializations
            └─ step 15 final SELECT → DataFrame com 95 colunas
  └─ _save_to_iceberg()             → glue_catalog.dw_2inst_ouro.tb_ft_turmas_acervo
                                       PARTITIONED BY (dth_referencia)
                                       overwritePartitions()
```

### 5.2 Linux legacy (acervo_turmas.py)

```
acervo_turmas.py
  ├─ local parquet → TempViews
  ├─ build_turmas_staged() → Parquet ouro
  └─ carrega_turmas_relatorio_pg() → dw2inst.turmas_relatorio_final (PG)
```

### 5.3 Staged Materializations

15 passos intermediários, cada um: `spark.sql() → persist(MEMORY_AND_DISK) → count() → createOrReplaceTempView()`.
Objetivo: quebrar plano de 700+ linhas em planos menores para evitar OOM.

| Step | TempView | Conteúdo |
|---|---|---|
| 0 | `complemento_stg` | lookup de complementos (sem 9999/59998) |
| 1 | `mov_relator_stg` | movimentorelator mais recente por coddoc |
| 2 | `relatorio_base_stg` | magistrado/orgão do relator atual |
| 3 | `indicadores_stg` | flags e classe do logprocesso mais recente |
| 4 | `ult_mov_stg` | último movimento geral |
| 5 | `ult_mov_cnj_stg` | último movimento excluindo codfase=67 |
| 6 | `ult_julg_stg` | último julgamento (codfaseext 193/50193) |
| 7 | `local_virt_stg` | local virtual mais recente |
| 8 | `assuntos_stg` | assuntos agregados por coddoc |
| 9 | `vara_origem_stg` | vara de origem (autoassociado.numseq=1) |
| 10 | `prim_relator_stg` | primeiro relator (movimentorelator ASC) |
| 11 | `local_fisico_stg` | local físico mais recente |
| 12 | `parte_stg` | partes polo 1 x polo 2 |
| 13a–13e | `suspensao/reativacao/repercussao/susp_rep_agg/susp_rep_stg` | suspensão e sobrestamento |
| 14 | `decisao_stg` | última decisão (codfase 3/20) |
| 14a–14g | `excl_*_stg` | fontes das flags de exclusão |
| 15 | (retorno) | DataFrame final com 95 colunas |

---

## 6. Tabelas Fonte (Glue bronze `ejud`)

`movimento`, `movimentofisico`, `movimentorelator`, `movsobrestamento`,
`t_processo`, `logprocesso`, `processoassunto`, `localvirtualdocumento`,
`autoassociado`, `parte`, `parteprocesso`, `magistrado`, `pessoa`,
`orgaojulgador`, `localvirtual`, `assunto`, `vara`, `fase`, `faseexterna`,
`complemento`, `tipoprocesso`, `localfisico`, `tipoparte`

---

## 7. Spark Config (EMR)

| Parâmetro | Valor padrão |
|---|---|
| `spark.executor.memory` | 24g |
| `spark.executor.memoryOverhead` | 4g |
| `spark.sql.shuffle.partitions` | 64 |
| `spark.sql.adaptive.enabled` | true |
| `spark.sql.autoBroadcastJoinThreshold` | -1 (sem auto-broadcast; broadcast() explícito) |
| `spark.sql.parquet.datetimeRebaseModeInRead` | LEGACY |

---

## 8. Deploy

### Azure Pipeline (`azure_pipeline.yml`)

Branch trigger: `prod_aws`

| Step | Source | S3 destination |
|---|---|---|
| Sync EJUD scripts | `ejud/scripts/*.py` | `.../ejud/scripts/` |
| Sync EJUD consultas | `ejud/consultas/*.sql` | `.../ejud/consultas/` |
| Sync EJUD json | `ejud/json/` | `.../ejud/json/` |
| Upload orchestrator | `run_all_turmas_acervo_emr.py` | `.../ run_all_turmas_acervo_emr.py` |

### Execução manual no EMR

```bash
# Via orchestrator wrapper (no cluster):
python3 s3://sgdai-projetos/2instancia/2inst_turmas_acervo/run_all_turmas_acervo_emr.py \
  --dateref 2026-04-24

# Dry-run:
python3 run_all_turmas_acervo_emr.py --dateref 2026-04-24 --dry-run

# Direto via spark-submit:
spark-submit \
  --master yarn --deploy-mode client \
  --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
  s3://sgdai-projetos/2instancia/2inst_turmas_acervo/ejud/scripts/ejud_turmas_acervo_emr.py \
  --dateref 2026-04-24
```

---

## 9. Controle de Carga (legacy)

O script `acervo_turmas.py` (linux) registra via `modulo.controle_carga`:

| Campo | Valor |
|---|---|
| `id_projeto` | 17 |
| `schema` | dw2inst |
| `tabela` | turmas_relatorio_final |

O script EMR Iceberg **não** registra em controle_carga (sem dependência de postgres no EMR).

---

## 10. Diferenças Linux ↔ Iceberg EMR

| Aspecto | Linux (`acervo_turmas.py`) | Iceberg (`ejud_turmas_acervo_emr.py`) |
|---|---|---|
| Fonte bronze | Parquet local / S3 via `spark.read.parquet()` | `glue_catalog.ejud.*` via `spark.table()` |
| Normalização coddoc | Não necessária (parquet já limpo) | `_norm()` remove `.0` / notação científica |
| Saída | Parquet ouro + PostgreSQL | Iceberg `dw_2inst_ouro.tb_ft_turmas_acervo` |
| Colunas extras | — | `ind_excl_*`, `ind_acervo_liquido` (8 colunas) |
| Partição | Sem partição no parquet | `dth_referencia` (DATE) |
| Controle carga | `modulo.controle_carga` (PG) | Sem (logs CloudWatch) |
