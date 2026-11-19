# 2inst_metacnj — CNJ Metas 2ª Instância

Pipeline de cálculo das Metas CNJ para 2ª instância do TJRJ.
Migração de Linux/Postgres para AWS EMR + Apache Spark + Iceberg (Glue Catalog).

---

## Estrutura do projeto

```
2inst_metacnj/
├── run_all_metas_emr.py          # Orquestrador principal (11 steps, spark-submit sequencial)
├── dag_carga_metas_2instancia.py # DAG Airflow — dispara o orquestrador via EMR AddStep
│
├── ejud/
│   ├── scripts/
│   │   ├── ejud_metacnj_m01_emr.py       # Meta 01 ejud: cnm01/julgadom/susm/desm
│   │   ├── ejud_metacnj_m02_emr.py       # Meta 02 ejud: dism02 + _ant + flow
│   │   ├── ejud_metacnj_mNN_emr.py       # Metas 04/06/07/08/10/11 (--meta NN)
│   │   ├── ejud_metacnj_meta_aux_emr.py  # Step 10: tb_ft_meta_aux
│   │   └── ejud_metacnj_relatorio_emr.py # Step 11: relatório por OJ/magistrado
│   ├── json/
│   │   ├── meta_config.json    # Constantes de negócio (situações, classes, assuntos)
│   │   └── emr-config.json     # Configuração do cluster EMR (Iceberg + Spark)
│   └── consultas/              # SQL auxiliar para diagnóstico (Athena)
│
├── eproc2g/
│   ├── scripts/
│   │   └── eproc_metacnj_m01_emr.py  # Meta 01 eproc2g: cnm01/julgadom/susm/desm
│   ├── json/
│   │   ├── meta_config.json    # Config eproc2g (trigger codes sem sit-61)
│   │   └── emr-config.json     # EMR config (idêntico ao ejud)
│   └── consultas/              # SQL auxiliar para diagnóstico (Athena)
│
└── azure_pipeline.yml          # CI/CD Azure DevOps
```

---

## Sistemas fonte

| Sistema  | Tabelas bronze                         | Metas          |
|----------|----------------------------------------|----------------|
| ejud     | `ejud.*`, `dw_2inst_ouro.tb_tf_situacao_ejud` | 01–02, 04, 06–08, 10–11 |
| eproc2g  | `dw_2inst_ouro.tb_tf_situacao_eproc`   | 01 apenas      |

---

## Pipeline — ordem dos steps

```
Step  1  ejud  Meta 01   cnm01 / julgadom01 / susm01 / desm01
Step  2  ejud  Meta 02   dism02 + dism02_ant + julgadom/susm/desm + _ant variants
Step  3  ejud  Meta 04   improbidade + adm pública       (cutoff = year-4)
Step  4  ejud  Meta 06   ambiental                       (cutoff = year-1)
Step  5  ejud  Meta 07   indígena / quilombola            (cutoff = year-1)
Step  6  ejud  Meta 08   feminicídio + viol. doméstica    (cutoff = year-2)
Step  7  ejud  Meta 10   criança e adolescente            (cutoff = year-2)
Step  8  ejud  Meta 11   idosos                          (cutoff = year-1)
Step  9  eproc2g Meta 01  (DEVE rodar após step 1 — DELETE+APPEND por cod_sistema)
Step 10  meta_aux         tb_ft_meta_aux (após todos os indicadores)
Step 11  relatorio        tb_ft_meta_por_oj + tb_ft_meta_por_magistrado
```

**Por que eproc2g Meta 01 é o step 9:**
ejud m01 usa `overwritePartitions()` — sobrescreve partição inteira.
eproc2g usa `DELETE + APPEND` para preservar linhas `cod_sistema=1`.
Ordem inversa apagaria os dados eproc2g.

---

## Tabelas Iceberg (Glue Catalog)

### Entrada (leitura)
| Tabela | Database | Descrição |
|--------|----------|-----------|
| `tb_tf_situacao_ejud` | `dw_2inst_ouro` | Situações ejud (partition: `month(dthrmov)`) |
| `tb_tf_situacao_eproc` | `dw_2inst_ouro` | Situações eproc2g (partition: `month(dth_inicio)`) |
| `ejud.t_processo` | `ejud` | Dados do processo ejud (codnat, codcompt, num_processo) |
| `ejud.tipoprocesso` | `ejud` | Lookup classe CNJ (`codextcnj`) |
| `ejud.competencia` | `ejud` | Lookup competência |
| `ejud.documento` | `ejud` | Flag `indativo` (somente ativos) |
| `ejud.processoassunto` | `ejud` | Assuntos do processo (numord) |
| `ejud.movimento` | `ejud` | Para exclusão judmigracao (inline) |
| `ejud.movimentorelator` | `ejud` | cod_oj + cod_magistrado (last movement) |
| `ejud.movcancedistr` | `ejud` | Cancelled distributions (anti-join safety filter) |

### Saída (escrita)
| Tabela | Database | Layer | Conteúdo |
|--------|----------|-------|----------|
| `tb_ft_meta_analitico` | `dw_2inst_ouro` | ouro | Uma linha por processo por indicador |
| `tb_ft_meta_aux` | `dw_2inst_ouro` | ouro | Uma linha por processo (atributos auxiliares) |

---

## Enrichment: cod_oj + cod_magistrado (_enrich_relator)

Source: `ejud.movimentorelator` — last movement ≤ cutoff per coddoc.

Safety filter: anti-join `ejud.movcancedistr` (exclude movements tied to cancelled distributions).

**Design decision**: We use raw `codorgjulg` (internal EJUD OJ code) without translating through
`CamaraCivelEspecializada` → `OrgaoJulgador.CodOrgJulgCNJ` (as MPS does). Reason: raw codes
expose source data mismatches instead of silently masking them behind valid-looking CNJ codes.
See `2inst_datajud/mps_procedure_analysis.md` for full CamaraCivelEspecializada reference.

---

## Algoritmo central — sit-88 (Pendente)

Regra CNJ oficial (Meta Nacional 2 / Meta 01):

Processos elegíveis são separados em dois grupos por classe — **cada grupo tem seu próprio conjunto de triggers**:

| Grupo | Classes | Triggers válidos para sit-88 |
|-------|---------|------------------------------|
| `CLASSE_CONHECIMENTO` | id_fase_processual=1 ou id_tipo_procedimento=1 | sit-24 (Distribuído) ou sit-61 (Recebido pelo Tribunal) |
| `CLASSE_OUTROS` | id_tipo_procedimento=5 ou 7 (investigatório) | sit-9 (Denúncia recebida), sit-65 (Fase iniciada), sit-81 (Classe evoluída para ação penal) |

**Regra de validação da sit-88:**
Processo entra na meta somente se sua **primeira sit-88** (`first_88_dt`) for precedida pelo trigger **correto para sua classe** (`trigger_dt ≤ first_88_dt`):
- Processo de CONHECIMENTO com sit-9/65/81 → **não válido** (trigger errado para a classe)
- Processo de OUTROS com sit-24/61 → **não válido** (trigger errado para a classe)

**dism** = `first_88_dt ≤ cutoff_date`, trigger class-correto, anti-join julgamento + anti-join judmigracao.

**cnm01** = mesmo cálculo, mas `first_88_dt IN [period_start, period_end]`.

**eproc2g**: sit-61 NÃO existe.
- `CLASSE_CONHECIMENTO` → trigger = sit-24 apenas
- `CLASSE_OUTROS` → trigger = sit-9, 65, 81 (igual ao ejud)

### tab_distribuicao / tab_julgamento (inline)

Linux usava parquet pré-computados. Iceberg recalcula inline a cada run:

```python
# tab_distribuicao equivalente
spark.table("dw_2inst_ouro.tb_tf_situacao_ejud")
    .filter(col("situacao").isin([9,65,81,61,24]))
    .groupBy("coddoc")
    .agg(min("dthrmov"))  # earliest distribution event

# tab_julgamento equivalente
spark.table("ejud.situacao")
    .filter(col("situacao").isin(JUDGMENT_CODES))
    # + guard sit-17: AND cod_tip_mov IN VALID_CODES_17
    .groupBy("coddoc")
    .agg(max("dthrmov"))  # latest judgment
```

### judmigracao (inline)

Processos migrados por `JUDMIGRACAO` / `USUARIO_*` = limbo.
Excluídos de dism (stock). Não excluídos de cnm01 (flow).

```python
judmig_coddoc = (
    spark.table("ejud.movimento")
    .filter(col("usuincl").like("USUARIO_%") | (col("usuincl") == "JUDMIGRACAO"))
    .select("coddoc").distinct()
)
```

---

## Config JSON

### `ejud/json/meta_config.json` — estrutura principal

```json
{
  "catalog": { "ind_sistema": 1, ... },
  "shared": {
    "dist_start": "1970-01-01",
    "situacao_codes": {
      "distribution_codes_stock": [9, 65, 81, 61, 24],
      "judgment": [...],
      "desm_cod_tip_mov": [...]
    },
    "classe_conhecimento": [...],
    "classe_conhecimento_criminal": [...]
  },
  "meta_01": { "conhecimento_dist_codes": [61, 24], "outros_dist_codes": [9, 65, 81], ... },
  "meta_02": { "cutoff_offset": 3, "cod_indicador_map": { "dism02": 6, ... } },
  "meta_04": { "cutoff_offset": 4, "assunto_codes": {...}, ... },
  ...
}
```

### `eproc2g/json/meta_config.json`

Mesmo esquema. Diferença principal:
- `meta_01.conhecimento_dist_codes: [24]` (sem sit-61)
- `meta_01.outros_dist_codes: [9, 65, 81]`

---

## Como executar

### Via DAG Airflow (produção)

DAG: `dag_carga_metas_2instancia`
- Valida cluster EMR (`EMR_CLUSTER_PROJETOS_2INST`)
- Monta step EMR que baixa `run_all_metas_emr.py` do S3 e executa
- Variável `metacnj_dry_run=true` para dry-run
- **Default**: `--skip-dism` ativo (não reconstrói stock histórico)

### Manual no master EMR

```bash
# Download orquestrador
python3 -c "import boto3; boto3.client('s3').download_file(
    'sgdai-projetos',
    '2instancia/2inst_metacnj/run_all_metas_emr.py',
    'run_all_metas_emr.py')"

# Run completo (skip-dism = padrão, para reruns)
python3 run_all_metas_emr.py --year 2026 2>&1 | \
    grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception'

# Primeira execução do ano OU bronze mudou — força rebuild do stock
python3 run_all_metas_emr.py --year 2026 --rebuild-dism 2>&1 | \
    grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception'

# Retomar após falha no step 4
python3 run_all_metas_emr.py --year 2026 --from-step 4
```

---

## Deploy (sync para S3)

Scripts precisam estar no S3 para o orquestrador os encontrar:

```bash
# ejud scripts
aws s3 sync ejud/scripts/ \
  s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ \
  --exclude "*" --include "*.py"

# eproc2g scripts
aws s3 sync eproc2g/scripts/ \
  s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/scripts/ \
  --exclude "*" --include "*.py"

# orquestrador
aws s3 cp run_all_metas_emr.py \
  s3://sgdai-projetos/2instancia/2inst_metacnj/run_all_metas_emr.py

# config JSON ejud
aws s3 sync ejud/json/ \
  s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/json/ \
  --exclude "*" --include "*.json"

# config JSON eproc2g
aws s3 sync eproc2g/json/ \
  s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/json/ \
  --exclude "*" --include "*.json"
```

---

## Variáveis Airflow relevantes

| Variável | Uso |
|----------|-----|
| `EMR_CLUSTER_PROJETOS_2INST` | Cluster ID (validado antes do step) |
| `s3_script_metas_2instancia` | Path S3 do orquestrador (default vem do DAG) |
| `metacnj_dry_run` | `true` = dry-run sem execução real |
| `aws_region` | Região AWS (default `sa-east-1`) |

---

## JAR Iceberg

```
/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar
```

Cluster usa Spark 3.5.5-amzn-0. Ivy resolve 3.4 como dependência transitiva — ambos
coexistem no classpath sem conflito porque 3.5 é carregado primeiro via `--jars`.

---

## Notas de diagnóstico

- **stderr** do step EMR contém logs JVM/Spark
- **stdout** do step EMR contém saída Python do orquestrador (banners STEP/Done/FAILED)
- Erro `NameError: META_REGISTRY` = `main()` do mNN desatualizado (fix: commit `2a918ae`)
- Erro `AnalysisException: Table not found` = script na S3 desatualizado, fazer sync
