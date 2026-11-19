# Situações da 2ª Instância

Projeto responsável por calcular e carregar mensalmente as situações processuais dos sistemas **EJUD** e **EPROC2G** para o Data Lake em formato Iceberg (Glue Catalog / S3).

---

## Arquitetura Geral

```
Azure DevOps (prod_aws)
    └── Pipeline CI/CD
            ├── Sincroniza scripts e JSONs para S3
            └── Airflow cria cluster EMR e executa os scripts via spark-submit
```

Os scripts são executados no **Amazon EMR (YARN)** lendo tabelas Iceberg do catálogo Glue e escrevendo na camada prata (`prata.*`).

---

## Estrutura do Repositório

```
2inst_situacao/
├── azure_pipeline.yml          # Pipeline CI/CD — sincroniza arquivos para S3
├── eproc2g/
│   ├── scripts/
│   │   └── eproc_situacao_emr.py   # Script principal EPROC2G
│   └── json/
│       ├── situacoes.json          # Mapeamento movimento -> situação
│       ├── situacoes_start_end.json # Regras de dth_fim por situação
│       └── emr-config.json         # Configuração Iceberg para o cluster EMR
└── ejud/
    ├── scripts/
    │   └── ejud_situacao_emr.py    # Script principal EJUD
    └── json/
        ├── situacoes.json          # Mapeamento movimento -> situação
        ├── situacoes_start_end.json # Regras de dth_fim por situação
        └── emr-config.json         # Configuração Iceberg para o cluster EMR
```

---

## Pipeline CI/CD (azure_pipeline.yml)

Disparado a cada push na branch `prod_aws`. Sincroniza via `aws s3 sync`:

| Etapa | Origem (repo) | Destino (S3) |
|-------|--------------|--------------|
| Sync eproc2g scripts | `eproc2g/scripts/` | `s3://sgdai-projetos/2instancia/2inst_situacao/eproc2g/scripts/` |
| Sync eproc2g json | `eproc2g/json/` | `s3://sgdai-projetos/2instancia/2inst_situacao/eproc2g/json/` |
| Sync ejud scripts | `ejud/scripts/` | `s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/` |
| Sync ejud json | `ejud/json/` | `s3://sgdai-projetos/2instancia/2inst_situacao/ejud/json/` |

O cluster EMR é criado pelo Airflow — o pipeline **não** submete steps ao EMR.

---

## Execução Manual (Putty / Master Node)

**EPROC**
```bash
spark-submit --master yarn --deploy-mode client \
  --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
  s3://sgdai-projetos/2instancia/2inst_situacao/eproc2g/scripts/eproc_situacao_emr.py \
  --year 2026 --month 3 2>&1 | \
  grep --line-buffered -E 'eproc_situacao_emr|ERROR|Exception'
```

**EJUD**
```bash
spark-submit --master yarn --deploy-mode client \
  --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
  s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/ejud_situacao_emr.py \
  --year 2026 --month 3 2>&1 | \
  grep --line-buffered -E 'ejud_situacao_emr|ERROR|Exception'
```

> **Importante:** O cluster não inclui Iceberg por padrão. O JAR `iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar` já está presente em `/usr/share/aws/iceberg/lib/` em todos os clusters EMR e deve ser passado via `--jars`. O Airflow precisa passar o `emr-config.json` com `iceberg.enabled: true` na criação do cluster para evitar isso no futuro.
---

## EPROC2G

### Tabelas de Entrada (Glue: `eproc2g`)

| Tabela | Papel |
|--------|-------|
| `processo_evento` | Eventos processuais — filtrado por ano |
| `evento_judicial` | Resolve `id_evento_judicial` → `cod_mni_movimento` |
| `evento_judicial_mni_complemento_tabelado` | Complementos tabelados do evento |
| `mni_complemento_tabelado` | Descrição dos complementos CNJ |
| `processo` | Metadados do processo (classe, órgão, sigilo) |
| `relacao_classe_judicial_mni` | Mapeia classe judicial → código CNJ |

### Tabela de Saída (Glue: `prata`)

`prata.tb_eproc2g_situacao` — particionada por `ano_mes`

### Lógica de Processamento

**Passo 1 — Situações principais:**
1. Lê `processo_evento` filtrado pelo ano alvo
2. Faz joins com lookup tables para resolver `cod_mni_movimento` e complementos
3. Aplica mapeamento do `situacoes.json` (broadcast join) — cada combinação de movimento + complementos resulta em um código de situação
4. Resolve `dth_fim` via `situacoes_start_end.json`:
   - **Tipo 1 (instantânea):** `dth_fim = dth_inicio` — `start_with` == `end_with`
   - **Tipo 2 (com fechamento):** `dth_fim` = data do próximo movimento de fechamento (`end_with` / `group_end_with`)
5. Escreve na tabela Iceberg (cria se não existir, sobrescreve partição se já existir)

**Passo 2 — Situações especiais 025 e 088:**
- Derivadas dos movimentos já mapeados no Passo 1
- Situação 025 e 088 são geradas quando um movimento de abertura ocorre sem fechamento dentro do mês
- Appendadas na mesma tabela Iceberg

---

## EJUD

### Tabelas de Entrada (Glue: `ejud`)

| Tabela | Papel |
|--------|-------|
| `movimento` | Movimentos processuais — filtrado por ano (`dthrmov`) |
| `integracaofasecompl_ejud_cnj` | Mapeia código interno `codfase` → código MNI/CNJ (`codfasenivel2cnj`) |
| `t_processo` | Metadados do processo (sigilo, tipo) |

### Tabela de Saída (Glue: `prata`)

`prata.tb_ejud_situacao` — particionada por `ano_mes`

### Lógica de Processamento

Idêntica ao EPROC2G com as diferenças de modelo de dados:

| | EPROC2G | EJUD |
|--|---------|------|
| Chave do processo | `id_processo` | `coddoc` |
| Coluna de data | `dth_evento` | `dthrmov` |
| Código de movimento | `cod_mni_movimento` (direto) | `codfase` → resolvido via `integracaofasecompl_ejud_cnj` |
| Complementos | `cod_mni_complemento`, `cod_mni_complemento_tabelado` | `codcompl1`, `codcompl2`, `codcompl3` → resolvidos via integração |

> **Nota:** O EJUD não precisa recriar a `tab_xml_andamento` como no servidor Linux. O join entre `movimento` e `integracaofasecompl_ejud_cnj` resolve os códigos MNI diretamente no EMR, simplificando o pipeline.

---

## JSONs de Configuração

### `situacoes.json`
Define o mapeamento de movimentos para situações. Estrutura:
```json
[
  {
    "situacao": 4,
    "movimentos": [
      { "cod_mni_movimento": 245 },
      { "cod_mni_movimento": 245, "complemento1": 12 }
    ]
  }
]
```

### `situacoes_start_end.json`
Define as regras de `dth_fim` por situação. Estrutura:
```json
[
  { "situacao": 24, "start_with": [24], "end_with": [24] },
  { "situacao": 4,  "start_with": [4],  "end_with": [2, 4, 10, 159] },
  { "situacao": 25, "start_with": [...], "end_with": [...] }
]
```

### `emr-config.json`
Configuração Iceberg injetada no cluster EMR pelo Airflow:
```json
[
  { "Classification": "iceberg-defaults", "Properties": { "iceberg.enabled": "true" } },
  { "Classification": "spark-defaults",   "Properties": { "spark.sql.catalog.glue_catalog": "..." } }
]
```

---

## Parâmetros dos Scripts

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `--year` | obrigatório | Ano de referência |
| `--month` | obrigatório | Mês de referência (1-12) |
| `--catalog` | `glue_catalog` | Catálogo Iceberg |
| `--db-eproc` / `--db-ejud` | `eproc2g` / `ejud` | Database de origem |
| `--db-prata` | `prata` | Database de destino |
| `--target-table` | `tb_eproc2g_situacao` / `tb_ejud_situacao` | Tabela de destino |
| `--executor-memory` | `12g` | Memória por executor |
| `--executor-memory-overhead` | `2g` | Overhead de memória |
| `--dry-run` | `false` | Se `true`, exibe schema e contagens sem escrever |

---

## Atualização Trimestral do CNJ

O CNJ publica atualizações nas situações do Datamart periodicamente (tipicamente a cada 3–6 meses).
Devemos acompanhar e incorporar novas situações/movimentos nos JSONs de mapeamento.

### Fonte oficial

- **Site**: https://www.cnj.jus.br/sistemas/datajud/parametrizacao/
- **Arquivo**: "Situações Datamart" (link para download do Excel/CSV)
- **Última versão verificada**: 12-2025 (`situacoes-datamart-12-2025(Situação).csv`)

> **ATENÇÃO**: A URL contém a versão no nome do arquivo (ex: `12-2025`).
> Se o CNJ mudar a estrutura do site ou o padrão de URL, será necessário atualizar manualmente.
> Não há API estável — o link é estático e muda a cada publicação.

### Procedimento de atualização

1. Acessar o site do CNJ e baixar o CSV mais recente
2. Comparar com os JSONs atuais:
   - `ejud/json/situacoes.json`
   - `eproc2g/json/situacoes.json`
   - `ejud/json/situacoes_start_end.json`
3. Identificar:
   - Novas situações (códigos novos)
   - Novos movimentos (leaf codes) em situações existentes
   - Situações removidas ou alteradas
4. Atualizar ambos os JSONs (ejud e eproc2g) — atentar para os nomes de campos diferentes:
   - EJUD usa: `complemento1`, `complemento2`, `complemento3`
   - EPROC2G usa: `cod_mni_complemento`, `cod_mni_complemento_tabelado`
5. Atualizar `situacoes_start_end.json` com regras de abertura/fechamento das novas situações
6. Recriar a tabela dimensão via `create_dim_mapeamento.py`
7. Fazer backfill se necessário (novos mapeamentos = novos registros na tabela fato)

### Histórico de atualizações

| Data | Versão CNJ | Alterações |
|------|-----------|------------|
| 2026-05 | 12-2025 | +sit 167 (Suspensão por Remissão), +sit 168 (Levantada sobrestamento por Remissão), +18 leaf codes em sits 17/21/28/29/70/73/74/75 |

### Notas

- Situações 25 (Tramitando) e 88 (Pendente) são geradas pelo script (`process_special_situacoes`), não vêm de movimentos. Tratadas em `situacoes_start_end.json`.
- Situação 65 (Fase processual) não tem mapeamento direto — é derivada. Não precisa estar em `situacoes.json`.
- O encoding do CSV do CNJ é `cp1252` (Windows Latin-1), delimitado por `;`.
