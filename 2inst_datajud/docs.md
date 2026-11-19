# 2inst_datajud — DataJud Pipeline (2ª Instância TJRJ)

---

## 1. O que é DataJud?

**DataJud** (Base Nacional de Dados do Poder Judiciário) é o repositório centralizado do CNJ que armazena dados e metadados processuais de **todos os tribunais brasileiros** — processos físicos e eletrônicos, públicos e sigilosos.

- **Instituído pela**: Resolução CNJ nº 331/2020
- **Fonte primária do**: SIESPJ (Sistema de Estatística do Poder Judiciário)
- **Mantido pelo**: DPJ (Departamento de Pesquisas Judiciárias) do CNJ
- **Padrão de dados**: MNI 2.2.2 (Modelo Nacional de Interoperabilidade)

### Fluxo resumido

```
TJRJ (EJUD / EPROC2G)
    ↓ envia XML (padrão MTD/MNI 2.2.2)
CNJ DataJud (Elasticsearch)
    ↓ processa / indexa
Datamart (indicadores estatísticos)
    ↓ publica
Painel de Estatísticas + API Pública
```

Cada tribunal é **obrigado** a enviar seus dados processuais ao CNJ no formato MTD. O CNJ então processa, calcula indicadores (Metas, produtividade, situações) e publica no Painel de Estatísticas.

---

## 2. Por que DataJud importa para nós (TJRJ 2ª Instância)?

O DataJud é a **visão que o CNJ tem dos nossos dados**. Quando o CNJ calcula Metas, rankings e indicadores, ele usa os dados que recebeu via DataJud — não consulta diretamente EJUD ou EPROC2G.

**Problema central**: Se nossos dados internos (EJUD/EPROC2G) divergem do que o CNJ tem no DataJud, os indicadores oficiais ficam errados.

**Nosso papel com 2inst_datajud**:
1. **Validar** — comparar dados internos vs DataJud para detectar divergências
2. **Diagnosticar** — entender por que processos/movimentos estão faltando ou diferentes
3. **Corrigir** — orientar reenvio de dados quando necessário
4. **Monitorar** — acompanhar evolução da qualidade dos dados enviados

---

## 3. Estrutura dos Dados DataJud

### 3.1 Modelo de Dados (campos do Elasticsearch)

#### Raiz do Documento (Processo)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | text/keyword | Chave: `Tribunal_Classe_Grau_OrgaoJulgador_NumeroProcesso` |
| `tribunal` | text/keyword | Sigla do tribunal (ex: `TJRJ`) |
| `numeroProcesso` | text/keyword | Numeração única CNJ (sem formatação) |
| `dataAjuizamento` | datetime | Data de ajuizamento (capa) |
| `grau` | text/keyword | Instância: `G1`, `G2`, `JE`, `REsp`, `RE`, `TUR`, `TSE` |
| `nivelSigilo` | long | Nível de sigilo (0 = público, ≥1 = sigiloso) |
| `dataHoraUltimaAtualizacao` | datetime | Última atualização (ms) |
| `@timestamp` | datetime | Timestamp de indexação |

#### Formato

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `formato.codigo` | long | Código do formato |
| `formato.nome` | text/keyword | `Físico` ou `Eletrônico` |

#### Sistema Processual

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `sistema.codigo` | long | Código do sistema |
| `sistema.nome` | text/keyword | Nome do sistema (EJUD, EPROC, etc.) |

#### Classe Processual

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `classe.codigo` | long | Código CNJ (TPU) |
| `classe.nome` | text/keyword | Descrição da classe |

#### Assuntos (array)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `assuntos[].codigo` | long | Código CNJ (TPU) |
| `assuntos[].nome` | text/keyword | Descrição do assunto |

#### Órgão Julgador

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `orgaoJulgador.codigo` | long | Código do órgão |
| `orgaoJulgador.nome` | text/keyword | Nome (ex: "1ª Câmara Cível") |
| `orgaoJulgador.codigoMunicipioIBGE` | long | Código IBGE do município |

#### Movimentos (array) — DADOS MAIS IMPORTANTES

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `movimentos[].codigo` | long | Código MNI do movimento (TPU) |
| `movimentos[].nome` | text/keyword | Descrição do movimento |
| `movimentos[].dataHora` | datetime | Data/hora de ocorrência |
| `movimentos[].orgaoJulgador.codigoOrgao` | long | Órgão do movimento |
| `movimentos[].orgaoJulgador.nomeOrgao` | text/keyword | Nome do órgão |

#### Complementos Tabelados (dentro de cada movimento)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `complementosTabelados[].codigo` | long | Código da variável |
| `complementosTabelados[].descricao` | text/keyword | Descrição da variável |
| `complementosTabelados[].valor` | long | Código do complemento |
| `complementosTabelados[].nome` | text/keyword | Descrição do complemento |

### 3.2 Tag Datamart (indicadores calculados pelo CNJ)

Após processamento estatístico, o CNJ adiciona a tag Datamart:

| Campo | Descrição |
|-------|-----------|
| `datamart.id` | ID do datamart |
| `datamart.id_situacao_atual` | Código da situação atual |
| `datamart.situacao_atual` | Texto da situação (ex: "Tramitando") |
| `datamart.id_fase_atual` | Código da fase atual |
| `datamart.fase_atual` | Texto da fase |
| `datamart.criminal` | Flag criminal (boolean) |
| `datamart.data_cn` | Data caso novo |
| `datamart.data_situacao_atual` | Data da situação atual |
| `datamart.data_baixa` | Data de baixa |
| `datamart.@timestamp` | Timestamp de processamento |

---

## 4. Fontes de Dados DataJud

### 4.1 API Pública

- **Base URL**: Elasticsearch endpoint do CNJ
- **Auth**: `Authorization: APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==`
- **Formato**: POST com query Elasticsearch (JSON body)
- **Dados**: metadados processuais públicos de todos os tribunais
- **Limitação**: não retorna processos sigilosos, rate limits

### 4.2 CSV Bulk (arquivo TJRJ.csv)

- **Origem**: Download direto do CNJ (dump periódico)
- **Localização anterior**: `E:\Bck_Documentos\TJRJ.csv`
- **Encoding**: UTF-8, separador `;`
- **Filtro 2ª instância**: `sigla_grau == 'G2'`
- **Tamanho**: vários GB (todos os graus do TJRJ)

#### Colunas conhecidas do CSV bulk:

| Coluna | Descrição |
|--------|-----------|
| `id_processo` | ID do processo no DataJud |
| `numero_sigilo` | Número com flag de sigilo |
| `sigla_grau` | G1, G2, JE, etc. |
| `procedimento` | Tipo de procedimento |
| `ramo_justica` | Ramo (Estadual, Federal, etc.) |
| `sigla_tribunal` | TJRJ |
| `id_tribunal` | ID numérico |
| `recurso` | Tipo de recurso |
| `id_ultimo_oj` | Último órgão julgador |
| `dt_recebimento` | Data de recebimento |
| `id_ultima_classe` | Última classe processual |
| `id_assunto` | Assunto principal |
| `dt_julgamento` | Data de julgamento |
| `primeirasentm2_b` | Primeira sentença M2 (boolean) |
| `baixm2_b` / `baixm2_b_ate` | Baixa M2 |
| `decm2_b` | Decisão M2 |
| `julgadom2_b` | Julgado M2 |
| `susm2_b` | Suspenso M2 |
| `dism2_b` | Distribuído M2 |
| `cnm1` | Caso novo M1 |
| `primeirasentm1` | Primeira sentença M1 |
| `baixm1` / `decm1` / `julgadom1` / `susm1` / `desm1` / `mpum1` | Indicadores M1 |
| `data_total_primeira_baixa` | Data total primeira baixa |

### 4.3 Database PostgreSQL (interno)

- **Host**: `172.20.30.141:55432`
- **Database**: `datajud`
- **User**: `adm_datajud`
- **Uso atual**: pipelines de dimensões e meta_relatorio já leem desta base

---

## 5. Relação com Pipelines Existentes

### 5.1 Mapeamento: Dados Internos → DataJud

```
EJUD (Oracle)                    DataJud (CNJ)
─────────────                    ─────────────
coddoc                    →      numeroProcesso (via NPU)
movimento.codfase         →      movimentos[].codigo (via integracaofasecompl_ejud_cnj)
movimento.codcompl1/2/3   →      complementosTabelados[].valor
movimento.dthrmov         →      movimentos[].dataHora
t_processo.codclasse      →      classe.codigo

EPROC2G (PostgreSQL)             DataJud (CNJ)
────────────────────             ─────────────
processo.id_processo      →      numeroProcesso
processo_evento.cod_mni_movimento → movimentos[].codigo (direto, já MNI)
processo_evento.dth_evento →     movimentos[].dataHora
```

**Diferença crítica**:
- EPROC2G já usa códigos MNI nativamente → mapeamento direto
- EJUD usa códigos internos (codfase) → precisa de tabela de tradução (`integracaofasecompl_ejud_cnj`)

### 5.2 Pipelines que já usam DataJud

| Pipeline | Como usa DataJud |
|----------|-----------------|
| `2inst_metacnj` | Valida exclusão de JUDMIGRACAO contra DataJud |
| `2inst_meta_relatorio` | Lê DB PostgreSQL DataJud para relatórios |
| `2inst_produtividade` | Filtra processos migrados (JUDMIGRACAO) validados contra DataJud |
| `2inst_situacao` | Usa parametrização CNJ (situacoes.json) derivada do Datamart |
| `2inst_dimensao` | 7 dimensões lêem do DB PostgreSQL DataJud (orgao, classe, magistrado, etc.) |

---

## 6. Glossário de Termos

| Termo | Significado |
|-------|-------------|
| **MNI** | Modelo Nacional de Interoperabilidade — padrão CNJ para troca de dados entre sistemas judiciários. Versão atual: 2.2.2 |
| **MTD** | Modelo de Transferência de Dados — formato XML que tribunais enviam ao CNJ. Baseado no MNI |
| **TPU** | Tabelas Processuais Unificadas — tabelas de códigos padronizados do CNJ (classes, assuntos, movimentos) |
| **SGT** | Sistema de Gestão de Tabelas — onde o CNJ mantém as TPUs |
| **Datamart** | Camada de indicadores calculados sobre os dados brutos do DataJud |
| **Situação** | Estado processual derivado de movimentos (ex: Tramitando, Baixado, Suspenso). Mapeado via `situacoes.json` |
| **Complemento Tabelado** | Qualificador do movimento (ex: movimento "Julgamento" + complemento "Provido") |
| **NPU** | Numeração Processual Única — formato: `NNNNNNN-DD.AAAA.J.TR.OOOO` |
| **Grau** | Instância: G1 (1º grau), G2 (2º grau), JE (Juizado Especial), REsp, RE, TUR, TSE |
| **JUDMIGRACAO** | Movimento especial: processo migrado entre sistemas. Excluído de vários indicadores |
| **Caso Novo (CN)** | Processo distribuído pela primeira vez — métrica base das Metas |
| **Baixa** | Saída definitiva do processo do acervo |

---

## 7. Parametrização CNJ (Situações Datamart)

O CNJ publica periodicamente (a cada 3-6 meses) atualizações nas regras de mapeamento movimento→situação.

- **Site**: https://www.cnj.jus.br/sistemas/datajud/parametrizacao/
- **Arquivo**: "Situações Datamart" (CSV, encoding `cp1252`, delimitador `;`)
- **Última versão verificada**: `situacoes-datamart-12-2025(Situação).csv`

Essas regras definem:
- Quais movimentos geram quais situações
- Quais situações são de início/fim
- Se situação é instantânea ou tem duração
- Se inicia fase de execução

---

## 8. Referências

- Portal DataJud: https://www.cnj.jus.br/sistemas/datajud/
- API Pública Wiki: https://datajud-wiki.cnj.jus.br/api-publica/
- Glossário de Dados: https://datajud-wiki.cnj.jus.br/api-publica/glossario/
- Parametrização: https://www.cnj.jus.br/sistemas/datajud/parametrizacao/
- MTD: https://datajud-wiki.cnj.jus.br/mtd/
- Tag Datamart: https://datajud-wiki.cnj.jus.br/para-tribunais/Datajud/tag-datamart/
- Resolução CNJ nº 331/2020: base legal do DataJud
