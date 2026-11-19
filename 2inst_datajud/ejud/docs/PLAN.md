# 2inst_datajud — Plano de Implementação

---

## Visão Geral do Projeto

**Objetivo final:** Gerar XML com transformações corretas para envio ao CNJ (matching XSD 1.2).
**Prioridade atual:** Validar cálculo de situação (2inst_situacao) contra DataJud `datamart.id_situacao_atual`.

**Referência:** 1inst já implementou modelo canônico completo (ver `iceberg/datajud_cnj/docs/`).
Nosso caminho: validação primeiro → tabelas canônicas depois → geração XML por último.

---

## Fase 0: Entendimento (COMPLETA)

- [x] Entender o que é DataJud
- [x] Mapear estrutura de dados (API + CSV + PostgreSQL)
- [x] Documentar relação com pipelines existentes
- [x] Criar glossário de termos
- [x] Documentar fontes de dados disponíveis
- [x] Estudar implementação 1inst (datajud_cnj repo)
- [x] Mapear DER canônico e regras de vigência
- [x] Entender priorização por feature (F1-F4)

---

## Fase 1: Validação de Situação + Meta com Regras DataJud (PRIORIDADE ATUAL)

Objetivo: Nosso `2inst_situacao` + `2inst_metacnj` já calculam indicadores.
Agora adicionar flags de elegibilidade DataJud para produzir DOIS resultados:
- Resultado nosso (mais amplo, inclui todos movimentos)
- Resultado DataJud-elegível (aplica filtros MPS: OJ not null, faseext exclusions, etc.)

Quem decide qual regra vale: DIRETORES do TJRJ, não nós nem MPS.

### 1.1 — Adicionar flags DataJud-elegível ao 2inst_metacnj
- [ ] Mapear regras MPS que diferem das nossas:
  - `Cod_OJ IS NOT NULL` (MPS exclui movimentos sem OJ)
  - `CodFaseExt NOT IN (50999, 60009, 60010, 60022)` (faseexts excluídas)
  - `IndFaseInvis = 'N'` (movimentos invisíveis excluídos)
  - `BUSCARDADOS_FASECOMPL_CNJ() IS NOT NULL` (sem tradução = excluído)
  - `codcompt NOT IN (9,10,11,14,15,19,20,22)` (competências excluídas)
- [ ] Adicionar coluna `fl_elegivel_datajud` (Y/N) ao output do meta
- [ ] Adicionar coluna `fl_match_mps` (Y/N) — nosso resultado bate com regra MPS?
- [ ] Gerar dois totais: contagem nossa vs contagem DataJud-elegível

### 1.2 — Obter situação/meta DataJud para comparação
- [ ] Identificar melhor fonte para `datamart.id_situacao_atual` de G2 (PostgreSQL? CSV? API?)
- [ ] Extrair lista: `nr_processo` + `datamart.id_situacao_atual` + `datamart.data_situacao_atual`
- [ ] Filtrar: tribunal=TJRJ, grau=G2
- [ ] Contar total de processos com situação definida

### 1.3 — Cruzamento: Nosso resultado (filtrado) vs DataJud
- [ ] JOIN processos: nosso DataJud-elegível LEFT JOIN datajud ON nr_processo
- [ ] Classificar: MATCH vs DIVERGE vs MISSING
- [ ] Para DIVERGE: agrupar por par (situação_interna, situação_datajud) — identificar padrões
- [ ] Quantificar % match, % diverge, % missing
- [ ] Repetir para resultado SEM filtro — ver como divergência muda

### 1.4 — Análise de Causa Raiz (divergências)
- [ ] Top 10 pares divergentes: investigar movimentos nos dois lados
- [ ] Verificar se divergência vem de: movimento faltante? Mapeamento diferente? Filtro MPS?
- [ ] Identificar casos onde NOSSO resultado está certo e MPS errado (e vice-versa)
- [ ] Documentar diferenças de regra que explicam cada divergência

### 1.5 — Relatório para Diretores
- [ ] Tabela resumo: total processos, matches, divergências por tipo
- [ ] Apresentar: "com filtro MPS = X matches, sem filtro = Y matches"
- [ ] Listar decisões pendentes de diretor (ex: incluir OJ vazio? excluir competências?)
- [ ] Recomendar ações corretivas com impacto quantificado

---

## Fase 2: Exploração de Dados (apoio)

### 2.1 — Explorar o banco PostgreSQL DataJud
- [ ] Conectar no PostgreSQL (`172.20.30.141:55432`, db `datajud`)
- [ ] Listar todas as tabelas e schemas
- [ ] Documentar schema completo (colunas, tipos, PKs, FKs)
- [ ] Contar registros por tabela
- [ ] Entender quais tabelas são de 2ª instância
- [ ] Verificar período de dados disponível
- [ ] Entender frequência de atualização

### 2.2 — Explorar o CSV Bulk (TJRJ.csv)
- [ ] Localizar arquivo CSV mais recente
- [ ] Analisar estrutura completa (todas as colunas)
- [ ] Filtrar e contar registros G2
- [ ] Comparar campos do CSV vs campos da API vs campos do PostgreSQL
- [ ] Identificar quais campos são indicadores calculados pelo CNJ

### 2.3 — Explorar a API Pública
- [ ] Testar consulta básica filtrando TJRJ + G2
- [ ] Verificar quantidade de processos retornados
- [ ] Verificar se movimentos estão completos
- [ ] Documentar limitações práticas (paginação, rate limit, sigilosos)

---

## Fase 3: Análise Comparativa Ampliada

### 3.1 — Processos: Interno vs DataJud
- [ ] Extrair lista de processos G2 do EJUD (via Iceberg bronze)
- [ ] Extrair lista de processos G2 do EPROC2G (via Iceberg bronze)
- [ ] Extrair lista de processos G2 do DataJud (PostgreSQL ou CSV)
- [ ] Cruzar: processos no EJUD/EPROC2G que **não existem** no DataJud
- [ ] Cruzar: processos no DataJud que **não existem** no EJUD/EPROC2G
- [ ] Quantificar divergências por sistema de origem

### 3.2 — Movimentos: Interno vs DataJud
- [ ] Para amostra de processos, comparar movimentos internos vs DataJud
- [ ] Verificar se mapeamento MNI está gerando códigos corretos
- [ ] Identificar movimentos internos sem mapeamento MNI
- [ ] Identificar movimentos no DataJud sem correspondência interna

### 3.3 — Metas: Nosso cálculo vs CNJ
- [ ] Comparar metas calculadas por `2inst_metacnj` vs indicadores do Painel CNJ
- [ ] Documentar divergências com causa raiz

---

## Fase 4: Tabelas Canônicas (modelo 1inst adaptado)

Adaptar subconjunto do modelo canônico de 1inst para 2inst (EJUD + EPROC2G).
Referência: `iceberg/datajud_cnj/docs/der-banco-datajud.md`

### 4.1 — Processo + Vigências (F1)
- [ ] `tb_datajud_processo` — dados base do processo
- [ ] `tb_datajud_processo_oj_vigencia` — vigência de OJ (distribuição/redistribuição)
- [ ] `tb_datajud_processo_classe_vigencia` — vigência de classe (mov 14738/14739)
- [ ] Adaptar lógica de vigência para fontes EJUD/EPROC2G

### 4.2 — Eventos (F3)
- [ ] `tb_datajud_processo_evento` — movimentos
- [ ] `tb_datajud_evento_complemento` — complementos consolidados
- [ ] Aplicar transformações MPS (24→193, 29→3, 30→11009)

### 4.3 — Ingestão Bronze
- [ ] Definir fonte primária (PostgreSQL? CSV? API?)
- [ ] Criar script de ingestão para camada bronze Iceberg
- [ ] Definir tabela(s) de destino e particionamento
- [ ] Implementar carga incremental ou full

### 4.4 — Transformação Silver
- [ ] Criar tabela prata com processos DataJud G2 normalizados
- [ ] Explodir movimentos (1 linha por movimento)
- [ ] Mapear campos para nomenclatura interna
- [ ] Adicionar chave de cruzamento com EJUD/EPROC2G

---

## Fase 5: Geração XML (objetivo final)

- [ ] Gerar XML no formato XSD 1.2 (MTD) a partir das tabelas canônicas
- [ ] Respeitar vigência de OJ/classe para atribuir aos eventos
- [ ] Aplicar todas as transformações MPS
- [ ] Validar XML gerado contra XSD schema
- [ ] Comparar XML gerado vs XML que MPS envia atualmente

---

## Fase 6: Automação e Monitoramento

- [ ] Integrar no Airflow (EMR)
- [ ] Pipeline CI/CD (Azure DevOps)
- [ ] Alertas de divergência acima de threshold
- [ ] Relatório mensal de qualidade DataJud

---

## Decisões Pendentes

| Decisão | Opções | Impacto |
|---------|--------|---------|
| Fonte para `datamart.id_situacao_atual` | PostgreSQL (se tem)? CSV bulk (tem)? API (consulta)? | Define como extrair situação CNJ |
| Fonte primária dados brutos | PostgreSQL (7 dims lêem) vs CSV (dump) vs API (tempo real) | Arquitetura de ingestão |
| Frequência de execução | Mensal (alinhado metas) vs Semanal vs Diário | Infra e custo |
| Relação com 1inst scripts | Reusar PySpark deles? Adaptar consultas SQL? Reescrever? | Falar com engenheiros |

## Perguntas para Engenheiros 1inst

1. `consultas/` folder SQL — vocês usam direto ou PySpark lê e transforma?
2. NULLs e hardcodes nas consultas — são campos não mapeados ainda ou decisão de não enviar?
3. Tabela de vigência OJ/classe — já está rodando em prod? Podemos consultar?
4. `TAB_XML_ANDAMENTO` (MPS) — vocês lêem dessa tabela ou reconstroem dos movimentos?

---

## Skills Necessárias

| Habilidade | Para quê | Status |
|------------|----------|--------|
| SQL (PostgreSQL) | Explorar DB DataJud | Necessário |
| PySpark / EMR | Pipeline ETL Iceberg | Necessário (padrão dos outros 2inst_) |
| Elasticsearch Query DSL | API Pública DataJud | Necessário se usar API |
| Pandas | Análise exploratória CSV | Já usado (scripts existentes) |
| MNI / TPU | Entender mapeamento de movimentos | Necessário (documentação CNJ) |
| Iceberg / Glue Catalog | Tabelas bronze/prata/gold | Necessário (padrão do projeto) |
