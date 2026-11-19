4. Relatório de Produtividade dos Magistrados (PROD)
Este requisito detalha os indicadores de desempenho, status e latência que devem ser contabilizados e atribuídos aos magistrados e órgãos julgadores.
4.1. Colunas Essenciais e Atribuição de Ato
Colunas Essenciais: O relatório deve incluir todas as colunas essenciais e atributos comuns definidos no item 2.2 do DLR.
Atribuição de Ato (Obrigatória): É mandatório que a contagem da produtividade seja atribuída ao Magistrado que proferiu o ato. A coluna de referência para essa atribuição é COD_MAG (Magistrado responsável pelo lançamento do movimento).
4.2. Indicadores de Processos 
ID	Indicador	Regra
1	Processos Distribuídos	Processos que receberam a fase EJUD '26 – Distribuídos' e não receberam cancelamento de distribuição no período de análise
2	Processos Redistribuídos	Processos que receberam a fase EJUD '36 – Redistribuição'  e não receberam cancelamento de redistribuição no período de análise.
3	Processos Julgados	Processos que receberam as fases EJUD '193 – Julgamento' ou '50193 – Julgamento Monocrático' no período de análise.
4	Processos Decididos	Processos que receberam as fases E-JUD '3 – Decisão' ou '60003 – Decisão em Sessão' no período de análise.
5	Processos não Julgados	Processos que constam no acervo (conforme RF.6.1 - A01) e não receberam as fases de Julgamento (193, 50193) até o final do período de análise.
6	Processos não Decididos	Processos que constam no acervo (conforme RF.6.1 - A01) e não receberam as fases de Decisão (3, 60003) até o final do período de análise.
Estes indicadores devem ser contados por processo (COD_DOCUMENTO) e refletem o status ou a entrada de processos no período de análise:

4.3. Indicadores de Votos Proferidos (fases)
Estes indicadores devem ser contados por ocorrência da fase (ato processual), refletindo a quantidade de atos individuais realizados pelo magistrado designado (COD_MAG), e não por processo:
ID	Indicador	Regra
1	Votos Proferidos – 
Julgamento Colegiado	Ocorrências da fase E-JUD '193 – Julgamento' no período de análise.
2	Votos Proferidos – 
Decisão em Sessão	Ocorrências da fase E-JUD '60003 – Decisão em Sessão' no período de análise.
3	Votos Proferidos – 
Julgamento Monocrático	Ocorrências da fase E-JUD '50193 – Julgamento Monocrático' no período de análise.
4	Votos Proferidos – 
Decisões	Ocorrências da fase E-JUD '3 – Decisão' no período de análise.


4.4. Indicadores de Latência (Tempo Médio de Conclusão) 
Este indicador tem por objetivo apurar o tempo médio de permanência do processo no gabinete do Magistrado, limitando o cálculo ao período do mês de análise.
O cálculo deverá descontar o dia inicial da contagem e considerar o último. As horas deverão ser desconsideradas para simplificação de cálculo. 
4.4.1. Marcos de contagem do indicador:
Marco Inicial: será considerada como marco inicial a fase ‘51 – conclusão’.
Marco final: serão considerados quaisquer movimentos que retiram o processo do gabinete.
Descontos: serão desconsiderados os processos que estejam com o indicador de suspensos ‘S’. 
4.4.2.  Colunas Essenciais e Atribuição de Ato
Colunas Essenciais: o relatório deve incluir todas as colunas essenciais e atributos comuns definidos no item 2.2 do DLR.
Atribuição de Ato (Obrigatória): é mandatório que a contagem de tempo seja atribuída ao magistrado que abriu a conclusão. É sugerida que a tabela tenha as colunas de:
ID	COLUNA	Regra
1	DIAS_CORRIDOS	Tempo de permanência do processo no Gabinete em dias corridos, desconsiderando o primeiro dia e incluindo o último dia, truncando as horas.
2	DIAS_DESCONTOS	Contagem de dias descontados de permanência do processo na conclusão.
3	SALDO_DIAS	Calculo de dias entre os indicadores de ‘DIAS_CORRIDOS’ e ‘DIAS_DESCONTOS’.

4.4.3. Exemplo de Contagem de Tempo Médio de Mensal de Conclusão
O cálculo do tempo de tramitação de um processo, que teve sua conclusão registrada em 28 de julho de 2025 e sua saída efetivada em 10 de agosto de 2025, deve ser segmentado entre os dois meses envolvidos para o cálculo da média mensal.
Para o mês de julho, o tempo médio deve computar o período que se inicia no dia seguinte à conclusão (28/07), ou seja, a partir de 29 de julho até 31 de julho. Nesse cálculo, o dia da conclusão é descontado, seguindo a regra de que o primeiro dia não é incluído na contagem.
Já para o mês de agosto, o tempo médio deverá abranger o período completo, iniciando em 1º de agosto e se estendendo até 10 de agosto, incluindo tanto o primeiro dia quanto o último dia (saída) na contagem dos dias.
Mês de Análise	Período de Calculo	Regra aplicada	Calculo	Resultado
jul/25	28/07 a 31/07	Desconta o primeiro dia (28/07), e considera o último dia do período (31/07). O limite do cálculo é o fim do mês.	Dias contados: 29,30,31	3 Dias
ago/25	01/08 a 10/08	Considera-se o período completo dentro do mês de agosto, incluindo o primeiro dia (01/08) e o dia da saída (10/08).	Dias contados: 1,2,3,4,5,6,7,8,9,10	10Dias

5. Relatório de Tempo Médio da Distribuição até o Primeiro Julgamento ou Decisão
Este cálculo estabelece as regras para a medição do tempo de tramitação, que se inicia na data de entrada do processo e se estende até o primeiro ato decisório ou de julgamento, com a aplicação obrigatória de desconto para o tempo de suspensão.
5.1. Requisitos Essenciais
Colunas Essenciais: o relatório deve incluir todas as colunas essenciais e atributos comuns definidos no item 2.2 do DLR.
Atribuição de Ato (Obrigatória): É mandatório que a contagem da produtividade (julgamento ou decisão) seja atribuída ao magistrado que proferiu o ato. A coluna de referência para essa atribuição é COD_MAG (Magistrado responsável pelo lançamento do movimento). É sugerida que a tabela tenha as colunas de:
ID	COLUNA	Regra
1	DIAS_CORRIDOS	Contagem de dias compreendido entre o marco inicial e final em dias corridos.
2	DIAS_DESCONTOS	Contagem de dias descontados por suspensão/sobrestamento.
3	SALDO_DIAS	Calculo de dias entre o marco final – marco inical – descontos.

5.2. Regras de Negócio para o Marco Zero (Data Inicial)
O ponto de partida para o cálculo do tempo médio (Marco Zero) deve seguir as seguintes regras, garantindo o tratamento correto para processos que são redistribuídos:
5.2.1. Regra Geral (Ponto de Partida): Na maioria dos casos, a data inicial de contagem deve ser a data e hora do lançamento da fase ejud ‘26 – Distribuição’.
5.2.2. Exceção para Redistribuição sem Ato: Caso o processo seja redistribuído e não possua registro de Julgamento ou Decisão anterior à nova distribuição, o Marco Zero para o cálculo passa a ser a data e hora da fase ejud ‘36 – Redistribuição’.
5.2.3. Regra de Exclusão: Processos que já tiverem sido Julgados ou Decididos e que, posteriormente, receberem uma nova Redistribuição, devem ser desconsiderados (excluídos) do cálculo de tempo médio para os indicadores deste relatório.
5.2.4. Desconto por Suspensão/Sobrestamento (NOVO): O cálculo da diferença de dias entre o Marco Zero e a Data Final (Julgamento/Decisão) deve EXCLUIR (descontar) todo o tempo em que o processo esteve formalmente suspenso ou sobrestado.
5.2.5. Desconto Aplicável: O cálculo da latência deve descontar o período compreendido entre a data de lançamento da Fase de Entrada na Suspensão e a data de lançamento da Fase de Saída da Suspensão.
5.3. Indicadores de Latência
O relatório deve apresentar os seguintes indicadores de tempo médio, retornando o valor em dias, com duas casas decimais:
5.3.1. Tempo médio até o primeiro julgamento (2.1): Deve calcular o tempo médio entre o Marco Zero (Distribuição ou Redistribuição*) até o primeiro lançamento das fases ‘193 – Julgamento’ ou ‘50193 – Decisão em Sessão’.
5.3.2. Tempo médio até a primeira decisão (2.2): Deve calcular o tempo médio entre o Marco Zero (Distribuição ou Redistribuição*) até o primeiro lançamento das fases ‘3 – Decisão’ ou ‘60003 – Decisão em Sessão’.
Exemplo de cálculo:
O resultado do tempo médio deverá ser retornado no mês de referência em que o magistrado proferiu o ato de decisão ou julgamento. 
Movimento	Data Movimento	Regra aplicada	Calculo	Resultado
Distribuição 	25/01/2026	Marco inicial - desconta o primeiro dia (25/01).	Dias contados: 
Jan - 26,27,28,29,30,31;
Fev - 01,02,03,04,05,06	12 dias
Suspensão	07/02/2026	Desconto - por Suspensão/Sobrestamento	Dias de desconto:
Fev: 07,08,09,10,11,12	6 dias
Julgado	13/02/2026	Marco Final - considera o último dia na contagem. 	Dias contados:
Fev - 13	1 dia
			Saldo em dias	13 dias

---

## 6. Implementação Técnica – `main_produtividade.py` (EJUD)

Esta seção descreve a implementação técnica do script `main_produtividade.py` e seu calculador `EjudFactTableCalculator`, detalhando os indicadores calculados, as colunas geradas e a lógica aplicada.

### 6.1. Visão Geral da Execução

O script é orquestrado pela classe `ProdutividadeOrchestrator`, que:

1. Determina o período de análise (`dt_ini` = primeiro dia do mês, `dt_fim` = último dia do mês, `dt_ref` = último dia do mês).
2. Inicializa uma `SparkSession` com configurações otimizadas de memória.
3. Instancia o `EjudFactTableCalculator` com o caminho base `/opt/parquet/data/bronze/ejud`.
4. Executa sequencialmente os 6 indicadores abaixo.
5. Persiste cada resultado na tabela PostgreSQL `dw2inst.ejud_produtividade`.
6. Registra início, fim e status da carga via `modulo.controle_carga`.

**Uso:**
```
python main_produtividade.py --year 2024 --month 11
```

---

### 6.2. Schema de Saída – Tabela `dw2inst.ejud_produtividade`

Todos os 6 indicadores produzem o mesmo schema de saída:

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 1 | `cod_documento` | STRING | Identificador único do processo (`coddoc` normalizado) |
| 2 | `cod_cnj` | STRING | Número CNJ formatado (7+2+4+1+2+4 dígitos) |
| 3 | `cod_classe` | STRING | Código da classe processual (`codtipproc`) |
| 4 | `cod_sigilo` | STRING | Nível de sigilo: `5`=supersigilo, `4`=sigilo, `1`=segredo de justiça, `0`=público |
| 5 | `cod_assunto_princ` | INTEGER | Código do assunto principal (`numord=1` em `processoassunto`) |
| 6 | `cod_fase` | STRING | Código da fase mapeado: fase interna `24` → `50193`; fase `20` → `60003`; demais mantidos |
| 7 | `dth_fase` | TIMESTAMP | Data/hora do lançamento da fase (`dthrmov`) |
| 8 | `cod_prim_oj` | STRING | Código do primeiro órgão julgador (`codorgjulg`) ao longo do histórico do processo |
| 9 | `cod_prim_relator` | STRING | Código do primeiro relator (`codmagrel`) ao longo do histórico do processo |
| 10 | `cod_ult_oj` | STRING | Código do último órgão julgador no período |
| 11 | `cod_ult_relator` | STRING | Código do último relator no período |
| 12 | `cod_mag` | STRING | Magistrado responsável pelo ato (regra varia por indicador – ver §6.3) |
| 13 | `ind_originario` | STRING | `S` se o CNJ termina em `0000` (processo originário), caso contrário `N` |
| 14 | `indicador` | STRING | Nome do indicador: `distribuicao`, `redistribuicao`, `julgamento`, `decisao`, `baixa`, `emandamento` |
| 15 | `dth_referencia` | TIMESTAMP | Data de referência do snapshot (`dt_ref`) |
| 16 | `ano_mes` | STRING | Período de análise no formato `YYYY_MM` |

**Observações:**
- `cod_cnj` é construído concatenando: `LPAD(numsequencial,7)` + `LPAD(dv,2)` + `numano` + `codjustica` + `codregi` + `LPAD(codlocregi,4)`.
- Competências excluídas (padrão): `1, 2, 9, 10, 11, 12, 14, 15, 19, 20, 21, 22`.
- Processos constantes em `judmigracao` são excluídos (exclusão via `left_anti`).
- Somente processos presentes em `tab_distribuicao` são mantidos (exclusão via `inner join`).

---

### 6.3. Indicadores Calculados

#### 6.3.1. `distribuicao` – Processos Distribuídos

| Atributo | Valor |
|----------|-------|
| Tabela fonte principal | `movdistribuicao` |
| Fases | Configuradas em `variaveis.distribuicao` (ex.: `[26]`) |
| Janela temporal | `dt_ini` a `dt_fim` (mês de análise) |
| Deduplicação | Um registro por `coddoc` – mantém o **mais antigo** (`dthrmov ASC`) |
| `cod_mag` | `codmagrel` do `movimentorelator` ativo no momento da distribuição |
| Relator histórico | `movimentorelator` carregado desde `01/01` do ano corrente |

**Lógica resumida:**
1. Carrega fases de distribuição de `movdistribuicao` no período.
2. Identifica primeiro e último relator via janelas sobre `movimentorelator`.
3. Junta a distribuição com o relator ativo no momento do evento.
4. Enriquece com dados de `t_processo` e `orgaojulgador`.
5. Deduplica mantendo a distribuição mais antiga por processo.

---

#### 6.3.2. `redistribuicao` – Processos Redistribuídos

| Atributo | Valor |
|----------|-------|
| Tabela fonte principal | `movdistribuicao` |
| Fases | Configuradas em `variaveis.redistribuicao` (ex.: `[36]`) |
| Janela temporal | `dt_ini` a `dt_fim` (mês de análise) |
| Deduplicação | Um registro por `coddoc` – mantém o **mais antigo** (`dthrmov ASC`) |
| `cod_mag` | `codmagrel` mais recente (`<= dthrmov` da redistribuição) de `movimentorelator` |
| Relator histórico | `movimentorelator` carregado desde `01/01` do ano corrente |

**Lógica resumida:** idêntica à distribuição, mas usando fases de redistribuição. O relator ativo é determinado pela janela `orderBy(rel.dthrmov DESC)` agrupada por processo e data do evento.

---

#### 6.3.3. `julgamento` – Processos Julgados

| Atributo | Valor |
|----------|-------|
| Tabelas fonte | `movjulg`, `movjulgparti`, `movimento`, `movdespa` |
| Fases | `FASE_ACORDAO` = `[193, 24]` (configurável via `variaveis.julgado`) |
| Janela temporal | `dt_ini` a `dt_fim` (mês de análise) |
| Deduplicação | Um registro por `coddoc` – mantém o **mais antigo** (`dthrmov ASC`) |
| `cod_mag` | Resolvido por prioridade (ver branches abaixo) |
| Filtro especial | Exclui magistrados aposentados (`historicomagistrado` com `codtipevntmag=2` e `dtinic < dt_ini`) |

**Três branches de cálculo (UNION DISTINCT):**

| Branch | Condição | `cod_mag` resolvido |
|--------|----------|---------------------|
| 1 – Julgamento padrão | `movimento.codfase IN (193, 24)` sem `indmagvista='S'` | `COALESCE(MovJulgParti[indmagdesig='S'].codmag, mj.codmagdesig, mj.codmagrel)` |
| 2 – Segundo Relator | Igual ao Branch 1, mas com `movjulg.coddec2 IS NOT NULL` | `COALESCE(MovJulgParti[indmagdesig2='S'].codmag, mj.codmagrel)` |
| 3 – Julgamento Monocrático | `movdespa.codfase = 24` | `movdespa.codmag` (fase mapeada para `cod_fase = 50193`) |

---

#### 6.3.4. `decisao` – Processos Decididos

| Atributo | Valor |
|----------|-------|
| Tabela fonte principal | `movdespa` |
| Fases | Configuradas em `variaveis.decisao` (fases internas `3`, `20`; `20` → `cod_fase = 60003`) |
| Janela temporal | `dt_ini` a `dt_fim` (carregamento interno desde `01/01` do ano anterior) |
| Deduplicação | Um registro por `coddoc` – mantém o **mais recente** (`dthrmov DESC`) |
| `cod_mag` | `movdespa.codmag` (magistrado que despachou) |
| Filtro especial | Exclui magistrados aposentados (`codtipevntmag=2`, `dtinic < dt_ini`) |

**Lógica resumida:**
1. Carrega `movdespa` com fases de decisão e `codmag IS NOT NULL`.
2. Localiza o relator mais recente (`<= dthrmov`) de `movimentorelator`.
3. Exclui magistrados aposentados.
4. Enriquece com `t_processo` e `orgaojulgador`.
5. Deduplica mantendo a decisão mais recente por processo.

---

#### 6.3.5. `baixa` – Processos Baixados

| Atributo | Valor |
|----------|-------|
| Tabela fonte principal | `movimento` |
| Fases simples | `22, 870, 50011, 50014` (configurável via `variaveis.baixa.simple_phases`) |
| Fases carregadas | `22, 870, 50011, 50014, 861, 50002, 60, 123, 50123` (`variaveis.baixa.all_phases_for_loading`) |
| Fases complexas | Regras dinâmicas via `variaveis.baixa.complex_rules` (combinações de `codfase`, `codcompl1`, `codcompl2`, `codlocal`) |
| Filtro obrigatório | `fase.indfaseext = 'S'` (join com tabela dimensão `fase`) |
| Tabela adicional | `movimentofisico` (LEFT JOIN por `coddoc`+`dthrmov` para obter `codlocal`) |
| Janela temporal | `dt_ini` a `dt_fim` (mês de análise) |
| Deduplicação | **Não aplicada** – múltiplos registros de baixa por processo são permitidos |
| `cod_mag` | `movimentorelator.codmagrel` mais recente (`<= dthrmov` da baixa) |

---

#### 6.3.6. `emandamento` – Processos em Andamento (Acervo)

| Atributo | Valor |
|----------|-------|
| Tabelas fonte | `movdistribuicao`, `movimento` |
| Fases de distribuição | `FASE_EMANDAMENTO_DIST` = `[26, 36]` |
| Fases finalizadoras | `FASE_EMANDAMENTO_BAIXA` = `[3, 20, 24, 193]` |
| Janela temporal | `01/01/(ano-1)` a `dt_fim` (year-to-date ampliado) |
| Deduplicação | Um registro por `coddoc` – mantém a distribuição **mais recente** e depois o relator **mais antigo** |
| `cod_mag` | `movimentorelator.codmagrel` mais recente no período |
| Critério de inclusão | Processo com distribuição mais recente e **sem** movimento finalizador posterior a ela |

**Lógica resumida:**
1. Carrega `movdistribuicao` com fases `[26, 36]` no intervalo year-to-date.
2. Seleciona a distribuição **mais recente** por processo (`rn=1 DESC`).
3. Compara com `movimento` de fases `[3, 20, 24, 193]`; usa `left_anti` para manter apenas processos **sem** movimento finalizador após a última distribuição.
4. Enriquece com relator, `t_processo` e `orgaojulgador`.
5. Deduplica mantendo o registro mais antigo por processo.

---

### 6.4. Tabelas de Origem (Parquet Bronze)

| Tabela | Uso |
|--------|-----|
| `movdistribuicao` | Distribuição e redistribuição (`distribuicao`, `redistribuicao`, `emandamento`) |
| `movimento` | Fases gerais e baixa (`baixa`, `emandamento`) |
| `movjulg` | Julgamentos colegiados (`julgamento` branches 1 e 2) |
| `movjulgparti` | Participantes do julgamento – `indmagvista`, `indmagdesig`, `indmagdesig2` (`julgamento`) |
| `movdespa` | Despachos e julgamentos monocrático (`decisao`, `julgamento` branch 3) |
| `movimentorelator` | Histórico de relatores – primeiro/último/ativo no evento |
| `movimentofisico` | Localização física do processo (`codlocal`) para regras complexas de baixa |
| `t_processo` | Dimensão processo: `cod_cnj`, `cod_classe`, `cod_sigilo`, `ind_originario` |
| `processoassunto` | Assunto principal do processo (`numord=1`) → `cod_assunto_princ` |
| `orgaojulgador` | Dimensão órgão julgador: `descr_orgao`, `codcompt`, `cod_grau_juris` |
| `historicomagistrado` | Filtro de magistrados aposentados (`codtipevntmag=2`) |
| `judmigracao` | Tabela de exclusão – processos migrados são removidos de todos os indicadores |
| `fase` | Dimensão de fases – filtra `indfaseext='S'` no indicador de baixa |
| `tab_distribuicao` | Tabela de inclusão – somente processos presentes são considerados |

---

### 6.5. Configuração via `config.json`

Os códigos de fase e competências excluídas são lidos pelo `ConfigManager` a partir de `config/config.json` (chave `variaveis`):

| Chave | Padrão | Indicador |
|-------|--------|-----------|
| `variaveis.distribuicao` | `[26]` | `distribuicao` |
| `variaveis.redistribuicao` | `[36]` | `redistribuicao` |
| `variaveis.julgado` | `[193, 24]` | `julgamento` |
| `variaveis.decisao` | `[3, 20]` | `decisao` |
| `variaveis.baixa.simple_phases` | `[22, 870, 50011, 50014]` | `baixa` |
| `variaveis.baixa.complex_rules` | lista de objetos | `baixa` |
| `variaveis.emandamento.distribution_phases` | `[26, 36]` | `emandamento` |
| `variaveis.emandamento.active_phases` | `[3, 20, 24, 193]` | `emandamento` |
| `variaveis.competencias_excluidas` | `[1,2,9,10,11,12,14,15,19,20,21,22]` | todos |

---

### 6.6. Controle de Carga

| Campo | Valor |
|-------|-------|
| `id_projeto` | `9` |
| `schema` | `dw2inst` |
| `tabela` | `ejud_produtividade` |
| Registro de início | `registrar_inicio_carga(id_projeto, schema, tabela, dt_referencia, home_executor, dt_carga)` |
| Registro de fim | `registrar_fim_carga(...)` com `qt_registros` contados da tabela após carga |
| Status | `SUCESSO` em caso de execução normal; registrado via `registrar_status_carga` |

---

## 7. Implementação Técnica – `eproc2g_prod.py` (eProc 2G)

Esta seção descreve a implementação técnica do script `eproc2g_prod.py` e seu calculador `ProdutividadeCalculator`, detalhando os indicadores calculados, as colunas geradas e a lógica aplicada para o sistema eProc 2G.

### 7.1. Visão Geral da Execução

O script é orquestrado pela classe `ProdutividadeOrchestrator` (idêntica em estrutura à do EJUD), que:

1. Determina o período de análise (`ano_mes` no formato `YYYY_MM`).
2. Inicializa uma `SparkSession` via `SparkSession.builder.appName(...).getOrCreate()`.
3. Instancia o `ProdutividadeCalculator` com o `ConfigManager` e o período de referência.
4. Executa sequencialmente os 6 indicadores abaixo, chamando `fn(ref_year_month=ano_mes)`.
5. Persiste cada resultado na tabela PostgreSQL `dw2inst.eproc2g_produtividade`.
6. Registra início, fim e status da carga via `modulo.controle_carga`.

**Diferença fundamental em relação ao EJUD:** o cálculo é baseado em **MNI (Modelo Nacional de Interoperabilidade)** — `cod_mni_movimento`, `cod_mni_complemento`, `cod_mni_complemento_tabelado` — em vez de fases EJUD numéricas.

**Uso:**
```
python eproc2g_prod.py --year 2024 --month 11
```

---

### 7.2. DataFrame Base – `select_coddoc()`

Todos os 6 indicadores são gerados a partir de um único DataFrame base construído por `select_coddoc()`, que é **cacheado** em `self._base_df` para reuso. A construção envolve os seguintes joins:

| Passo | Tabela | Join | Propósito |
|-------|--------|------|-----------|
| 1 | `processo` | base | Identificação, competência, sigilo, classe, assunto |
| 2 | `relacao_classe_judicial_mni` | INNER on `id_classe_judicial` | Obtém `cod_mni_classe_judicial` (código MNI da classe) |
| 3 | `processo_evento` (movimento) | INNER on `id_processo`+`seq_processo` | Eventos do processo; filtra `dth_evento <= fim_do_mês` e exclui desativados (exceto eventos `40400400` e `80488`) |
| 4 | `evento_judicial` | INNER on `id_evento_judicial` | Obtém `cod_mni_movimento` |
| 5 | `evento_judicial_mni_complemento_tab` | LEFT on `id_evento_judicial` | Obtém `cod_mni_complemento_tabelado` |
| 6 | `mni_complemento_tabelado` | LEFT on `cod_mni_complemento_tabelado` | Obtém `cod_mni_complemento` |
| 7 | `sessao_julgamento_item` | LEFT on `id_processo_evento_resultado`+`id_processo`+`seq_processo` | Liga eventos a itens de sessão de julgamento |
| 8 | `sessao_julgamento_item_magistrado` | INNER on `id_sessao_julgamento_item` (somente `cod_status_magistrado='C'`) | Obtém `id_orgao_juizo_magistrado` → `cod_mag` (magistrado do julgamento) |
| 9 | `historico_distribuicao_judicial` | LEFT on `id_processo`+`seq_processo` | Primeiro e último relator por processo (`cod_prim_relator`, `cod_ult_relator`) |
| 10 | `processo_log` | LEFT on `id_processo` | Primeiro e último órgão colegiado (`cod_prim_oj`, `cod_ult_oj`) via `dth_alteracao` |
| 11 | `classe_judicial` | LEFT on `id_classe_judicial` | Flag de originário (`sin_originario` → `ind_originario`) |

**Regra especial para `cod_mag`:** após o join com a sessão de julgamento, o campo `cod_mag` é **zerado (NULL)** para eventos cujo `cod_mni_movimento` não pertença às fases de `julgado` configuradas — garantindo que somente magistrados de julgamento sejam atribuídos.

**Regra de fallback para `cod_mag` na saída:** `COALESCE(cod_mag, cod_ult_relator)` — se não há magistrado de julgamento atribuído, usa o último relator.

---

### 7.3. Schema de Saída – Tabela `dw2inst.eproc2g_produtividade`

Todos os 6 indicadores produzem o mesmo schema de saída:

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 1 | `cod_documento` | STRING | Identificador do processo (`id_processo`) |
| 2 | `cod_cnj` | STRING | Número do processo (`num_processo`) padronizado com 20 dígitos (LPAD) |
| 3 | `cod_sigilo` | INTEGER | Nível de sigilo (`id_sigilo`) |
| 4 | `cod_complemento` | INTEGER | Complemento MNI do evento (`cod_mni_complemento`) |
| 5 | `cod_tabelado` | INTEGER | Complemento tabelado MNI (`cod_mni_complemento_tabelado`) |
| 6 | `cod_classe` | INTEGER | Código MNI da classe judicial (`cod_mni_classe_judicial`) |
| 7 | `cod_assunto_princ` | INTEGER | ID do assunto principal (`id_assunto_principal`) |
| 8 | `cod_fase` | INTEGER | Código MNI do movimento (`cod_mni_movimento`) |
| 9 | `dth_fase` | TIMESTAMP | Data/hora do evento (`dth_evento`) |
| 10 | `cod_prim_oj` | INTEGER | Primeiro órgão colegiado (`id_orgao_colegiado`, via `processo_log` ASC) |
| 11 | `cod_prim_relator` | INTEGER | Primeiro relator (`cod_magistrado`, via `historico_distribuicao_judicial` ASC) |
| 12 | `cod_ult_oj` | INTEGER | Último órgão colegiado (`id_orgao_colegiado`, via `processo_log` DESC) |
| 13 | `cod_ult_relator` | INTEGER | Último relator (`cod_magistrado`, via `historico_distribuicao_judicial` DESC) |
| 14 | `cod_mag` | LONG | Magistrado responsável pelo ato; `COALESCE(cod_mag_julgamento, cod_ult_relator)` |
| 15 | `ind_originario` | STRING | `S` se `classe_judicial.sin_originario='S'`, caso contrário `N` |
| 16 | `indicador` | STRING | Nome do indicador: `distribuicao`, `redistribuicao`, `julgado`, `decisao`, `baixa`, `emandamento` |
| 17 | `dth_referencia` | TIMESTAMP | Último instante do mês de referência (`last_day(dth_fase) 23:59:59`) |
| 18 | `ano_mes` | STRING | Período de análise no formato `YYYY_MM` |

---

### 7.4. Lógica de Cálculo por Indicador

Todos os indicadores (exceto `emandamento`) compartilham o método genérico `process_variable(movement_codes, variable_name)`:

1. Reutiliza o `_base_df` cacheado.
2. Filtra eventos **do mês de referência** (`date_format(dth_evento, 'yyyy-MM') == period_ref`).
3. Seleciona linhas cujo `cod_mni_movimento` pertence à lista configurada.
4. Aplica deduplicação via `dropDuplicates()`.
5. Adiciona `ano_mes` e `dth_referencia` (último instante do mês da fase).

#### 7.4.1. `distribuicao` – Processos Distribuídos
- Códigos MNI: `variaveis.distribuicao` (ex.: `[10300]`)
- Lógica: eventos do mês com `cod_mni_movimento` nas fases de distribuição.

#### 7.4.2. `redistribuicao` – Processos Redistribuídos
- Códigos MNI: `variaveis.redistribuicao`
- Lógica: idêntica à distribuição, mas com fases de redistribuição.

#### 7.4.3. `julgado` – Processos Julgados
- Códigos MNI: `variaveis.julgado`
- Lógica: eventos do mês com `cod_mni_movimento` nas fases de julgamento. `cod_mag` é preenchido apenas para estas fases (zerando magistrado para outras fases no base df).

#### 7.4.4. `decisao` – Processos Decididos
- Códigos MNI: `variaveis.decisao`
- Lógica: eventos do mês com `cod_mni_movimento` nas fases de decisão.

#### 7.4.5. `baixa` – Processos Baixados
- Códigos MNI simples: `variaveis.baixa`
- **Regras compostas** (`variaveis.baixa_compl.rem`): cada regra especifica `cod_mni_movimento` + `cod_mni_complemento` (ator da remessa) + `cod_mni_complemento_tabelado` (lista de motivos). Os resultados são unidos ao dataset principal via `unionByName`.
- As mesmas regras compostas são reaplicadas no filtro de exclusão do `emandamento`.

#### 7.4.6. `emandamento` – Processos em Andamento (Acervo)
Implementação própria (não usa `process_variable`):

| Passo | Descrição |
|-------|-----------|
| 1 | Carrega o `_base_df` sem filtro de mês (histórico completo até `dt_fim`). |
| 2 | Filtra eventos com `cod_mni_movimento` em `variaveis.distribuicao` ∪ `variaveis.redistribuicao`. |
| 3 | Aplica filtro temporal: `dth_evento <= último_dia_do_mês_referência`. |
| 4 | Seleciona a **distribuição mais recente** por processo (`ROW_NUMBER() DESC` sobre `dth_evento`). |
| 5 | Define os códigos de exclusão: `variaveis.julgado` ∪ `variaveis.baixa` ∪ `variaveis.decisao` + regras `baixa_compl.rem`. |
| 6 | LEFT JOIN com eventos de exclusão onde `excl.dth_evento > distr.dth_evento` (exclusão posterior à distribuição). |
| 7 | Mantém apenas processos sem exclusão posterior (`excl.id_processo IS NULL`). |
| 8 | Deduplicação por `(id_processo, dth_fase)` DESC. |
| 9 | `cod_mag` = `COALESCE(cod_mag, cod_ult_relator)`. |

---

### 7.5. Tabelas de Origem (Parquet Bronze – eProc 2G)

| Tabela | Alias / Função |
|--------|----------------|
| `processo` | Base do processo: `id_processo`, `seq_processo`, `num_processo`, `id_orgao_juizo`, `id_orgao_colegiado`, `id_assunto_principal`, `id_classe_judicial`, `cod_competencia`, `id_sigilo` |
| `processo_evento` (movimento) | Eventos do processo; exclui desativados (exceto `40400400`/`80488`) |
| `evento_judicial` | Decodifica `id_evento_judicial` → `cod_mni_movimento` |
| `evento_judicial_mni_complemento_tab` | Liga evento → `cod_mni_complemento_tabelado` |
| `mni_complemento_tabelado` | Decodifica tabelado → `cod_mni_complemento` |
| `sessao_julgamento_item` | Liga evento de resultado → item de sessão |
| `sessao_julgamento_item_magistrado` | Magistrado votante (`cod_status_magistrado='C'`) → `cod_mag` |
| `historico_distribuicao_judicial` | Primeiro/último relator por processo |
| `processo_log` | Primeiro/último órgão colegiado por processo |
| `relacao_classe_judicial_mni` | Mapeia `id_classe_judicial` → `cod_mni_classe_judicial` |
| `classe_judicial` | Flag `sin_originario` → `ind_originario` |

---

### 7.6. Configuração via `config.json`

| Chave | Indicador |
|-------|-----------|
| `variaveis.distribuicao` | `distribuicao`, `emandamento` (distribuições) |
| `variaveis.redistribuicao` | `redistribuicao`, `emandamento` (redistribuições) |
| `variaveis.julgado` | `julgado`, `emandamento` (exclusão), `cod_mag` (filtro no base df) |
| `variaveis.decisao` | `decisao`, `emandamento` (exclusão) |
| `variaveis.baixa` | `baixa`, `emandamento` (exclusão) |
| `variaveis.baixa_compl.rem` | Regras compostas de baixa: lista de objetos com `cod_mni_movimento`, `cod_mni_complemento`, `cod_mni_complemento_tabelado` |

---

### 7.7. Controle de Carga

| Campo | Valor |
|-------|-------|
| `id_projeto` | `10` |
| `schema` | `dw2inst` |
| `tabela` | `eproc2g_produtividade` |
| Registro de início | `registrar_inicio_carga(id_projeto, schema, tabela, dt_referencia, home_executor, dt_carga)` |
| Registro de fim | `registrar_fim_carga(...)` com `qt_registros` contados da tabela após carga |
| Status | `SUCESSO` em caso de execução normal; registrado via `registrar_status_carga` |

---

### 7.8. Diferenças-Chave: EJUD vs eProc 2G

| Aspecto | EJUD (`ejud_produtividade`) | eProc 2G (`eproc2g_produtividade`) |
|---------|-----------------------------|------------------------------------|
| Sistema-fonte | EJUD (fases numéricas) | eProc 2G (MNI) |
| Identificador de processo | `coddoc` (string normalizado) | `id_processo` (string) |
| Número CNJ | Construído via concat de campos | `num_processo` (LPAD 20 dígitos) |
| Código de fase | `codfase` interno mapeado para externo (`24→50193`, `20→60003`) | `cod_mni_movimento` (MNI direto) |
| Complemento | `codcompl1` / `codcompl2` | `cod_mni_complemento` + `cod_mni_complemento_tabelado` |
| Magistrado do ato | Resolvido por branches (movjulg/movdespa/movdespa) | `sessao_julgamento_item_magistrado` (`cod_status_magistrado='C'`) |
| Primeiro/último relator | `movimentorelator` (janela sobre `dthrmov`) | `historico_distribuicao_judicial` (janela sobre `dth_distribuicao`) |
| Primeiro/último órgão | `movimentorelator.codorgjulg` | `processo_log.id_orgao_colegiado` (janela sobre `dth_alteracao`) |
| Exclusão de migração | `judmigracao` (left_anti) | Não aplicável |
| Tabela de inclusão | `tab_distribuicao` (inner join) | Não aplicável |
| Filtro de competências | Lista configurável excluída | Não aplicável |
| Filtro de magistrado aposentado | `historicomagistrado` (`codtipevntmag=2`) | Não aplicável |
| Baixa complexa | Regras por `codfase`+`codcompl`+`codlocal` via `config.json` | Regras por `cod_mni_movimento`+`cod_mni_complemento`+`cod_mni_complemento_tabelado` (`baixa_compl.rem`) |
| Emandamento – janela temporal | `01/01/(ano-1)` a `dt_fim` | Histórico completo até `último_dia_do_mês` |
| `id_projeto` (controle de carga) | `9` | `10` |
