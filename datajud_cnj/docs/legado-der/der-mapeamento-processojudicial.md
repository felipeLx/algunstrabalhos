# DER x XSD 1.2 (processojudicial) - mapeamento e lacunas

Origem XSD: `utils/modelo-de-transferencia-de-dados-1.2.xsd`
Origem DER: `docs/DER.png`
Escopo: somente o ramo `processojudicial` (tipoProcessoJudicial e dependencias).

## 1) Resumo do DER (entidades)
- Processo: `tb_processo`
- Assuntos: `processo_assunto`
- Magistrados: `processo_magistrado`
- Vinculos: `processo_vinculado`
- Relacao incidental: `processo_rel_incidental`
- Prioridade do processo: `processo_prioridade`
- BO e inquerito: `processo_bol_ocorrencia`, `processo_inq_policial`
- Partes/personagens: `tb_processo_personagem`, `tb_personagem`
- Advogados: `tb_personagem_advogado`, `tb_advogado`
- Documentos pessoais: `tb_personagem_documento`
- Enderecos: `tb_personagem_endereco`
- Prioridade da parte: `tb_personagem_prioridade`
- Movimentos: `tb_processo_evento`, `tb_evento_complemento`, `tb_evento_oj`, `tb_evento_classe`

## 2) Mapeamento principal XSD -> DER

### 2.1 Processo (tipoCabecalhoProcesso)
- `numero` (tipoNumeroUnico) -> `tb_processo.nr_processo`
- `classeProcessual` -> `tb_processo.cd_classe`
- `competencia` -> `tb_processo.cd_comp`
- `nivelSigilo` -> `tb_processo.cd_nivel_sigilo`
- `dataAjuizamento` -> `tb_processo.dt_ajuizamento`
- `intervencaoMP` -> `tb_processo.fl_intervencao_mp`
- `procEl` (1 eletronico / 2 fisico) -> `tb_processo.ind_proc_eletr` (requer mapeamento)
- `juizo100Digital` -> `tb_processo.fl_juizo_100_digital`
- `grau` -> `tb_processo.cd_grau` (requer mapeamento para SUP/G2/TR/G1/JE/TRU/TNU/TEU/CJF/CSJT)
- `pedidoPrioridade` -> `tb_processo.fl_pedido_prioridade`
- `valorCausa` -> `tb_processo.valor_causa`
- `custasRecolhidas` -> `tb_processo.custasRecolhidas`
- `assunto` + atributo `principal` -> `processo_assunto` (`cd_assunto`, `fl_assunto_principal`)
  - O campo `tb_processo.cd_assunto_princ` pode ser redundante com `processo_assunto`.

### 2.2 Polo/parte/personagem
- `tipoPoloProcessual.polo` -> `tb_processo_personagem.sg_tipo_polo`
- `tipoParte` (parte) -> `tb_processo_personagem` (uma linha por parte)
- `assistenciaJudiciaria` -> `tb_processo_personagem.fl_assist_judiciaria`
- `tipoPessoa` -> `tb_personagem`
  - `nome` -> `tb_personagem.ds_nome`
  - `sexo` -> `tb_personagem.sg_sexo` (valores M/F/D)
  - `dataNascimento` -> `tb_personagem.dt_nascimento`
  - `dataObito` -> `tb_personagem.dt_obito`
  - `numeroDocumentoPrincipal` -> `tb_personagem.ds_cpf` (requer regra para CNPJ/RIC)
  - `tipoPessoa` -> `tb_personagem.sg_tipo_pessoa` (fisica/juridica/autoridade/orgaorepresentacao/ente despersonalizado)
  - `racaCor` -> `tb_personagem.ds_raca_cor` (validar enumeracoes)
  - `nacionalidade` -> `tb_personagem.sg_nacionalidade`
  - `populacaoderua` -> `tb_personagem.fl_pop_rua`

### 2.3 Documentos pessoais e enderecos
- `tipoDocumentoIdentificacao` -> `tb_personagem_documento`
  - `codigoDocumento` -> `ds_documento`
  - `emissorDocumento` -> `ds_emissor`
  - `tipoDocumento` -> `sg_tipo_documento`
- `tipoEndereco` -> `tb_personagem_endereco`
  - `cep` -> `cd_cep`
  - `logradouro` -> `ds_logradouro`
  - `numero` -> `ds_numero`
  - `complemento` -> `ds_complemento`
  - `bairro` -> `ds_bairro`
  - `cidade` -> `ds_cidade`
  - `codCidade` -> `cd_cidade`
  - `estado` -> `sg_estado`
  - `pais` -> `sg_pais`

### 2.4 Advogados / representantes processuais
- `tipoRepresentanteProcessual` -> `tb_personagem_advogado` + `tb_advogado`
  - `nome` -> `tb_advogado.ds_nome`
  - `numeroDocumentoPrincipal` / `inscricao` / `tipoRepresentante` / `intimacao` -> NAO presentes no DER (ver lacunas)

### 2.5 Vinculos e relacoes
- `processoVinculado` (tipoVinculacaoProcessual) -> `processo_vinculado`
  - `numeroProcesso` -> `nr_processo_vinculado`
  - `vinculo` -> `sg_tp_vinculo`
- `relacaoIncidental` (tipoRelacaoIncidental) -> `processo_rel_incidental`
  - `numeroProcesso` -> `nr_processo_inc`
  - `tipoRelacao` -> `sg_tp_rel_inc`
  - `classeProcessual` -> `cd_classe_inc`

### 2.6 Prioridades
- `tipoPrioridade` (processo) -> `processo_prioridade.sg_tp_prioridade`
- `prioridadeProcessoParte` (parte) -> `tb_personagem_prioridade`
  - `tipo` -> `sg_tipo_prioridade`
  - `dataConcessao` -> `dt_ini_prioridade`
  - `dataFim` -> `dt_fim_prioridade`

### 2.7 BO e inquerito
- `numeroBoletimOcorrencia` -> `processo_bol_ocorrencia.ds_ident_bol_ocorrencia`
- `numeroInqueritoPolicial` -> `processo_inq_policial.ds_ident_inq_pol`

### 2.8 Movimentos processuais
- `tipoMovimentoProcessual` -> `tb_processo_evento`
  - `dataHora` -> `dtl_mov`
  - `responsavelMovimento` -> `cd_resp_mov` (se armazenar CPF)
  - `tipoResponsavelMovimento` -> `cd_tp_resp_mov`
  - `idDocumentoVinculado` -> NAO identificado no DER
- `movimentoNacional` / `movimentoLocal` -> `tb_processo_evento.cd_mov`
  - Observacao: o DER nao indica se o movimento e nacional ou local. Pode ser necessario uma regra de mapeamento.
- `complemento` / `complementoNacional` -> `tb_evento_complemento`
  - `codComplemento` -> `cd_compl`
  - `codComplementoTabelado` -> `cd_compl_tab`
  - `descricaoComplemento` -> NAO presente; pode exigir tabela de referencia
  - `valor` -> `ds_vlr_compl` (se usado como texto de complemento)
- `orgaoJulgador` / `orgaoJulgadorColegiado` -> `tb_evento_oj`
  - `cd_oj` (e dados origem) representam o orgao julgador do movimento
- `classeProcessual` (no movimento) -> `tb_evento_classe.cd_classe`

## 3) Lacunas e pontos de adaptacao

### 3.1 Processo (cabecalho)
- `siglaTribunal`, `codigoLocalidade`, `outrosnumeros` e `tamanhoProcesso` nao aparecem no DER.
- `dscSistema` no XSD e enumerado (1..13). O DER possui `cd_sist_origem` (origem local). E necessario definir mapeamento.
- `orgaoJulgador` atual (no cabecalho) nao aparece; apenas em movimentos (`tb_evento_oj`).

### 3.2 Partes/pessoas
- `interessePublico` (choice em `tipoParte`) nao aparece.
- `intimacaoPendente` e `relacionamentoProcessual` nao aparecem.
- `pessoaProcessualRelacionada`, `pessoaRelacionada` e `pessoaVinculada` (relacoes) nao aparecem.
- `email`, `nomeGenitor`, `nomeGenitora`, `cidadeNatural`, `estadoNatural` nao aparecem.

### 3.3 Representantes processuais
- Faltam campos para `tipoRepresentante`, `inscricao` (OAB), `numeroDocumentoPrincipal` e `intimacao`.
  - Sugestao: estender `tb_advogado` ou criar tabela `tb_representante`.

### 3.4 Movimentos
- Faltam: `nivelSigilo` do movimento, `tipoDecisao`, `identificadorMovimento`, `classeProcessual` (v1.1), `idDocumentoVinculado`.
- Nao ha distincao explicita entre movimento nacional e local.
- `descricaoComplemento` (complemento nacional) nao existe; precisa tabela de referencia ou campo adicional.

### 3.5 Documentos processuais
- O XSD possui `tipoDocumento` (conteudo, assinatura, mimetype, hash, vinculos). Nao ha tabelas no DER para isso.
  - Sugestao: criar `tb_documento` e `tb_documento_assinatura`, com relacionamento ao processo e/ou movimento.

### 3.6 Prioridades
- Processo: so `processo_prioridade.sg_tp_prioridade` (sem datas). XSD nao exige datas para processo, ok.
- Parte: `tb_personagem_prioridade` contem datas, atende `prioridadeProcessoParte`.

## 4) Recomendacoes objetivas
1) Definir mapeamento formal `cd_sist_origem` -> `dscSistema` (XSD) e, se necessario, manter o codigo local em `outroParametro`.
2) Adicionar entidade para `orgaoJulgador` atual do processo (cabecalho), se for exigido no envio.
3) Incluir modelo de documentos processuais (`tipoDocumento`) e vinculo com movimentos.
4) Tratar relacoes entre partes/pessoas (pessoaProcessualRelacionada, pessoaRelacionada, pessoaVinculada) se forem relevantes na consolidacao.
5) Padronizar dominos (sexo, tipoPessoa, racaCor, tipoPrioridade, polo) para aderir ao XSD.
6) Completar campos de representante processual (OAB, tipoRepresentante, numeroDocumentoPrincipal).

---

Se quiser, posso:
- ajustar o DER com novas entidades/campos sugeridos;
- gerar um mapeamento campo-a-campo em formato CSV;
- propor regras de transformacao (ETL) por sistema de origem.
