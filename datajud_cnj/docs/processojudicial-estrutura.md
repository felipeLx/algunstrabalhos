# Processo judicial (XSD 1.2) - Estrutura consolidada

Origem: `utils/modelo-de-transferencia-de-dados-1.2.xsd`
Escopo: somente o ramo `processojudicial`, a partir do `complexType` `tipoProcessoJudicial` e seus tipos dependentes.

## 1) Raiz do ramo
`tipoProcessoJudicial`
- Elementos:
  - `dadosBasicos` (tipoCabecalhoProcesso) [1..1]
  - `movimento` (tipoMovimentoProcessual) [0..*]
  - `documento` (tipoDocumento) [0..*]

## 2) Dados basicos do processo
`tipoCabecalhoProcesso`
- Elementos:
  - `polo` (tipoPoloProcessual) [1..*]
  - `assunto` (tipoAssuntoProcessual) [1..*]
  - `magistradoAtuante` (tipoCadastroIdentificador) [0..*]
  - `processoVinculado` (tipoVinculacaoProcessual) [0..*]
  - `relacaoIncidental` (tipoRelacaoIncidental) [0..*]
  - `siglaTribunal` (string) [0..1]
    - Observacao: a documentacao lista valores possiveis (STF, STJ, TJ*, TRF*, TRT*, TRE*, etc.), mas nao ha restricao de schema.
  - `grau` (string) [0..1]
    - Valores documentados: SUP, G2, TR, G1, JE, TRU, TNU, TEU, CJF, CSJT.
  - `validaProcesso` (string) [0..1]
  - `pedidoPrioridade` (boolean) [0..1] (v1.1)
  - `tipoPrioridade` (tipoPrioridade) [0..*] (v1.1)
  - `prioridade` (string) [0..*] (legado v1.0; nao usar a partir de v1.1)
  - `outroParametro` (tipoParametro) [0..*]
  - `valorCausa` (double) [0..1]
  - `custasIniciais` (double) [0..1]
  - `custasRecursais` (double) [0..1]
  - `custasFinais` (double) [0..1]
  - `custasRecolhidas` (double) [0..1]
    - Observacao: substitui os demais campos de custas (v1.1).
  - `orgaoJulgador` (tipoOrgaoJulgador) [1..1]
  - `outrosnumeros` (string) [0..*]
  - `numeroBoletimOcorrencia` (string) [0..*] (v1.2)
  - `numeroInqueritoPolicial` (string) [0..*] (v1.2)

- Atributos:
  - `numero` (tipoNumeroUnico) [obrigatorio]
  - `competencia` (int) [opcional]
  - `classeProcessual` (int) [obrigatorio]
  - `codigoLocalidade` (string) [opcional] (IBGE 7 digitos)
  - `nivelSigilo` (int) [obrigatorio]
  - `intervencaoMP` (boolean) [opcional]
  - `tamanhoProcesso` (int) [opcional] (bytes)
  - `dataAjuizamento` (tipoDataHora) [opcional]
  - `procEl` (int) [opcional] (1=eletronico, 2=fisico)
  - `dscSistema` (int) [opcional] (1..13 conforme documentacao; ex.: 1=PJe, 2=Projudi, 3=SAJ, 4=EPROC, 5=Apolo, 6=Themis, 7=Libra, 8=Outros, 9=SEEU, 10=DCP, 11=EJUD, 12=SCPV, 13=Tucujuris)
  - `juizo100Digital` (boolean) [opcional] (v1.1)
  - `anoEleicao` (int) [opcional] (v1.1; exclusivo justica eleitoral)

## 3) Polo, parte e pessoa
`tipoPoloProcessual`
- Elementos:
  - `parte` (tipoParte) [1..*]
- Atributos:
  - `polo` (modalidadePoloProcessual) [opcional]
    - Valores: AT, PA, TC, FL, TJ, AD, VI, CI (v1.1)

`tipoParte`
- Choice (1):
  - `pessoa` (tipoPessoa) [1..1]
  - `interessePublico` (string) [1..1]
- Elementos:
  - `advogado` (tipoRepresentanteProcessual) [0..*]
  - `pessoaProcessualRelacionada` (tipoParte) [0..*] (recursivo)
  - `prioridade` (prioridadeProcessoParte) [0..*] (v1.2)
- Atributos:
  - `assistenciaJudiciaria` (boolean) [opcional] (v1.1)
  - `assistenciaJudiciariaDeferimento` (tipoData) [opcional] (v1.1)
  - `assistenciaJudiciariaRevogacao` (tipoData) [opcional] (v1.1)
  - `intimacaoPendente` (int >= 0) [opcional]
  - `relacionamentoProcessual` (modalidadeRelacionamentoProcessual) [opcional]
    - Valores: CP, RP, TF, AT, AS

`tipoPessoa`
- Elementos:
  - `outroNome` (string) [0..*]
  - `documento` (tipoDocumentoIdentificacao) [0..*]
  - `endereco` (tipoEndereco) [0..*]
  - `pessoaRelacionada` (tipoRelacionamentoPessoal) [0..*] (recursivo)
  - `pessoaVinculada` (tipoPessoa) [0..1] (recursivo)
- Atributos:
  - `nome` (string) [obrigatorio]
  - `sexo` (modalidadeGeneroPessoa: M/F/D) [obrigatorio]
  - `nomeGenitor` (string) [opcional]
  - `nomeGenitora` (string) [opcional]
  - `dataNascimento` (tipoData) [opcional]
  - `dataObito` (tipoData) [opcional]
  - `numeroDocumentoPrincipal` (tipoCadastroIdentificador) [opcional]
  - `tipoPessoa` (tipoQualificacaoPessoa) [obrigatorio]
    - Valores: fisica, juridica, autoridade, orgaorepresentacao, ente despersonalizado
  - `racaCor` (tipoRacaCorPessoa) [opcional]
    - Valores: Branco(a), Pardo(a), Preto(a), Indigena, Amarelo(a), Nao declarado
  - `cidadeNatural` (string) [opcional]
  - `estadoNatural` (UF 2 letras) [opcional]
  - `nacionalidade` (ISO-3166-1-alpha-2, default BR) [opcional]
  - `email` (string) [opcional]
  - `populacaoderua` (boolean) [opcional]

`tipoRepresentanteProcessual`
- Elementos:
  - `endereco` (tipoEndereco) [0..*]
- Atributos:
  - `nome` (string) [obrigatorio]
  - `inscricao` (tipoCadastroOAB) [opcional]
  - `numeroDocumentoPrincipal` (string) [opcional]
  - `intimacao` (boolean) [opcional]
  - `tipoRepresentante` (modalidadeRepresentanteProcessual) [obrigatorio]
    - Valores: A, E, M, D, P

## 4) Assuntos do processo
`tipoAssuntoProcessual`
- Choice (1):
  - `codigoNacional` (int) [1..1]
  - `assuntoLocal` (tipoAssuntoLocal) [1..1]
- Atributo:
  - `principal` (boolean, default=false) [opcional]

`tipoAssuntoLocal`
- Elementos:
  - `assuntoLocalPai` (tipoAssuntoLocal) [0..1]
- Atributos:
  - `codigoAssunto` (int) [obrigatorio]
  - `codigoPaiNacional` (int) [obrigatorio]
  - `descricao` (string) [obrigatorio]

## 5) Vinculacoes e relacoes incidentais
`tipoVinculacaoProcessual`
- Atributos:
  - `numeroProcesso` (tipoNumeroUnico) [obrigatorio]
  - `vinculo` (modalidadeVinculacaoProcesso) [obrigatorio]
    - Valores: CX, CT, DP, AR, CD, RR, RG, OR

`tipoRelacaoIncidental`
- Atributos:
  - `numeroProcesso` (tipoNumeroUnico) [opcional]
  - `tipoRelacao` (string) [opcional] (PP=principal, PI=incidental)
  - `classeProcessual` (int) [opcional]

## 6) Movimento processual
`tipoMovimentoProcessual`
- Elementos:
  - `complemento` (tipoComplemento) [0..*]
  - Choice (1):
    - `movimentoNacional` (tipoMovimentoNacional) [1..1]
    - `movimentoLocal` (tipoMovimentoLocal) [1..1]
  - `complementoNacional` (tipoComplementoNacional) [0..*]
  - `magistradoProlator` (tipoCadastroIdentificador) [0..*]
  - `idDocumentoVinculado` (string) [0..*]
  - `orgaoJulgador` (tipoOrgaoJulgador) [0..1]
  - `orgaoJulgadorColegiado` (tipoOrgaoJulgador) [0..1] (v1.1)
- Atributos:
  - `dataHora` (tipoDataHora) [obrigatorio]
  - `nivelSigilo` (int) [opcional]
  - `identificadorMovimento` (string) [opcional]
  - `responsavelMovimento` (tipoCadastroIdentificador) [opcional]
  - `tipoResponsavelMovimento` (int) [opcional] (0=Servidor, 1=Magistrado, 2=Automatico)
  - `tipoDecisao` (int) [opcional] (0=Monocratica, 1=Colegiada)
  - `classeProcessual` (int) [opcional] (v1.1)

`tipoMovimentoNacional`
- Elementos:
  - `complemento` (tipoComplemento) [0..*]
- Atributos:
  - `codigoNacional` (int) [obrigatorio]

`tipoMovimentoLocal`
- Elementos:
  - `movimentoLocalPai` (tipoMovimentoLocal) [0..1]
- Atributos:
  - `codigoMovimento` (int) [obrigatorio]
  - `codigoPaiNacional` (int) [obrigatorio]
  - `descricao` (string) [obrigatorio]

`tipoComplementoNacional`
- Atributos:
  - `codComplemento` (int) [obrigatorio]
  - `descricaoComplemento` (string) [obrigatorio]
  - `codComplementoTabelado` (int) [opcional]

## 7) Documento processual
`tipoDocumento`
- Elementos:
  - `conteudo` (base64Binary) [0..1]
  - `assinatura` (tipoAssinatura) [0..*]
  - `outroParametro` (tipoParametro) [0..*]
  - `documentoVinculado` (tipoDocumento) [0..*] (recursivo)
- Atributos:
  - `idDocumento` (string) [opcional]
  - `idDocumentoVinculado` (string) [opcional]
  - `tipoDocumento` (string) [obrigatorio]
  - `dataHora` (tipoDataHora) [opcional]
  - `mimetype` (string) [opcional]
  - `nivelSigilo` (int) [opcional]
  - `movimento` (int) [opcional]
  - `hash` (string) [opcional]
  - `descricao` (string) [opcional]
  - `tipoDocumentoLocal` (string) [opcional]

`tipoAssinatura`
- Elementos:
  - `signatarioLogin` (tipoSignatarioSimples) [0..*]
- Atributos:
  - `assinatura` (string) [opcional]
  - `dataAssinatura` (tipoDataHora) [opcional]
  - `cadeiaCertificado` (string) [opcional]
  - `algoritmoHash` (string) [opcional]
  - `codificacaoCertificado` (string) [opcional]

`tipoSignatarioSimples`
- Atributos:
  - `identificador` (string) [opcional]
  - `dataHora` (tipoDataHora) [opcional]

## 8) Tipos base e formatos
- `tipoNumeroUnico`: string com 20 digitos (`\d{20}`)
- `tipoData`: `AAAAMMDD`
- `tipoDataHora`: `AAAAMMDDHHMMSS`
- `tipoCadastroIdentificador`: documentacao indica 11/14 digitos numericos (CPF/CNPJ/RIC), sem regex ativa no schema
- `tipoEndereco`: campos textuais opcionais e atributo `cep` (8 digitos)

## 9) Prioridade (processo e parte)
`tipoPrioridade` (v1.1)
- Valores: ID, RP, CA, PE, DG, VD, LC, OUT, PCD, FRJ, AT, TE, PS, PSR, VT, AMS, DI

`prioridadeProcessoParte` (v1.2)
- Elementos:
  - `tipo` (tipoPrioridade) [1..1]
  - `dataConcessao` (tipoData) [0..1]
  - `dataFim` (tipoData) [0..1]

## 10) Observacoes gerais
- Algumas restricoes sao apenas documentais (ex.: listas em `siglaTribunal`, `dscSistema`).
- Campos legados: `prioridade` (v1.0) permanece no XSD, mas a recomendacao e nao usar desde v1.1.
- Ha recursao em `tipoDocumento` (documentoVinculado), `tipoParte` (pessoaProcessualRelacionada) e `tipoPessoa` (pessoaVinculada e pessoaRelacionada).

