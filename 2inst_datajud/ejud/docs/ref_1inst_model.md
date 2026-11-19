# Referência: Modelo Canônico 1inst (datajud_cnj)

Fonte: `iceberg/datajud_cnj/docs/`

## Tabelas por Feature/Prioridade

### F1 - Processo (P1-P2)
| Tabela | Prioridade | Descrição |
|--------|-----------|-----------|
| tb_datajud_processo_oj_vigencia | P1 | Vigência de OJ por processo |
| tb_datajud_processo_classe_vigencia | P1 | Vigência de classe por processo |
| tb_datajud_processo | P2 | Dados base do processo |
| tb_datajud_processo_assunto | P2 | Assuntos (código CNJ) |
| tb_datajud_processo_prioridade | P2 | Tipo prioridade processo |
| tb_datajud_processo_vinculado | P2 | Vínculos entre processos |
| tb_datajud_relacao_incidental | P2 | Relações incidentais |
| tb_datajud_processo_outro_numero | P2 | Números alternativos |
| tb_datajud_processo_parametro | P2 | Parâmetros extras |
| tb_datajud_processo_boletim_ocorrencia | P2 | BOs vinculados |
| tb_datajud_processo_inquerito_policial | P2 | Inquéritos vinculados |

### F3 - Eventos (P1-P4)
| Tabela | Prioridade | Descrição |
|--------|-----------|-----------|
| tb_datajud_processo_evento | P1 | Movimentos processuais |
| tb_datajud_evento_complemento | P1 | Complementos (texto+nacional consolidados) |
| tb_datajud_evento_magistrado_prolator | P2 | Magistrado do movimento |
| tb_datajud_evento_documento_vinculado | P4 | Documento vinculado ao movimento |

### F2 - Partes/Pessoas (P3)
tb_datajud_parte_processual, tb_datajud_pessoa, tb_datajud_pessoa_outro_nome,
tb_datajud_pessoa_relacionada, tb_datajud_pessoa_vinculada, tb_datajud_representante_processual,
tb_datajud_representante_endereco, tb_datajud_parte_representante, tb_datajud_prioridade_parte,
tb_datajud_endereco, tb_datajud_documento_identificacao

### F4 - Documentos (P4)
tb_datajud_documento, tb_datajud_documento_relacao, tb_datajud_documento_parametro,
tb_datajud_assinatura_documento, tb_datajud_signatario_login

## Chaves Compostas

- Processo: `nr_processo` (20 dígitos CNJ)
- Evento: `nr_processo + cd_sist_origem + cd_tb_origem + cd_seq_tb_origem`
- Parte: `nr_processo + cd_sist_origem + cd_tb_origem + cd_seq_tb_origem`
- Pessoa: `cd_sist_origem + cd_tb_origem + cd_seq_tb_origem`

## Tabelas NÃO Materializadas

- `tb_datajud_evento_complemento_texto` → consolidada em evento_complemento
- `tb_datajud_evento_complemento_nacional` → consolidada em evento_complemento
- `tb_datajud_evento_oj` → derivada da vigência OJ do processo
- `tb_datajud_evento_classe` → derivada da vigência classe do processo

## Vigência OJ — Regras

Script: `scripts/2_load_proc_oj_vigencia.py`
- Full: recalcula toda série por processo
- Incremental: identifica processos com distribuição/redistribuição no mês, recalcula só esses
- `dt_ini_vig_oj` = data/hora do movimento de distribuição/redistribuição
- `dt_fim_vig_oj` = `dt_ini_vig_oj` da próxima mudança (NULL se vigente)
- Consecutivos mesmo OJ colapsados
- `cd_oj_cnj` da fonte ou `ouro.tb_dim_serventia` via `cd_oj_tj`

## Vigência Classe — Regras

Script: `scripts/3_load_proc_classe_vigencia.py`
Fontes: `dw_1inst_prata.tb_dcp_processo_evento` + `evento_complemento` + `ouro.tb_dim_cnj_complemento`

- Sem retificação/evolução → usa `tb_datajud_processo.cd_classe`
- Vigência inicial começa no primeiro movimento 26 (distribuição)
- Mov 14739 (Evolução): `classe_nova` vale a partir da data do movimento
- Mov 14738 (Retificação): `classe_nova` corrige retroativamente
  - Sem evolução anterior → vale desde primeiro mov 26
  - Com evolução anterior → vale desde última evolução 14739
- Mesmo início de vigência: prevalece registro mais tarde (retificação sobrescreve)
- Consecutivos mesma classe colapsados

## Notas para 2inst

- Nossos fontes: EJUD (Oracle) + EPROC2G (Postgres) — não DCP/EPROC1G/PJE
- EPROC2G já usa MNI nativo → mapeamento direto
- EJUD precisa: movimento → faseexterna → integracaofasecompl → código CNJ
- Transformações MPS obrigatórias: 24→193, 29→3, 30→11009
