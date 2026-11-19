# DER original (transcricao do DER.png)

Origem: `docs/DER.png`
Objetivo: reproduzir o DER original com os mesmos nomes de tabelas/campos, para comparacao com o DER proposto.

```mermaid
erDiagram
  processo_magistrado {
    string nr_processo
    string cd_magistrado
    date dt_reg_atuacao
  }

  processo_assunto {
    string nr_processo
    int cd_assunto
    boolean fl_assunto_principal
  }

  processo_vinculado {
    string nr_processo
    string nr_processo_vinculado
    string sg_tp_vinculo
  }

  processo_rel_incidental {
    string nr_processo
    string nr_processo_inc
    string sg_tp_rel_inc
    int cd_classe_inc
  }

  processo_prioridade {
    string nr_processo
    string sg_tp_prioridade
  }

  processo_bol_ocorrencia {
    string nr_processo
    string ds_ident_bol_ocorrencia
  }

  processo_inq_policial {
    string nr_processo
    string ds_ident_inq_pol
  }

  tb_processo {
    string nr_processo
    int cd_sist_origem
    int cd_oj
    int cd_comp
    int cd_classe
    int cd_assunto_princ
    int cd_nivel_sigilo
    boolean fl_intervencao_mp
    datetime dt_ajuizamento
    int ind_proc_eletr
    string cd_grau
    boolean fl_pedido_prioridade
    decimal valor_causa
    decimal custasRecolhidas
  }

  tb_evento_oj {
    string nr_processo
    int cd_sist_origem
    int cd_tb_origem
    int cd_seq_tb_origem
    int cd_oj
  }

  tb_evento_classe {
    string nr_processo
    int cd_sist_origem
    int cd_tb_origem
    int cd_seq_tb_origem
    int cd_classe
  }

  tb_processo_evento {
    string nr_processo
    int cd_sist_origem
    int cd_tb_origem
    int cd_seq_tb_origem
    int cd_mov
    int cd_tp_resp_mov
    datetime dtl_mov
    boolean fl_int_ativo
  }

  tb_evento_complemento {
    string nr_processo
    int cd_sist_origem
    int cd_tb_origem
    int cd_seq_tb_origem
    int cd_mov
    int cd_compl
    int cd_compl_tab
    string ds_vlr_compl
    datetime dtl_mov
    boolean fl_int_ativo
  }

  tb_processo_personagem {
    string nr_processo
    int cd_sist_origem
    string cd_identif_personagem
    string sg_tipo_polo
    boolean fl_assist_judiciaria
  }

  tb_personagem {
    int cd_sist_origem
    string cd_identif_personagem
    string sg_tipo_pessoa
    string ds_cpf
    string ds_nome
    string sg_sexo
    date dt_nascimento
    date dt_obito
    string sg_naturalidade
    string sg_nacionalidade
    string ds_raca_cor
    boolean fl_pop_rua
  }

  tb_personagem_documento {
    int cd_sist_origem
    string cd_identif_personagem
    string ds_documento
    string ds_emissor
    string sg_tipo_documento
  }

  tb_personagem_endereco {
    int cd_sist_origem
    string cd_identif_personagem
    string cd_cep
    string ds_logradouro
    string ds_numero
    string ds_complemento
    string ds_bairro
    string ds_cidade
    string cd_cidade
    string sg_estado
    string sg_pais
  }

  tb_personagem_prioridade {
    int cd_sist_origem
    string cd_identif_personagem
    string sg_tipo_prioridade
    date dt_ini_prioridade
    date dt_fim_prioridade
  }

  tb_personagem_advogado {
    string nr_processo
    int cd_sist_origem
    string cd_identif_personagem
    string cd_matr_adv
  }

  tb_advogado {
    string cd_matr_adv
    string ds_nome
  }

  %% Relacoes principais (como no DER.png)
  tb_processo ||--o{ processo_magistrado : possui
  tb_processo ||--o{ processo_assunto : possui
  tb_processo ||--o{ processo_vinculado : vincula
  tb_processo ||--o{ processo_rel_incidental : relaciona
  tb_processo ||--o{ processo_prioridade : prioridade
  tb_processo ||--o{ processo_bol_ocorrencia : registra
  tb_processo ||--o{ processo_inq_policial : registra

  tb_processo ||--o{ tb_processo_evento : movimenta
  tb_processo_evento ||--o{ tb_evento_oj : ocorre_em
  tb_processo_evento ||--o{ tb_evento_classe : classe
  tb_processo_evento ||--o{ tb_evento_complemento : complemento

  tb_processo ||--o{ tb_processo_personagem : parte
  tb_processo_personagem ||--|| tb_personagem : identifica
  tb_personagem ||--o{ tb_personagem_documento : documento
  tb_personagem ||--o{ tb_personagem_endereco : endereco
  tb_personagem ||--o{ tb_personagem_prioridade : prioridade

  tb_processo_personagem ||--o{ tb_personagem_advogado : representa
  tb_personagem_advogado ||--|| tb_advogado : advogado
```

## Observacoes
- Este DER reproduz os nomes de tabelas/campos do `DER.png`.
- Caso algum campo esteja divergente, me sinalize a caixa/tabela para eu ajustar pontualmente.
