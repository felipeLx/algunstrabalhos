-- Regra conservadora:
-- `tb_proc_parte_represntante` mistura representantes processuais com relacoes
-- pessoais/processuais (mae, pai, curador, tutor, inventariante etc.).
-- No XSD, `representanteProcessual` representa advogado/escritorio/MP/DP/
-- advocacia publica, enquanto relacoes pessoais devem ser tratadas em
-- `pessoaProcessualRelacionada`. Por isso, nesta tabela entram apenas registros
-- ativos classificados como ADVOGADO (`id_tipo_representante = 7`) e com cadastro
-- correspondente em `tb_pessoa_advogado`. Os demais tipos ficam para a etapa de
-- `tb_datajud_pessoa_relacionada`/`tb_datajud_pessoa_vinculada`.
with representantes_base as (
    select distinct
           r.id_representante as id_pessoa
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_proc_parte_represntante r
        on r.id_processo_parte = cast(dpp.cd_seq_tb_origem as int)
      join pje.tb_pessoa_advogado pa
        on pa.id = r.id_representante
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
       and r.in_situacao = 'A'
       and r.id_tipo_representante = 7 -- ADVOGADO
),
-- Regra inspirada na view original do PJE: client.vs_nomes_pessoa.
nomes_pessoa as (
    select
           n.id_pessoa,
           n.ds_nome_pessoa,
           row_number() over (
               partition by n.id_pessoa
               order by
                   case n.ic_nome
                       when 'D' then 1
                       when 'C' then 2
                       when 'S' then 3
                       when 'A' then 4
                       else 5
                   end,
                   n.id_ordem desc
           ) as rn
      from (
            select
                   tul.id_usuario as id_pessoa,
                   tul.ds_nome as ds_nome_pessoa,
                   'C' as ic_nome,
                   tul.id_usuario as id_ordem
              from pje.tb_usuario_login tul
             where nullif(trim(tul.ds_nome), '') is not null

            union all

            select
                   pf.id_pessoa_fisica as id_pessoa,
                   pf.ds_nome_social as ds_nome_pessoa,
                   'S' as ic_nome,
                   pf.id_pessoa_fisica as id_ordem
              from pje.tb_pessoa_fisica pf
             where nullif(trim(pf.ds_nome_social), '') is not null

            union all

            select
                   pna.id_pessoa,
                   pna.ds_pessoa_nome_alternativo as ds_nome_pessoa,
                   'A' as ic_nome,
                   pna.id_pessoa_nome_alternativo as id_ordem
              from pje.tb_pessoa_nome_alternativo pna
             where nullif(trim(pna.ds_pessoa_nome_alternativo), '') is not null

            union all

            select
                   pdi.id_pessoa,
                   pdi.ds_nome_pessoa,
                   'D' as ic_nome,
                   pdi.id_pessoa_doc_identificacao as id_ordem
              from pje.tb_pess_doc_identificacao pdi
             where nullif(trim(pdi.ds_nome_pessoa), '') is not null
           ) n
),
documento_principal as (
    select
           pdi.id_pessoa,
           regexp_replace(trim(pdi.nr_documento_identificacao), '[^0-9]', '') as numero_documento_principal,
           row_number() over (
               partition by pdi.id_pessoa
               order by
                   case when pdi.in_principal = true then 0 else 1 end,
                   pdi.id_pessoa_doc_identificacao desc
           ) as rn
      from pje.tb_pess_doc_identificacao pdi
     where pdi.in_ativo = true
       and coalesce(pdi.in_usado_falsamente, false) = false
       and pdi.cd_tp_documento_identificacao = 'CPF'
       and length(regexp_replace(trim(pdi.nr_documento_identificacao), '[^0-9]', '')) = 11
),
dados_oab as (
    select
           pa.id as id_pessoa,
           regexp_replace(coalesce(pa.nr_oab, ''), '[^0-9]', '') as nr_oab_digitos,
           nullif(
               regexp_replace(
                   upper(coalesce(nullif(trim(pa.in_tipo_inscricao), ''), nullif(trim(pa.ds_letra_oab), ''))),
                   '[^A-Z]',
                   ''
               ),
               ''
           ) as tipo_inscricao,
           uf.cd_estado as uf_oab
      from pje.tb_pessoa_advogado pa
      left join pje.tb_estado uf
        on uf.id_estado = pa.id_uf_oab
)
select
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(rb.id_pessoa as string) as cd_seq_tb_origem,
       np.ds_nome_pessoa as nome,
       case
           when oab.uf_oab is not null
            and length(oab.nr_oab_digitos) between 1 and 7
            and oab.tipo_inscricao is not null
           then concat(oab.uf_oab, lpad(oab.nr_oab_digitos, 7, '0'), substr(oab.tipo_inscricao, 1, 1))
       end as inscricao_oab,
       dp.numero_documento_principal,
       cast(null as boolean) as intimacao_preferencial,
       cast('A' as string) as tipo_representante
  from representantes_base rb
  join nomes_pessoa np
    on np.id_pessoa = rb.id_pessoa
   and np.rn = 1
  left join documento_principal dp
    on dp.id_pessoa = rb.id_pessoa
   and dp.rn = 1
  left join dados_oab oab
    on oab.id_pessoa = rb.id_pessoa;
