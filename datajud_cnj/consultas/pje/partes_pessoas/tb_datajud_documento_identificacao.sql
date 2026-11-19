with pessoas_base as (
    select distinct
           p.id_pessoa
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_processo_parte pp
        on cast(pp.id_processo_parte as string) = dpp.cd_seq_tb_origem
      join pje.tb_pessoa p
        on p.id_pessoa = pp.id_pessoa
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
),
-- Mesma regra de nome principal usada em tb_datajud_pessoa.
nomes_pessoa as (
    select
           n.id_pessoa,
           n.ds_nome_pessoa,
           n.ic_nome,
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
nome_principal as (
    select
           id_pessoa,
           lower(trim(regexp_replace(
               translate(
                   ds_nome_pessoa,
                   'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ',
                   'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn'
               ),
               '[^A-Za-z0-9]+',
               ' '
           ))) as nome_principal_normalizado
      from nomes_pessoa
     where rn = 1
),
documentos_base as (
    select
           pdi.id_pessoa_doc_identificacao,
           pdi.id_pessoa,
           trim(upper(pdi.cd_tp_documento_identificacao)) as tipo_raw,
           upper(regexp_replace(trim(pdi.nr_documento_identificacao), '[^A-Za-z0-9]', '')) as codigo_documento,
           nullif(trim(pdi.ds_orgao_expedidor), '') as emissor_raw,
           nullif(trim(pdi.ds_nome_pessoa), '') as nome_no_documento_raw,
           lower(trim(regexp_replace(
               translate(
                   pdi.ds_nome_pessoa,
                   'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ',
                   'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn'
               ),
               '[^A-Za-z0-9]+',
               ' '
           ))) as nome_no_documento_normalizado,
           pdi.in_principal
      from pessoas_base pb
      join pje.tb_pess_doc_identificacao pdi
        on pdi.id_pessoa = pb.id_pessoa
     where pdi.in_ativo = true
       and coalesce(pdi.in_usado_falsamente, false) = false
       and regexp_replace(trim(pdi.nr_documento_identificacao), '[^A-Za-z0-9]', '') <> ''
),
documentos_calc as (
    select
           db.*,
           case
               when db.tipo_raw = 'CPF' then 'CMF'
               when db.tipo_raw = 'CPJ' then 'CMF'
               when db.tipo_raw = 'RG' then 'CI'
               when db.tipo_raw = 'TIT' then 'TE'
               when db.tipo_raw = 'OAB' then 'OAB'
               when db.tipo_raw = 'CNH' then 'CNH'
               when db.tipo_raw = 'IDF' then 'IF'
               when db.tipo_raw = 'RJI' then 'OUT'
               when db.tipo_raw = 'CNA' then 'CN'
               when db.tipo_raw = 'CCP' then 'CP'
               when db.tipo_raw = 'RJC' then 'RJC'
               when db.tipo_raw = 'PAS' then 'PAS'
               when db.tipo_raw = 'RGE' then 'RGE'
               when db.tipo_raw = 'CTR' then 'CT'
               when db.tipo_raw = 'CCA' then 'CC'
               when db.tipo_raw = 'NIT' then 'NIT'
               when db.tipo_raw = 'PIS' then 'PIS_PASEP'
               else 'OUT'
           end as tipo_documento,
           db.emissor_raw as emissor_documento
      from documentos_base db
),
documentos_ranked as (
    select
           dc.*,
           row_number() over (
               partition by dc.id_pessoa, dc.tipo_documento, dc.codigo_documento
               order by
                   case when dc.in_principal = true then 0 else 1 end,
                   case when dc.emissor_raw is not null then 0 else 1 end,
                   dc.id_pessoa_doc_identificacao desc
           ) as rn
      from documentos_calc dc
)
select
       cast(dr.id_pessoa_doc_identificacao as string) as id_documento_id,
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(dr.id_pessoa as string) as cd_seq_tb_origem,
       dr.codigo_documento,
       dr.emissor_documento,
       dr.tipo_documento,
       case
           when np.nome_principal_normalizado is not null
            and dr.nome_no_documento_normalizado <> np.nome_principal_normalizado
           then dr.nome_no_documento_raw
           else cast(null as string)
       end as nome_no_documento
  from documentos_ranked dr
  left join nome_principal np
    on np.id_pessoa = dr.id_pessoa
 where dr.rn = 1
;
