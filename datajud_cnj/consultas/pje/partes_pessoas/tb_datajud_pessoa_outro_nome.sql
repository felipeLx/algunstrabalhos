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
nomes_alternativos as (
    select
           pna.id_pessoa,
           pna.id_pessoa_nome_alternativo,
           nullif(trim(pna.ds_pessoa_nome_alternativo), '') as outro_nome,
           lower(trim(regexp_replace(
               translate(
                   pna.ds_pessoa_nome_alternativo,
                   'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ',
                   'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn'
               ),
               '[^A-Za-z0-9]+',
               ' '
           ))) as outro_nome_normalizado
      from pje.tb_pessoa_nome_alternativo pna
     where nullif(trim(pna.ds_pessoa_nome_alternativo), '') is not null
),
nomes_alternativos_dedup as (
    select
           na.*,
           row_number() over (
               partition by na.id_pessoa, na.outro_nome_normalizado
               order by na.id_pessoa_nome_alternativo desc
           ) as rn_dup
      from nomes_alternativos na
)
select
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(na.id_pessoa as string) as cd_seq_tb_origem,
       na.outro_nome
  from pessoas_base pb
  join nomes_alternativos_dedup na
    on na.id_pessoa = pb.id_pessoa
  left join nome_principal np
    on np.id_pessoa = pb.id_pessoa
 where na.rn_dup = 1
   and (
        na.outro_nome_normalizado <> np.nome_principal_normalizado
        or np.nome_principal_normalizado is null
   )
;
