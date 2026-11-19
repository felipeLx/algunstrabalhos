with pessoas_base as (
    select distinct
           p.id_pessoa,
           p.id_tipo_pessoa,
           p.in_tipo_pessoa
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_processo_parte pp
        on cast(pp.id_processo_parte as string) = dpp.cd_seq_tb_origem
      join pje.tb_pessoa p
        on p.id_pessoa = pp.id_pessoa
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
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
           pdi.nr_documento_identificacao,
           pdi.ds_nome_pessoa,
           pdi.nm_pai,
           pdi.nm_mae,
           pdi.dt_nascimento,
           row_number() over (
               partition by pdi.id_pessoa
               order by
                   case when pdi.in_ativo = true then 0 else 1 end,
                   case when pdi.in_principal = true then 0 else 1 end,
                   case when pdi.nr_documento_identificacao is not null then 0 else 1 end,
                   pdi.id_pessoa_doc_identificacao desc
           ) as rn
      from pje.tb_pess_doc_identificacao pdi
),
email_pessoa as (
    select
           mc.id_pessoa,
           lower(trim(mc.vl_meio_contato)) as email,
           row_number() over (
               partition by mc.id_pessoa
               order by mc.id_meio_contato desc
           ) as rn
      from pje.tb_meio_contato mc
      join pje.tb_tipo_contato tc
        on tc.id_tipo_contato = mc.id_tipo_contato
     where mc.id_tipo_contato = 3
       and tc.ds_tipo_contato = 'E-mail'
       and lower(trim(mc.vl_meio_contato)) rlike '^[a-z0-9._%+-]+@[a-z0-9.-]+[.][a-z]{2,}$'
)
select
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(pb.id_pessoa as string) as cd_seq_tb_origem,
       coalesce(
           nullif(trim(np.ds_nome_pessoa), ''),
           nullif(trim(dp.ds_nome_pessoa), ''),
           nullif(trim(pf.ds_nome_social), ''),
           nullif(trim(pj.nm_fantasia), '')
       ) as nome,
       case
           when upper(trim(pf.in_sexo)) in ('M', 'F', 'D') then upper(trim(pf.in_sexo))
           else null
       end as sexo,
       coalesce(nullif(trim(pf.nm_genitor), ''), nullif(trim(dp.nm_pai), '')) as nome_genitor,
       coalesce(nullif(trim(pf.nm_genitora), ''), nullif(trim(dp.nm_mae), '')) as nome_genitora,
       coalesce(pf.dt_nascimento, dp.dt_nascimento) as data_nascimento,
       cast(pf.dt_obito as date) as data_obito,
       nullif(trim(dp.nr_documento_identificacao), '') as numero_documento_principal,
       case
           when pb.in_tipo_pessoa = 'F' then 'fisica'
           when pb.in_tipo_pessoa = 'J' then 'juridica'
           when pb.in_tipo_pessoa = 'A' then 'autoridade'
           else null
       end as tipo_pessoa,
       -- O XSD aceita Branco(a), Pardo(a), Preto(a), Indigena, Amarelo(a) e Nao declarado.
       -- `Quilombola` existe no PJE, mas nao existe no enum do XSD; mantemos nulo para nao distorcer o dado.
       case rc.id_racacor
           when 1 then 'Branco(a)'
           when 2 then 'Preto(a)'
           when 3 then 'Amarelo(a)'
           when 4 then 'Indígena'
           when 5 then 'Pardo(a)'
       end as raca_cor,
       m.ds_municipio as cidade_natural,
       e.cd_estado as estado_natural,
       case
           when pb.in_tipo_pessoa <> 'F' then 'BR'
           when pf.id_municipio_nascimento is not null then 'BR'
           when upper(trim(pa.ds_sigla)) rlike '^[A-Z]{2}$' then upper(trim(pa.ds_sigla))
           else 'BR'
       end as nacionalidade,
       ep.email,
       cast(null as boolean) as populacao_de_rua
  from pessoas_base pb
  left join pje.tb_pessoa_fisica pf
    on pf.id_pessoa_fisica = pb.id_pessoa
  left join pje.tb_pessoa_juridica pj
    on pj.id_pessoa_juridica = pb.id_pessoa
  left join nomes_pessoa np
    on np.id_pessoa = pb.id_pessoa
   and np.rn = 1
  left join documento_principal dp
    on dp.id_pessoa = pb.id_pessoa
   and dp.rn = 1
  left join email_pessoa ep
    on ep.id_pessoa = pb.id_pessoa
   and ep.rn = 1
  left join pje.tb_municipio m
    on m.id_municipio = pf.id_municipio_nascimento
  left join pje.tb_estado e
    on e.id_estado = m.id_estado
  left join pje.tb_pais pa
    on pa.id_pais = pf.id_pais
  left join pje.tb_racacor rc
    on rc.id_racacor = pf.id_racacor
   and rc.in_ativo = true
;
