with partes_base as (
    select distinct
           cast(dpp.cd_seq_tb_origem as int) as id_processo_parte
      from dw_1inst_prata.tb_datajud_parte_processual dpp
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
),
enderecos_base as (
    select
           pp.id_pessoa,
           e.id_endereco,
           regexp_replace(coalesce(c.nr_cep, ''), '[^0-9]', '') as cep_limpo,
           nullif(trim(e.nm_logradouro), '') as logradouro,
           nullif(trim(e.nr_endereco), '') as numero,
           nullif(trim(e.ds_complemento), '') as complemento,
           nullif(trim(e.nm_bairro), '') as bairro,
           nullif(trim(m.ds_municipio), '') as cidade,
           nullif(trim(m.id_municipio_ibge), '') as cod_cidade_ibge,
           nullif(trim(uf.cd_estado), '') as estado,
           row_number() over (
               partition by pp.id_pessoa, e.id_endereco
               order by ppe.id_processo_parte_endereco desc
           ) as rn
      from partes_base pb
      join pje.tb_processo_parte pp
        on pp.id_processo_parte = pb.id_processo_parte
      join pje.tb_processo_parte_endereco ppe
        on ppe.id_processo_parte = pp.id_processo_parte
      join pje.tb_endereco e
        on e.id_endereco = ppe.id_endereco
      left join pje.tb_cep c
        on c.id_cep = e.id_cep
      left join pje.tb_municipio m
        on m.id_municipio = c.id_municipio
      left join pje.tb_estado uf
        on uf.id_estado = m.id_estado
)
select
       concat(cast(eb.id_pessoa as string), '-', cast(eb.id_endereco as string)) as id_endereco,
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(eb.id_pessoa as string) as cd_seq_tb_origem,
       case when length(eb.cep_limpo) = 8 then eb.cep_limpo end as cep,
       eb.logradouro,
       eb.numero,
       eb.complemento,
       eb.bairro,
       eb.cidade,
       eb.cod_cidade_ibge,
       eb.estado,
       case
           when length(eb.cep_limpo) = 8
             or eb.cidade is not null
             or eb.estado is not null
           then 'BR'
       end as pais
  from enderecos_base eb
 where eb.rn = 1;
