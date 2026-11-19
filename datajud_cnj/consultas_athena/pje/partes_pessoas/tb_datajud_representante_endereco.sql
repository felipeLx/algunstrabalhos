-- Regra conservadora alinhada a `tb_datajud_representante_processual`:
-- considera apenas representantes processuais ADVOGADO (`id_tipo_representante = 7`)
-- ativos e cadastrados em `tb_pessoa_advogado`.
-- O endereco vem da parte tecnica do representante (`id_parte_representante`),
-- validada contra `tb_processo_parte_endereco`. Nao usamos fallback textual de
-- `tb_cep` para logradouro/bairro/complemento.
with representantes_base as (
    select distinct
           r.id_proc_parte_representante,
           r.id_representante,
           r.id_parte_representante
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_proc_parte_represntante r
        on r.id_processo_parte = cast(dpp.cd_seq_tb_origem as integer)
      join pje.tb_pessoa_advogado pa
        on pa.id = r.id_representante
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
       and r.in_situacao = 'A'
       and r.id_tipo_representante = 7 -- ADVOGADO
),
enderecos_base as (
    select
           rb.id_representante,
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
               partition by rb.id_representante, e.id_endereco
               order by rb.id_proc_parte_representante desc
           ) as rn
      from representantes_base rb
      join pje.tb_processo_parte_endereco ppe
        on ppe.id_processo_parte = rb.id_parte_representante
      join pje.tb_endereco e
        on e.id_endereco = ppe.id_endereco
      left join pje.tb_cep c
        on c.id_cep = e.id_cep
      left join pje.tb_municipio m
        on m.id_municipio = c.id_municipio
      left join pje.tb_estado uf
        on uf.id_estado = m.id_estado
)
select distinct
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(eb.id_representante as varchar) as cd_seq_tb_origem,
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
