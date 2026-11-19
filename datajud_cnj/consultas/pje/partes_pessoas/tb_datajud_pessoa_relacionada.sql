-- Regra conservadora:
-- `tb_relacao_pessoal` e `tb_tipo_relacao_pessoal` existem no PJE original,
-- mas estao vazias; `tb_pessoa_filiacao` possui apenas nome textual da filiacao,
-- sem `id_pessoa` da pessoa relacionada. Por isso, usamos `tb_proc_parte_represntante`
-- apenas para tipos que casam diretamente com o enum pessoal do XSD:
-- MAE/PAI -> P, TUTOR -> T, CURADOR/CURADOR ESPECIAL -> C.
-- Tipos ambiguos ou processuais ficam fora ate validacao de negocio.
with pessoas_datajud as (
    select distinct
           pp.id_pessoa
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_processo_parte pp
        on pp.id_processo_parte = cast(dpp.cd_seq_tb_origem as int)
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
),
relacoes as (
    select
           pp.id_pessoa,
           r.id_representante as id_pessoa_relacionada,
           case
               when r.id_tipo_representante in (248, 244) then 'P'
               when r.id_tipo_representante = 78 then 'T'
               when r.id_tipo_representante in (3, 252) then 'C'
           end as modalidade_relacionamento
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_processo_parte pp
        on pp.id_processo_parte = cast(dpp.cd_seq_tb_origem as int)
      join pje.tb_proc_parte_represntante r
        on r.id_processo_parte = pp.id_processo_parte
      join pessoas_datajud pd_rel
        on pd_rel.id_pessoa = r.id_representante
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
       and r.in_situacao = 'A'
       and r.id_tipo_representante in (248, 244, 78, 3, 252)
       and pp.id_pessoa <> r.id_representante
)
select distinct
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(r.id_pessoa as string) as cd_seq_tb_origem,
       cast(2 as int) as cd_sist_origem_relacionada,
       cast(39 as int) as cd_tb_origem_relacionada,
       cast(r.id_pessoa_relacionada as string) as cd_seq_tb_origem_relacionada,
       cast(r.modalidade_relacionamento as string) as modalidade_relacionamento
  from relacoes r;
