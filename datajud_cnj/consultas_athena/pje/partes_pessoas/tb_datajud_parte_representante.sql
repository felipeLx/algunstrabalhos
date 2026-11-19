-- Regra conservadora alinhada a `tb_datajud_representante_processual`:
-- cria apenas o vinculo da parte com representantes processuais do tipo
-- ADVOGADO (`id_tipo_representante = 7`) ativos e cadastrados em
-- `tb_pessoa_advogado`. Os demais tipos de `tb_proc_parte_represntante`
-- representam relacoes pessoais/processuais ou papeis ambiguos e serao tratados
-- nas tabelas de pessoa relacionada/vinculada.
select
       dpp.nr_processo,
       cast(2 as int) as cd_sist_origem_parte,
       cast(38 as int) as cd_tb_origem_parte,
       dpp.cd_seq_tb_origem as cd_seq_tb_origem_parte,
       cast(2 as int) as cd_sist_origem_rep,
       cast(39 as int) as cd_tb_origem_rep,
       cast(r.id_representante as varchar) as cd_seq_tb_origem_rep
  from dw_1inst_prata.tb_datajud_parte_processual dpp
  join pje.tb_proc_parte_represntante r
    on r.id_processo_parte = cast(dpp.cd_seq_tb_origem as integer)
  join pje.tb_pessoa_advogado pa
    on pa.id = r.id_representante
 where dpp.cd_sist_origem = 2
   and dpp.cd_tb_origem = 38
   and r.in_situacao = 'A'
   and r.id_tipo_representante = 7 -- ADVOGADO
 group by
       dpp.nr_processo,
       dpp.cd_seq_tb_origem,
       r.id_representante;
