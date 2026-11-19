-- Regra conservadora:
-- `tb_pessoa_autoridade` representa a vinculacao de uma autoridade
-- (`in_tipo_pessoa = 'A'`) ao orgao/ente/PJ vinculado (`in_tipo_pessoa in ('J', 'A')`).
-- Mantemos apenas vinculos em que as duas pontas tambem existem no universo de
-- `tb_datajud_pessoa`, para evitar referencia a pessoa nao exportada.
-- Nao usamos `tb_pessoa_procuradoria`, pois ela representa cadastro operacional
-- de procuradoria/procurador e gera vinculos muito ruidosos.
with pessoas_datajud as (
    select distinct
           pp.id_pessoa
      from dw_1inst_prata.tb_datajud_parte_processual dpp
      join pje.tb_processo_parte pp
        on pp.id_processo_parte = cast(dpp.cd_seq_tb_origem as integer)
     where dpp.cd_sist_origem = 2
       and dpp.cd_tb_origem = 38
),
vinculos as (
    select distinct
           pa.id_pessoa_autoridade as id_pessoa,
           pa.id_orgao_vinculacao as id_pessoa_vinculada
      from pje.tb_pessoa_autoridade pa
      join pje.tb_pessoa p_aut
        on p_aut.id_pessoa = pa.id_pessoa_autoridade
      join pje.tb_pessoa p_vinc
        on p_vinc.id_pessoa = pa.id_orgao_vinculacao
      join pessoas_datajud pd_aut
        on pd_aut.id_pessoa = pa.id_pessoa_autoridade
      join pessoas_datajud pd_vinc
        on pd_vinc.id_pessoa = pa.id_orgao_vinculacao
     where p_aut.in_tipo_pessoa = 'A'
       and p_vinc.in_tipo_pessoa in ('J', 'A')
       and pa.id_pessoa_autoridade <> pa.id_orgao_vinculacao
)
select distinct
       cast(2 as int) as cd_sist_origem,
       cast(39 as int) as cd_tb_origem,
       cast(v.id_pessoa as varchar) as cd_seq_tb_origem,
       cast(2 as int) as cd_sist_origem_vinculada,
       cast(39 as int) as cd_tb_origem_vinculada,
       cast(v.id_pessoa_vinculada as varchar) as cd_seq_tb_origem_vinculada
  from vinculos v;
