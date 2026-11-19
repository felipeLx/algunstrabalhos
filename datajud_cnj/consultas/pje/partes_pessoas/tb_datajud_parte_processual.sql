select distinct
       pdp.nr_processo,
       cast(2 as int) as cd_sist_origem,
       cast(38 as int) as cd_tb_origem,
       cast(pp.id_processo_parte as string) as cd_seq_tb_origem,
       case
           when pp.id_tipo_parte = 272 then 'AD'
           when pp.id_tipo_parte = 268 then 'CI'
           when pp.id_tipo_parte = 277 then 'FL'
           when pp.id_tipo_parte in (80, 287, 288) then 'TJ'
           when pp.id_tipo_parte in (271, 276) then 'VI'
           when pp.id_tipo_parte = 10 then 'TC'
           when pp.in_participacao = 'A' then 'AT'
           when pp.in_participacao = 'P' then 'PA'
           when pp.in_participacao = 'T' then 'TC'
           else null
       end as tipo_polo,
       coalesce(pp.in_justica_gratuita, false) as assistencia_judiciaria,
       cast(null as date) as assistencia_deferimento,
       cast(null as date) as assistencia_revogacao,
       cast(null as int) as intimacao_pendente,
       cast(null as string) as relacionamento_processual,
       cast(null as string) as interesse_publico
  from dw_1inst_prata.tb_datajud_processo pdp
  join pje.tb_processo tp
    on cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
  join pje.tb_processo_trf tpt
    on tp.id_processo = tpt.id_processo_trf
  join pje.tb_processo_parte pp
    on pp.id_processo_trf = tpt.id_processo_trf
 where tpt.cd_processo_status = 'D';
