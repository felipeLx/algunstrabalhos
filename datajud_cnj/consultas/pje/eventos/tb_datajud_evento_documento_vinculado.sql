select distinct
       pdp.nr_processo,
       cast(2 as int) as cd_sist_origem,
       cast(1 as int) as cd_tb_origem, -- pje.tb_processo_evento
       cast(tpe.id_processo_evento as string) as cd_seq_tb_origem,
       cast(tpe.id_processo_documento as string) as id_documento
  from dw_1inst_prata.tb_datajud_processo pdp
  join pje.tb_processo tp
    on cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
  join pje.tb_processo_evento tpe
    on tpe.id_processo = tp.id_processo
 where tpe.in_ativo = true
   and tpe.id_processo_documento is not null
   and tpe.id_processo_documento > 0
   and date_format(tpe.dt_atualizacao, 'yyyyMM') = '{anomes}';
