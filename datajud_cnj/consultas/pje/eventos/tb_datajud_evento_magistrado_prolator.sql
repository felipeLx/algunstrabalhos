select distinct
       pdp.nr_processo,
       cast(2 as int) as cd_sist_origem,
       cast(1 as int) as cd_tb_origem, -- pje.tb_processo_evento
       cast(tpe.id_processo_evento as string) as cd_seq_tb_origem,
       cast(regexp_replace(trim(tul.ds_login), '[^0-9]', '') as string) as cpf_magistrado
  from dw_1inst_prata.tb_datajud_processo pdp
  join pje.tb_processo tp
    on cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
  join pje.tb_processo_evento tpe
    on tpe.id_processo = tp.id_processo
  join pje.tb_evento te
    on te.id_evento = tpe.id_evento
  join ouro.tb_dim_cnj_movimento tdcm
    on cast(te.cd_evento as int) = cast(tdcm.cd_mov as int)
  left join pje.tb_usuario_login tul
    on tul.id_usuario = tpe.id_usuario
 where tpe.in_ativo = true
   and tpe.id_usuario <> 0
   and concat(':', coalesce(tdcm.breadcrumb, ''), ':') like '%:193:%' -- filhos de 193 (julgamento)
   and cast(tdcm.cd_mov as int) <> 193
   and date_format(tpe.dt_atualizacao, 'yyyyMM') = '{anomes}'
   and regexp_replace(coalesce(trim(tul.ds_login), ''), '[^0-9]', '') rlike '^[0-9]{11}$'
;
