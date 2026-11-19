select pe.nr_processo,
       pe.cd_sist_origem,
       pe.cd_tb_origem,
       pe.cd_seq_tb_origem,
       pe.cd_evento,
       cast(ttc.cd_tipo_complemento as int) as cd_compl,
       cast(tcs.ds_texto as varchar) as cd_compl_tab,
       cast(tcs.ds_valor_complemento as varchar) as ds_vlr_compl,
       pe.dth_evento,
       pe.fl_evento_ativo
  from prata.tb_datajud_processo_evento pe
  join pje.tb_complemento_segmentado tcs
    on cast(pe.cd_seq_tb_origem as int) = tcs.id_movimento_processo
  join pje.tb_tipo_complemento ttc
    on tcs.id_tipo_complemento = ttc.id_tipo_complemento
 where pe.cd_sist_origem = 2
   and format_datetime(cast(pe.dth_evento as timestamp), 'yyyyMM') = '{anomes}';
