select tdpe.nr_processo,
       tdpe.cd_sist_origem,
       tdpe.cd_tb_origem,
       tdpe.cd_seq_tb_origem,
       tdpe.cd_evento,
       cast(mc.cod_mni_complemento as int) as cd_compl,
       cast(ejmct.cod_mni_complemento_tabelado as varchar) as cd_compl_tab,
       cast(mct.des_mni_complemento_tabelado as varchar) as ds_vlr_compl,
       tdpe.dth_evento,
       tdpe.fl_evento_ativo
  from prata.tb_datajud_processo_evento tdpe
  join eproc1g.processo_evento pe
    on cast(tdpe.cd_seq_tb_origem as decimal(30,0)) = cast(pe.id_processo_evento as decimal(30,0))
  join eproc1g.evento_judicial_mni_complemento_tabelado ejmct
    on pe.id_evento_judicial = ejmct.id_evento_judicial
  join eproc1g.mni_complemento_tabelado mct
    on ejmct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
  join eproc1g.mni_complemento mc
    on mct.cod_mni_complemento = mc.cod_mni_complemento
 where tdpe.cd_sist_origem = 3
   and format_datetime(cast(tdpe.dth_evento as timestamp), 'yyyyMM') = '{anomes}';
