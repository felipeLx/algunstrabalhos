SELECT tdpe.nr_processo,
       tdpe.cd_sist_origem,
       tdpe.cd_tb_origem,
       tdpe.cd_seq_tb_origem,
       tdpe.cd_evento,
       CAST(mc.cod_mni_complemento AS INT) AS cd_compl,
       CAST(ejmct.cod_mni_complemento_tabelado AS STRING) AS cd_compl_tab,
       CAST(mct.des_mni_complemento_tabelado AS STRING) AS ds_vlr_compl,
       tdpe.dth_evento,
       tdpe.fl_evento_ativo
  FROM prata.tb_datajud_processo_evento tdpe
  JOIN eproc1g.processo_evento pe
    ON CAST(tdpe.cd_seq_tb_origem AS DECIMAL(30,0)) = CAST(pe.id_processo_evento AS DECIMAL(30,0))
  JOIN eproc1g.evento_judicial_mni_complemento_tabelado ejmct
    ON pe.id_evento_judicial = ejmct.id_evento_judicial
  JOIN eproc1g.mni_complemento_tabelado mct
    ON ejmct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
  JOIN eproc1g.mni_complemento mc
    ON mct.cod_mni_complemento = mc.cod_mni_complemento
 WHERE tdpe.cd_sist_origem = 3
   AND date_format(tdpe.dth_evento, 'yyyyMM') = '{anomes}';