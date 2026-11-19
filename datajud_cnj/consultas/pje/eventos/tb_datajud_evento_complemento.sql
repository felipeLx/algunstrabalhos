  SELECT pe.nr_processo,
        pe.cd_sist_origem,
        pe.cd_tb_origem,
        pe.cd_seq_tb_origem,
        pe.cd_evento,
        CAST(ttc.cd_tipo_complemento AS INT) AS cd_compl,
        CAST(tcs.ds_texto AS STRING) AS cd_compl_tab,
        CAST(tcs.ds_valor_complemento AS STRING) AS ds_vlr_compl,
        pe.dth_evento,
        pe.fl_evento_ativo
  FROM prata.tb_datajud_processo_evento pe
  JOIN pje.tb_complemento_segmentado tcs
    ON CAST(pe.cd_seq_tb_origem AS INT) = tcs.id_movimento_processo
  JOIN pje.tb_tipo_complemento ttc
    ON tcs.id_tipo_complemento = ttc.id_tipo_complemento
 WHERE pe.cd_sist_origem = 2
   AND date_format(pe.dth_evento, 'yyyyMM') = '{anomes}';