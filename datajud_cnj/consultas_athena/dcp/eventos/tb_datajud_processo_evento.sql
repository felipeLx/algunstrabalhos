select distinct cast(regexp_replace(pp.cod_cnj, '[^0-9]', '') as decimal(20, 0)) nr_processo, -- NÇ§mero do processo no CNJ, excl. '.', '-' e zeros a esquerda.
       1 cd_sist_origem, -- CÇüdigo do sistema de origem (1-DCP,2-PJE,3-EPROC1G)
       case
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tm.breadcrumb like '14:%' and tpe.cd_evento <> 92 then 3 -- DCP.ANDAMENTOPROCESSO_SERVIDOR
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 4 -- DCP.AUDIENCIA
         when split_part(tpe.id_processo_evento, ':', 1) = 'D' then 5 -- DCP.DISTRIBUICAOPROCESSO
         when split_part(tpe.id_processo_evento, ':', 1) = 'S' then 6 -- DCP.SITUACAOPROCESSO
         when split_part(tpe.id_processo_evento, ':', 1) = 'C' then 7 -- DCP.PROCESSOCLASSESALTERADAS
         when split_part(tpe.id_processo_evento, ':', 1) = 'I' then 8 -- PROCCORP.INTIMACAOELETRONICAORGAO
         when split_part(tpe.id_processo_evento, ':', 1) = 'M' then 9 -- DW_1INST.MVM_MOV_SCJ
         when split_part(tpe.id_processo_evento, ':', 1) = 'B' then 10 -- DW_1INST.DATAJUD_PRECATORIOSSUSPCANC
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tm.breadcrumb like '1:%' then 11 -- DCP.ANDAMENTOPROCESSO_MAGISTRADO
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tpe.cd_evento = 92 then 12 -- DCP.ANDAMENTOPROCESSO_PUBLICACAO
         else null
       end cd_tb_origem, -- CÇüdigo da tabela de origem dos dados do movimento
       split_part(tpe.id_processo_evento, ':', 2) cd_seq_tb_origem, -- Identificador do movimento na tabela de origem
       cast(tpe.cd_evento as int) cd_evento, -- CÇüdigo do evento na TPU
       cast(tpe.dth_evento as timestamp) dth_evento, -- Data/hora em que o evento fora registrado
       0 nivel_sigilo,
       cast(null as varchar) cpf_resp_evento,
       cast(null as int) cd_tp_resp_evento,
       true fl_evento_ativo -- Indica que o evento estÇ­ ativo
  from prata.tb_dcp_processo_evento tpe
  join proccorp.processo pp
    on tpe.id_processo = pp.cod_proc
   and pp.cod_tip_proc = 1
  left join ouro.tb_dim_cnj_movimento tm
    on tpe.cd_evento = tm.cd_mov
 where format_datetime(cast(tpe.dth_evento as timestamp), 'yyyyMM') = '{anomes}';
