select DISTINCT CAST(REGEXP_REPLACE(pp.cod_cnj, '[^0-9]', '') AS DECIMAL(20, 0)) nr_processo, -- Número do processo no CNJ, excl. '.', '-' e zeros a esquerda.
       1 cd_sist_origem, -- Código do sistema de origem (1-DCP,2-PJE,3-EPROC1G)
       case 
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tm.breadcrumb like '14:%' and tpe.cd_evento <> 92 then 3 -- DCP.ANDAMENTOPROCESSO_SERVIDOR
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 4 -- DCP.AUDIENCIA
         when split(tpe.id_processo_evento, ':')[0] = 'D' then 5 -- DCP.DISTRIBUICAOPROCESSO
         when split(tpe.id_processo_evento, ':')[0] = 'S' then 6 -- DCP.SITUACAOPROCESSO
         when split(tpe.id_processo_evento, ':')[0] = 'C' then 7 -- DCP.PROCESSOCLASSESALTERADAS
         when split(tpe.id_processo_evento, ':')[0] = 'I' then 8 -- PROCCORP.INTIMACAOELETRONICAORGAO
         when split(tpe.id_processo_evento, ':')[0] = 'M' then 9 -- DW_1INST.MVM_MOV_SCJ
         when split(tpe.id_processo_evento, ':')[0] = 'B' then 10 -- DW_1INST.DATAJUD_PRECATORIOSSUSPCANC
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tm.breadcrumb like '1:%' then 11 -- DCP.ANDAMENTOPROCESSO_MAGISTRADO
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tpe.cd_evento = 92 then 12 -- DCP.ANDAMENTOPROCESSO_PUBLICACAO
         else null
       end cd_tb_origem, -- Código da tabela de origem dos dados do movimento
       SPLIT(tpe.id_processo_evento, ':')[1] cd_seq_tb_origem, -- Identificador do movimento na tabela de origem
       CAST(tpe.cd_evento AS INT) cd_evento, -- Código do evento na TPU
       CAST(tpe.dth_evento AS TIMESTAMP) dth_evento, -- Data/hora em que o evento fora registrado
       0 nivel_sigilo,
       cast(null as STRING) cpf_resp_evento,
       cast(null as INT) cd_tp_resp_evento,
       true fl_evento_ativo -- Indica que o evento está ativo
  from prata.tb_dcp_processo_evento tpe 
  join proccorp.processo pp 
    on tpe.id_processo = pp.cod_proc
   and pp.cod_tip_proc = 1
  left join ouro.tb_dim_cnj_movimento tm
    on tpe.cd_evento = tm.cd_mov
 where date_format(tpe.dth_evento, 'yyyyMM') = '{anomes}';