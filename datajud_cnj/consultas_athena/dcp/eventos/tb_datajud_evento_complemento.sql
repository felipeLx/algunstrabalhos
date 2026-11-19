select distinct cast(regexp_replace(pp.cod_cnj, '[^0-9]', '') as decimal(20, 0)) nr_processo, -- NÇ§mero do processo no CNJ, excl. '.', '-' e zeros a esquerda.
       1 cd_sist_origem, -- CÇüdigo do sistema de origem (1-DCP,2-PJE,3-EPROC1G)
       case
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tm.breadcrumb like '14:%' and tpe.cd_evento not in (92, 132) then 3 -- DCP.ANDAMENTOPROCESSO_SERVIDOR
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 4 -- DCP.AUDIENCIA - DESIGNADA
         when split_part(tpe.id_processo_evento, ':', 1) = 'D' then 5 -- DCP.DISTRIBUICAOPROCESSO
         when split_part(tpe.id_processo_evento, ':', 1) = 'S' then 6 -- DCP.SITUACAOPROCESSO
         when split_part(tpe.id_processo_evento, ':', 1) = 'C' then 7 -- DCP.PROCESSOCLASSESALTERADAS
         when split_part(tpe.id_processo_evento, ':', 1) = 'I' then 8 -- PROCCORP.INTIMACAOELETRONICAORGAO
         when split_part(tpe.id_processo_evento, ':', 1) = 'M' then 9 -- DW_1INST.MVM_MOV_SCJ
         when split_part(tpe.id_processo_evento, ':', 1) = 'B' then 10 -- DW_1INST.DATAJUD_PRECATORIOSSUSPCANC
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tm.breadcrumb like '1:%' then 11 -- DCP.ANDAMENTOPROCESSO_MAGISTRADO
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tpe.cd_evento = 92 then 12 -- DCP.ANDAMENTOPROCESSO_PUBLICACAO
         when split_part(tpe.id_processo_evento, ':', 1) = 'P' and tpe.cd_evento = 132 then 13 -- DCP.ANDAMENTOPROCESSO_RECEBIMENTO
         when split_part(tpe.id_processo_evento, ':', 1) = 'H' then 14 -- HOLOSPEP.PRECA_PRECATORIO
         when split_part(tpe.id_processo_evento, ':', 1) = 'M' then 15 -- SCM.MANDADO - RECEBIMENTO
         when split_part(tpe.id_processo_evento, ':', 1) = 'M' then 16 -- SCM.MANDADO - DEVOLUCAO
         when split_part(tpe.id_processo_evento, ':', 1) = 'Q' then 17 -- ARQUIVO.DOCUMENTO - RECEBIDO
         when split_part(tpe.id_processo_evento, ':', 1) = 'Q' then 18 -- ARQUIVO.DOCTOSIT - ELIMINADO
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 19 -- DCP.AUDIENCIA - REALIZADA
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 20 -- DCP.AUDIENCIA - NÇŸO REALIZADA
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 21 -- DCP.AUDIENCIA - REDESIGNADA
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 22 -- DCP.AUDIENCIA - CONVERTIDA EM DILIGENCIA
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 23 -- DCP.AUDIENCIA - CONCILIACAO
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 24 -- DCP.AUDIENCIA - REM. P/ CEJUSC (12614)
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 25 -- DCP.AUDIENCIA - REC. NO CEJUSC (12621)
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 26 -- DCP.AUDIENCIA - RET. DO CEJUSC (12619)
         when split_part(tpe.id_processo_evento, ':', 1) = 'A' then 27 -- DCP.AUDIENCIA - MEDIACAO
         else null
       end cd_tb_origem, -- CÇüdigo da tabela de origem dos dados do movimento
       split_part(tpe.id_processo_evento, ':', 2) cd_seq_tb_origem, -- Identificador do movimento na tabela de origem
       cast(tpe.cd_evento as int) cd_evento, -- CÇüdigo do evento na TPU
       cast(tec.cod_complemento as int) cd_compl, -- CÇüdigo do complemento na TPU
       cast(
           case
               when try_cast(regexp_replace(cast(tec.valor_compl as varchar), '[^0-9]', '') as integer) < 999
                   then regexp_replace(cast(tec.valor_compl as varchar), '[^0-9]', '')
               else null
           end as varchar
       ) cd_compl_tab, -- CÇüdigo de complemento tabelado na TPU
       cast(trim(regexp_replace(cast(tec.valor_compl as varchar), '[^A-Za-z0-9 \\.,;:/()\\-+_@&%#]', '')) as varchar) ds_vlr_compl, -- Valor do complemento nÇœo tabelado ou descriÇõÇœo do movimento tabelado
       cast(tpe.dth_evento as timestamp) dth_evento, -- Data/hora em que o evento fora registrado
       true in_ativo -- Indica que o evento estÇ­ ativo
  from prata.tb_dcp_processo_evento tpe
  join proccorp.processo pp
    on tpe.id_processo = pp.cod_proc
   and pp.cod_tip_proc = 1
  join prata.tb_dcp_evento_complemento tec
    on tpe.id_processo = tec.id_processo
   and tpe.id_processo_evento = tec.id_processo_evento
   and date_trunc('second', cast(tpe.dth_evento as timestamp)) = date_trunc('second', cast(tec.dth_evento as timestamp))
  join ouro.tb_dim_cnj_complemento tc
    on tpe.cd_evento = tc.cd_mov
   and tec.cod_complemento = tc.cd_compl
  left join ouro.tb_dim_cnj_movimento tm
    on tpe.cd_evento = tm.cd_mov
 where format_datetime(cast(tpe.dth_evento as timestamp), 'yyyyMM') = '{anomes}'
   and trim(regexp_replace(cast(tec.valor_compl as varchar), '[^A-Za-z0-9 \\.,;:/()\\-+_@&%#]', '')) <> '';
