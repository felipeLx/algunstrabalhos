select DISTINCT CAST(REGEXP_REPLACE(pp.cod_cnj, '[^0-9]', '') AS DECIMAL(20, 0)) nr_processo, -- Número do processo no CNJ, excl. '.', '-' e zeros a esquerda.
       1 cd_sist_origem, -- Código do sistema de origem (1-DCP,2-PJE,3-EPROC1G)
       case 
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tm.breadcrumb like '14:%' and tpe.cd_evento not in (92, 132) then 3 -- DCP.ANDAMENTOPROCESSO_SERVIDOR
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 4 -- DCP.AUDIENCIA - DESIGNADA
         when split(tpe.id_processo_evento, ':')[0] = 'D' then 5 -- DCP.DISTRIBUICAOPROCESSO
         when split(tpe.id_processo_evento, ':')[0] = 'S' then 6 -- DCP.SITUACAOPROCESSO
         when split(tpe.id_processo_evento, ':')[0] = 'C' then 7 -- DCP.PROCESSOCLASSESALTERADAS
         when split(tpe.id_processo_evento, ':')[0] = 'I' then 8 -- PROCCORP.INTIMACAOELETRONICAORGAO
         when split(tpe.id_processo_evento, ':')[0] = 'M' then 9 -- DW_1INST.MVM_MOV_SCJ
         when split(tpe.id_processo_evento, ':')[0] = 'B' then 10 -- DW_1INST.DATAJUD_PRECATORIOSSUSPCANC
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tm.breadcrumb like '1:%' then 11 -- DCP.ANDAMENTOPROCESSO_MAGISTRADO
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tpe.cd_evento = 92 then 12 -- DCP.ANDAMENTOPROCESSO_PUBLICACAO
         when split(tpe.id_processo_evento, ':')[0] = 'P' and tpe.cd_evento = 132 then 13 -- DCP.ANDAMENTOPROCESSO_RECEBIMENTO
         when split(tpe.id_processo_evento, ':')[0] = 'H' then 14 -- HOLOSPEP.PRECA_PRECATORIO
         when split(tpe.id_processo_evento, ':')[0] = 'M' then 15 -- SCM.MANDADO - RECEBIMENTO
         when split(tpe.id_processo_evento, ':')[0] = 'M' then 16 -- SCM.MANDADO - DEVOLUCAO
         when split(tpe.id_processo_evento, ':')[0] = 'Q' then 17 -- ARQUIVO.DOCUMENTO - RECEBIDO
         when split(tpe.id_processo_evento, ':')[0] = 'Q' then 18 -- ARQUIVO.DOCTOSIT - ELIMINADO
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 19 -- DCP.AUDIENCIA - REALIZADA
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 20 -- DCP.AUDIENCIA - NÃO REALIZADA
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 21 -- DCP.AUDIENCIA - REDESIGNADA
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 22 -- DCP.AUDIENCIA - CONVERTIDA EM DILIGENCIA
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 23 -- DCP.AUDIENCIA - CONCILIACAO
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 24 -- DCP.AUDIENCIA - REM. P/ CEJUSC (12614)
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 25 -- DCP.AUDIENCIA - REC. NO CEJUSC (12621)
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 26 -- DCP.AUDIENCIA - RET. DO CEJUSC (12619)
         when split(tpe.id_processo_evento, ':')[0] = 'A' then 27 -- DCP.AUDIENCIA - MEDIACAO
         else null
       end cd_tb_origem, -- Código da tabela de origem dos dados do movimento
       SPLIT(tpe.id_processo_evento, ':')[1] cd_seq_tb_origem, -- Identificador do movimento na tabela de origem
       CAST(tpe.cd_evento AS INT) cd_evento, -- Código do evento na TPU
       CAST(tec.cod_complemento AS INT) cd_compl, -- Código do complemento na TPU
       CAST(case 
                when REGEXP_REPLACE(tec.valor_compl, '[^0-9]', '') < 999 then REGEXP_REPLACE(tec.valor_compl, '[^0-9]', '')
                else null 
            end AS STRING) cd_compl_tab, -- Código de complemento tabelado na TPU
       CAST(trim(regexp_replace(CAST(tec.valor_compl AS STRING),'[^A-Za-z0-9 \\.,;:/()\\-+_@&%#]','')) AS STRING) ds_vlr_compl, -- Valor do complemento não tabelado ou descrição do movimento tabelado
       CAST(tpe.dth_evento AS TIMESTAMP) dth_evento, -- Data/hora em que o evento fora registrado
       true in_ativo -- Indica que o evento está ativo
 from prata.tb_dcp_processo_evento tpe 
 join proccorp.processo pp 
   on tpe.id_processo = pp.cod_proc
  and pp.cod_tip_proc = 1
 join prata.tb_dcp_evento_complemento tec 
   on tpe.id_processo = tec.id_processo 
  and tpe.id_processo_evento = tec.id_processo_evento
  and date_trunc('SECOND', tpe.dth_evento) = date_trunc('SECOND', tec.dth_evento)
 join ouro.tb_dim_cnj_complemento tc 
   on tpe.cd_evento = tc.cd_mov
  and tec.cod_complemento = tc.cd_compl
 left join ouro.tb_dim_cnj_movimento tm
    on tpe.cd_evento = tm.cd_mov
where date_format(tpe.dth_evento, 'yyyyMM') = '{anomes}'
  and trim(regexp_replace(CAST(tec.valor_compl AS STRING),'[^A-Za-z0-9 \\.,;:/()\\-+_@&%#]','')) <> '';