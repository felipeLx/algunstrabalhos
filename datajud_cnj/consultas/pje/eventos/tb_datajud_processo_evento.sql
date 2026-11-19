select CAST(REGEXP_REPLACE(tp.nr_processo, '[^0-9]', '') AS DECIMAL(20, 0)) nr_processo, -- Número do processo no CNJ, excl. '.', '-' e zeros a esquerda.
       2 cd_sist_origem, -- Código do sistema de origem (1-DCP,2-PJE,3-EPROC1G)
       1 cd_tb_origem, -- CORE.TB_PROCESSO_EVENTO
       CAST(tpe.id_processo_evento AS STRING) cd_seq_tb_origem, -- Identificador do movimento na tabela de origem
       CAST(te.cd_evento AS INT) cd_evento, -- Código do evento na TPU
       CAST(tpe.dt_atualizacao AS TIMESTAMP) dth_evento, -- Data/hora em que o evento fora registrado
       0 nivel_sigilo,
       NULLIF(tul.ds_login, '0') cpf_resp_evento,
       case 
          when te.breadcrumb like '1:%' and tpe.id_usuario <> 0 then 1
          when te.breadcrumb like '14:%' and tpe.id_usuario <> 0 then 0
          else 2
       end cd_tp_resp_evento,
       tpe.in_ativo fl_evento_ativo -- Indica que o evento está ativo
  from pje.tb_processo_evento tpe 
  join pje.tb_processo tp on tpe.id_processo = tp.id_processo
  join pje.tb_evento te on tpe.id_evento = te.id_evento
  left join pje.tb_usuario_login tul ON tpe.id_usuario = tul.id_usuario
 where date_format(tpe.dt_atualizacao, 'yyyyMM') = '{anomes}';