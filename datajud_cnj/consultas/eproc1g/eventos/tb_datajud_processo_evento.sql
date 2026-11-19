WITH cte_evento_15417 AS (
SELECT pe.id_processo, MAX(pe.dth_evento) AS dt_evento_15417
  FROM eproc1g.processo_evento pe
  JOIN eproc1g.evento_judicial ej ON pe.id_evento_judicial = ej.id_evento_judicial
  JOIN eproc1g.mni_movimento mm ON ej.cod_mni_movimento = mm.cod_mni_movimento
  LEFT JOIN eproc1g.evento_judicial_mni_complemento_tabelado ejmct ON ej.id_evento_judicial = ejmct.id_evento_judicial
  LEFT JOIN eproc1g.mni_complemento_tabelado mct ON ejmct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
  LEFT JOIN eproc1g.mni_complemento mc ON mct.cod_mni_complemento = mc.cod_mni_complemento
 WHERE (mm.cod_mni_movimento in (123, 982)
   AND  mc.cod_mni_complemento = 18
   AND  ejmct.cod_mni_complemento_tabelado = 380)
    OR mm.cod_mni_movimento = 15417
GROUP BY pe.id_processo
)
SELECT CAST(regexp_replace(p.num_processo, '[^0-9]', '') AS DECIMAL(20, 0)) AS nr_processo,
       3 AS cd_sist_origem,
       2 AS cd_tb_origem, -- EPROC1G.PROCESSO_EVENTO
       CAST(pe.id_processo_evento AS STRING) AS cd_seq_tb_origem,
       CAST(mm.cod_mni_movimento AS INT) AS cd_evento,
       CAST(pe.dth_evento AS TIMESTAMP) AS dth_evento,
       0 nivel_sigilo,
       pi.ident_principal cpf_resp_evento,
       case 
          when m.breadcrumb like '1:%' and pi.ident_principal is not null then 1
          when m.breadcrumb like '14:%' and pi.ident_principal is not null then 0
          else 2
       end cd_tp_resp_evento,
       CASE WHEN status_evento = 'A' THEN true ELSE false END AS fl_evento_ativo
  FROM eproc1g.processo p
  JOIN eproc1g.processo_evento pe ON p.id_processo = pe.id_processo
  JOIN eproc1g.evento_judicial ej ON pe.id_evento_judicial = ej.id_evento_judicial
  JOIN eproc1g.mni_movimento mm ON ej.cod_mni_movimento = mm.cod_mni_movimento
  LEFT JOIN eproc1g.usuario u on pe.id_usuario = u.id_usuario
  LEFT JOIN eproc1g.pessoa_identificacao pi on u.id_pessoa = pi.id_pessoa
  LEFT JOIN ouro.tb_tpu_movimentos m on mm.cod_mni_movimento = m.cd_mov
  LEFT JOIN cte_evento_15417 e15417 ON pe.id_processo = e15417.id_processo
  WHERE date_format(pe.dth_evento, 'yyyyMM') = '{anomes}'
    AND pe.sin_declinado = 'N'
    AND pi.tipo_identificacao in ('CPF')
    AND (e15417.dt_evento_15417 IS NULL
          OR pe.dth_evento > e15417.dt_evento_15417)