with processos_migrados as
(
    select distinct cast(regexp_replace(cast(pp_pje.cod_cnj as string), '[^0-9]', '') as decimal(20, 0)) as nr_processo
      from proccorp.processo pp_pje
      join proccorp.processo pp_eproc1g
        on pp_eproc1g.cod_cnj = pp_pje.cod_cnj
     where pp_pje.cod_tip_proc = 13
       and pp_eproc1g.cod_tip_proc = 16
)
SELECT CAST(regexp_replace(tp.nr_processo, '[^0-9]', '') AS DECIMAL(20,0)) AS nr_processo,
       tpt.id_competencia AS cd_comp_tj,
       cj.cd_classe_judicial AS cd_classe,
       tpt.cd_nivel_acesso AS nivel_sigilo,
       CASE WHEN tpp.id_processo_trf IS NULL THEN false ELSE true END AS intervencao_mp,
       0 AS tamanho_processo_bytes,
       tpt.dt_distribuicao AS data_ajuizamento,
       1 AS proc_el,
       1 AS dsc_sistema,
       (case when tpt.in_solicitado_juizo_100_digital is null then false else true end) AS juizo_100_digital,
       'TJRJ' AS sigla_tribunal,
       CASE WHEN tpt.id_competencia IN (13, 14, 23, 59, 60, 66) THEN 'JE' ELSE 'G1' END AS grau,
       CASE WHEN tppp.id_processo_trf IS NULL THEN true ELSE false END AS pedido_prioridade,
       CAST(coalesce(tpt.vl_causa, 0) AS DECIMAL(20,2)) AS valor_causa,
       CAST(coalesce(tpt.vl_custas_recolhidas, 0) AS DECIMAL(20,2)) AS custas_recolhidas,
       ds.cd_oj_tj AS cd_oj_tj
  FROM pje.tb_processo tp
  JOIN pje.tb_processo_trf tpt
    ON tp.id_processo = tpt.id_processo_trf
  JOIN pje.tb_classe_judicial cj
    ON tpt.id_classe_judicial = cj.id_classe_judicial
  LEFT JOIN ouro.tb_dim_serventia ds
    ON tpt.id_orgao_julgador = ds.cd_oj_pje
  LEFT JOIN pje.tb_proc_prioridde_processo tppp
    ON tp.id_processo = tppp.id_processo_trf
  LEFT JOIN pje.tb_processo_parte tpp
    ON tp.id_processo = tpp.id_processo_trf
   AND tpp.id_pessoa = 53
  LEFT JOIN processos_migrados mg
    ON mg.nr_processo = CAST(regexp_replace(tp.nr_processo, '[^0-9]', '') AS DECIMAL(20,0))
 WHERE tpt.cd_processo_status = 'D'
   AND mg.nr_processo IS NULL
