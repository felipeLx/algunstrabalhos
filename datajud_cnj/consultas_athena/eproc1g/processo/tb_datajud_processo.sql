select distinct cast(regexp_replace(cast(p.num_processo as varchar), '[^0-9]', '') as decimal(20,0)) as nr_processo,
       cast(p.cod_competencia as int) as cd_comp_tj,
       cast(rcj.cod_mni_classe_judicial as int) as cd_classe,
       cast(p.id_sigilo as int) as nivel_sigilo,
       cast(false as boolean) as intervencao_mp, --revisar
       cast(0 as bigint) as tamanho_processo_bytes, --revisar
       cast(p.dth_autuacao as timestamp) as data_ajuizamento,
       cast(1 as int) as proc_el,
       cast(4 as int) as dsc_sistema,
       cast(false as boolean) as juizo_100_digital, --revisar
       cast('TJRJ' as varchar) as sigla_tribunal,
       cast(CASE WHEN p.cod_competencia IN (13, 14, 23, 59, 60, 66) THEN 'JE' ELSE 'G1' END as varchar) as grau,
       cast(false as boolean) as pedido_prioridade, --revisar
       cast(p.valor_causa as decimal(20,2)) as valor_causa,
       cast(0 as decimal(20,2)) as custas_recolhidas, --revisar
       cast(o.cod_orgao_secretaria as int) as cd_oj_tj
  from eproc1g.processo p
  left join eproc1g.orgao o
    on cast(p.id_orgao_secretaria as varchar) = o.cod_orgao_secretaria
  left join ouro.tb_dim_serventia tds
    on p.id_orgao_secretaria = tds.cd_oj_tj
  left join eproc1g.relacao_classe_judicial_mni rcj
    on p.id_classe_judicial = rcj.id_classe_judicial 
 where p.num_processo is not null
   and p.sin_declinado = 'N';