select distinct cast(regexp_replace(cast(p.num_processo as string), '[^0-9]', '') as decimal(20,0)) as nr_processo,
       cast(null as int) as cd_comp_tj,
       cast(null as int) as cd_classe,
       cast(0 as int) as nivel_sigilo,
       cast(false as boolean) as intervencao_mp,
       cast(0 as bigint) as tamanho_processo_bytes,
       cast(null as timestamp) as data_ajuizamento,
       cast(1 as int) as proc_el,
       cast(3 as int) as dsc_sistema,
       cast(false as boolean) as juizo_100_digital,
       cast('TJRJ' as string) as sigla_tribunal,
       cast('G1' as string) as grau,
       cast(false as boolean) as pedido_prioridade,
       cast(0 as decimal(20,2)) as valor_causa,
       cast(0 as decimal(20,2)) as custas_recolhidas,
       cast(o.cod_orgao_secretaria as int) as cd_oj_tj
  from eproc1g.processo p
  left join eproc1g.orgao o
    on cast(p.id_orgao_secretaria as string) = o.cod_orgao_secretaria
 where p.num_processo is not null;
