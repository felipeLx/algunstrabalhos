with ultima_distribuicao as
(
    select dp.cod_proc,
           dp.cod_serv,
           row_number() over (
               partition by dp.cod_proc
               order by dp.seq_dist desc
           ) as rn
      from dcp.distribuicaoprocesso dp
)
select distinct cast(regexp_replace(pp.cod_cnj, '[^0-9]', '') as decimal(20,0)) as nr_processo,
       cast(null as int) as cd_comp_tj,
       cast(null as int) as cd_classe,
       cast(0 as int) as nivel_sigilo,
       cast(false as boolean) as intervencao_mp,
       cast(0 as bigint) as tamanho_processo_bytes,
       cast(null as timestamp) as data_ajuizamento,
       cast(1 as int) as proc_el,
       cast(2 as int) as dsc_sistema,
       cast(false as boolean) as juizo_100_digital,
       cast('TJRJ' as string) as sigla_tribunal,
       cast('G1' as string) as grau,
       cast(false as boolean) as pedido_prioridade,
       cast(0 as decimal(20,2)) as valor_causa,
       cast(0 as decimal(20,2)) as custas_recolhidas,
       cast(ds.cd_oj_tj as int) as cd_oj_tj
  from proccorp.processo pp
  left join ultima_distribuicao dp
    on pp.cod_proc = dp.cod_proc
   and dp.rn = 1
  left join ouro.tb_dim_serventia ds
    on dp.cod_serv = ds.cd_oj_dcp
 where pp.cod_tip_proc = 1
   and pp.cod_cnj is not null;
