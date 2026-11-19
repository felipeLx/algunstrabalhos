with cte_distrib as
(
    select tpe.id_processo,
           tpe.id_processo_evento,
           te.cd_evento,
           tpe.dt_atualizacao
      from pje.tb_processo_evento tpe
      join pje.tb_evento te
        on tpe.id_evento = te.id_evento
     where date_format(tpe.dt_atualizacao,'yyyyMM') = '{anomes}'
       and te.cd_evento = '26'
),
cte_prim_redistr as
(
    select tptr.id_processo_trf as id_processo,
           tptr.id_orgao_julgador_anterior,
           row_number() over
           (
               partition by tptr.id_processo_trf
               order by tptr.dt_redistribuicao
           ) as rn
      from pje.tb_proc_trf_redistribuicao tptr
      join cte_distrib cd
        on tptr.id_processo_trf = cd.id_processo
       and tptr.dt_redistribuicao > cd.dt_atualizacao
),
cte_evento_redistrib as
(
    select tpe.id_processo,
           tpe.id_processo_evento,
           cast(tpe.dt_atualizacao as timestamp) as dt_atualizacao,
           row_number() over
           (
               partition by tpe.id_processo
               order by cast(tpe.dt_atualizacao as timestamp), tpe.id_processo_evento
           ) as rn_ev
      from pje.tb_processo_evento tpe
      join pje.tb_evento te
        on tpe.id_evento = te.id_evento
     where te.cd_evento = '36'
       and exists
           (
               select 1
                 from pje.tb_proc_trf_redistribuicao rd
                where rd.id_processo_trf = tpe.id_processo
                  and date_format(rd.dt_redistribuicao,'yyyyMM') = '{anomes}'
           )
),
cte_redistrib_base as
(
    select rd.id_processo_trf as id_processo,
           cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20,0)) as nr_processo,
           rd.id_orgao_julgador as cd_oj_pje,
           cast(rd.dt_redistribuicao as timestamp) as dth_incl_oj,
           row_number() over
           (
               partition by rd.id_processo_trf
               order by cast(rd.dt_redistribuicao as timestamp), rd.id_orgao_julgador
           ) as rn_rd
      from pje.tb_proc_trf_redistribuicao rd
      join pje.tb_processo tp
        on rd.id_processo_trf = tp.id_processo
     where date_format(rd.dt_redistribuicao,'yyyyMM') = '{anomes}'
),
cte_redistrib_match_10s as
(
    select rb.id_processo,
           rb.rn_rd,
           er.id_processo_evento,
           row_number() over
           (
               partition by rb.id_processo, rb.rn_rd
               order by unix_timestamp(er.dt_atualizacao) - unix_timestamp(rb.dth_incl_oj),
                        er.dt_atualizacao,
                        er.id_processo_evento desc
           ) as rn
      from cte_redistrib_base rb
 left join cte_evento_redistrib er
        on er.id_processo = rb.id_processo
       and er.dt_atualizacao >= rb.dth_incl_oj
       and er.dt_atualizacao <= rb.dth_incl_oj + interval 10 seconds
),
cte_redistrib_match_mes as
(
    select rb.id_processo,
           rb.rn_rd,
           er.id_processo_evento,
           row_number() over
           (
               partition by rb.id_processo, rb.rn_rd
               order by abs(unix_timestamp(er.dt_atualizacao) - unix_timestamp(rb.dth_incl_oj)),
                        er.dt_atualizacao,
                        er.id_processo_evento desc
           ) as rn
      from cte_redistrib_base rb
 left join cte_evento_redistrib er
        on er.id_processo = rb.id_processo
       and date_format(er.dt_atualizacao,'yyyyMM') = date_format(rb.dth_incl_oj,'yyyyMM')
),
cte_redistrib_match as
(
    select rb.nr_processo,
           rb.cd_oj_pje,
           rb.dth_incl_oj,
           coalesce(m10.id_processo_evento, mm.id_processo_evento, es.id_processo_evento) as id_processo_evento
      from cte_redistrib_base rb
 left join cte_redistrib_match_10s m10
        on rb.id_processo = m10.id_processo
       and rb.rn_rd = m10.rn_rd
       and m10.rn = 1
 left join cte_redistrib_match_mes mm
        on rb.id_processo = mm.id_processo
       and rb.rn_rd = mm.rn_rd
       and mm.rn = 1
 left join cte_evento_redistrib es
        on rb.id_processo = es.id_processo
       and rb.rn_rd = es.rn_ev
),
cte_eventos as
(
    -- Redistribuições
    select rm.nr_processo,
           cast(rm.id_processo_evento as string) as cd_seq_tb_origem,
           cast(36 as string) as cd_evento,
           rm.cd_oj_pje,
           rm.dth_incl_oj
      from cte_redistrib_match rm

    union all

    -- Distribuições
    select cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20,0)) as nr_processo,
           cast(cd.id_processo_evento as string) as cd_seq_tb_origem,
           cast(cd.cd_evento as string) as cd_evento,
           coalesce(cpr.id_orgao_julgador_anterior, tpt.id_orgao_julgador) as cd_oj_pje,
           cd.dt_atualizacao as dth_incl_oj
      from cte_distrib cd
      left join cte_prim_redistr cpr
        on cd.id_processo = cpr.id_processo
       and cpr.rn = 1
      join pje.tb_processo_trf tpt
        on cd.id_processo = tpt.id_processo_trf
      join pje.tb_processo tp
        on cd.id_processo = tp.id_processo
     where tpt.cd_processo_status = 'D'
)
select ev.nr_processo,
       cast(2 as int) cd_sist_origem,
       cast(1 as int) cd_tb_origem,
       ev.cd_seq_tb_origem,
       cast(ev.cd_evento as int) cd_evento,
       cast(ds.cd_oj_tj as int) cd_oj_tj,
       ev.dth_incl_oj
  from cte_eventos ev
  join ouro.tb_dim_serventia ds
    on ev.cd_oj_pje = ds.cd_oj_pje;
