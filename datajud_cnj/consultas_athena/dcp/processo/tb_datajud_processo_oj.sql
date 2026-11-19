with base as
(
    select cast(regexp_replace(pp.cod_cnj, '[^0-9]', '') as decimal(20,0)) as nr_processo,
           dp.seq_dist as seq_dist,
           case when dp.redistribuicao = 'S' then 36 else 26 end as cd_evento,
           cast(
           case 
           	when cast(dp.cod_serv_vara as int) = 3 then 10315
           	when cast(dp.cod_serv_vara as int) = 4 then 11274
           	when cast(dp.cod_serv_vara as int) = 6 then 11273
           	when cast(dp.cod_serv_vara as int) = 201590 then 6673
           	when cast(dp.cod_serv_vara as int) = 201592 then 6698
           	when cast(dp.cod_serv_vara as int) = 222089 then 7993
           	when cast(dp.cod_serv_vara as int) = 222090 then 7994
           	when cast(dp.cod_serv_vara as int) = 201593 then 6699
           	when cast(dp.cod_serv_vara as int) = 201390 then 6759
           	when cast(dp.cod_serv_vara as int) = 222116 then 9928
           	else tds.cd_oj_tj
           end as int) as cd_oj_tj,
           cast(dp.data_dis as date) as data_dis,
           regexp_replace(trim(cast(dp.hora_dis as varchar)), '[^0-9]', '') as clean,
           dp.redistribuicao,
           tds.cd_atrib_cgj as cd_atrib_cgj_dest,
           tds.cd_oj_tj as cd_oj_tj_dest,
           tdsa.cd_atrib_cgj as cd_atrib_cgj_orig,
           tdsa.cd_oj_tj as cd_oj_tj_orig
      from dcp.distribuicaoprocesso dp
 left join dcp.distribuicaoprocesso dpa
        on dp.cod_proc = dpa.cod_proc
       and dp.seq_dist = dpa.seq_dist + 1
      join proccorp.processo pp
        on dp.cod_proc = pp.cod_proc
       and pp.cod_tip_proc = 1
 left join ouro.tb_dim_serventia tds
        on dp.cod_serv = tds.cd_oj_dcp
 left join ouro.tb_dim_serventia tdsa
        on dpa.cod_serv = tdsa.cd_oj_dcp
     where format_datetime(cast(dp.data_dis as timestamp), 'yyyyMM') = '${anomes}'
)
select nr_processo,
       cast(1 as int) cd_sist_origem,
       cast(5 as int) cd_tb_origem,
       cast(seq_dist as varchar) cd_seq_tb_origem,
       cast(cd_evento as int) cd_evento,
       cast(cd_oj_tj as int) cd_oj_tj,
       cast(
           at_timezone(
               with_timezone(
                   cast(
                       concat(
                           cast(data_dis as varchar),
                           ' ',
                           case
                               when length(clean) = 6
                                and cast(substr(clean, 1, 2) as int) between 0 and 23
                                and cast(substr(clean, 3, 2) as int) between 0 and 59
                                and cast(substr(clean, 5, 2) as int) between 0 and 59
                                   then concat(
                                            substr(clean, 1, 2), ':',
                                            substr(clean, 3, 2), ':',
                                            substr(clean, 5, 2)
                                        )
                               when length(clean) = 4
                                and cast(substr(clean, 1, 2) as int) between 0 and 23
                                and cast(substr(clean, 3, 2) as int) between 0 and 59
                                   then concat(
                                            substr(clean, 1, 2), ':',
                                            substr(clean, 3, 2), ':00'
                                        )
                               else '00:00:01'
                           end
                       ) as timestamp
                   ),
                   'America/Sao_Paulo'
               ),
               'UTC'
           ) as timestamp
       ) as dth_incl_oj
  from base
 where (
            coalesce(cd_atrib_cgj_dest, -1) not in (12215, 12216, 5703)
            and coalesce(cd_oj_tj_dest, -1) <> 19225
            and coalesce(cd_atrib_cgj_orig, -1) not in (12215, 12216, 5703)
            and coalesce(cd_oj_tj_orig, -1) <> 19225
            and not (
                cd_oj_tj_dest is not null
                and cd_oj_tj_orig is not null
                and cd_oj_tj_dest = cd_oj_tj_orig
            )
        );