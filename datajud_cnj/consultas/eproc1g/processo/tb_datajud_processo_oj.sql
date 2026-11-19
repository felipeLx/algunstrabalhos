with cte_evento_15417 as
(
    select pe.id_processo,
           max(pe.dth_evento) as dt_evento_15417
      from eproc1g.processo_evento pe
      join eproc1g.evento_judicial ej
        on pe.id_evento_judicial = ej.id_evento_judicial
      join eproc1g.mni_movimento mm
        on ej.cod_mni_movimento = mm.cod_mni_movimento
 left join eproc1g.evento_judicial_mni_complemento_tabelado ejmct
        on ej.id_evento_judicial = ejmct.id_evento_judicial
 left join eproc1g.mni_complemento_tabelado mct
        on ejmct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
 left join eproc1g.mni_complemento mc
        on mct.cod_mni_complemento = mc.cod_mni_complemento
     where (mm.cod_mni_movimento in (123, 982)
       and  mc.cod_mni_complemento = 18
       and  ejmct.cod_mni_complemento_tabelado = 380)
        or mm.cod_mni_movimento = 15417
     group by pe.id_processo
),
redistribuicoes_base as
(
    select pe.id_processo,
           cast(regexp_replace(cast(p.num_processo as string), '[^0-9]', '') as decimal(20,0)) as nr_processo,
           pe.id_processo_evento,
           to_utc_timestamp(cast(pe.dth_evento as timestamp), 'America/Sao_Paulo') as dth_incl_oj,
           o1.cod_orgao_secretaria as cd_oj_origem,
           o2.cod_orgao_secretaria as cd_oj_destino
      from eproc1g.processo_evento pe
      join eproc1g.processo p
        on pe.id_processo = p.id_processo
      join eproc1g.evento_judicial ej
        on pe.id_evento_judicial = ej.id_evento_judicial
 left join cte_evento_15417 e15417
        on pe.id_processo = e15417.id_processo
 left join eproc1g.orgao o1
        on o1.sig_orgao =
           regexp_replace(
               trim(
                   case
                       when substr(
                                trim(replace(replace(element_at(split(pe.complemento, ' para'), 1), '(', ''), ')', '')),
                                1,
                                3
                            ) = 'de '
                           then substr(
                                    trim(replace(replace(element_at(split(pe.complemento, ' para'), 1), '(', ''), ')', '')),
                                    4
                                )
                       else trim(
                                replace(
                                    replace(element_at(split(pe.complemento, ' para'), 1), '(', ''),
                                    ')',
                                    ''
                                )
                            )
                   end
               ),
               '[0-9]+J$',
               '1J'
           )
 left join eproc1g.orgao o2
        on o2.sig_orgao =
           regexp_replace(
               trim(
                   case
                       when instr(reverse(element_at(split(pe.complemento, ' para'), 2)), ')') > 0
                           then substr(
                                    element_at(split(pe.complemento, ' para'), 2),
                                    1,
                                    length(element_at(split(pe.complemento, ' para'), 2)) - instr(reverse(element_at(split(pe.complemento, ' para'), 2)), ')')
                                )
                       else element_at(split(pe.complemento, ' para'), 2)
                   end
               ),
               '[0-9]+J$',
               '1J'
           )
     where ej.cod_mni_movimento = 36
       and size(split(pe.complemento, ' para')) >= 2
       and (e15417.dt_evento_15417 is null or pe.dth_evento > e15417.dt_evento_15417)
),
primeira_redistribuicao as
(
    select id_processo,
           nr_processo,
           dth_incl_oj,
           cd_oj_origem,
           row_number() over (
               partition by id_processo
               order by dth_incl_oj
           ) as rn
      from redistribuicoes_base
),
distribuicoes as
(
    select cast(regexp_replace(cast(p.num_processo as string), '[^0-9]', '') as decimal(20,0)) as nr_processo,
           pe.id_processo_evento,
           26 as cd_evento,
           coalesce(pr.cd_oj_origem, o.cod_orgao_secretaria) as cd_oj_tj,
           to_utc_timestamp(cast(pe.dth_evento as timestamp), 'America/Sao_Paulo') as dth_incl_oj
      from eproc1g.processo_evento pe
      join eproc1g.processo p
        on pe.id_processo = p.id_processo
      join eproc1g.evento_judicial ej
        on pe.id_evento_judicial = ej.id_evento_judicial
 left join cte_evento_15417 e15417
        on pe.id_processo = e15417.id_processo
 left join primeira_redistribuicao pr
        on pe.id_processo = pr.id_processo
       and pr.rn = 1
 left join eproc1g.orgao o
        on cast(p.id_orgao_secretaria as string) = o.cod_orgao_secretaria
     where ej.cod_mni_movimento = 26
       and date_format(to_utc_timestamp(cast(pe.dth_evento as timestamp), 'America/Sao_Paulo'), 'yyyyMM') = '{anomes}'
       and (e15417.dt_evento_15417 is null or pe.dth_evento > e15417.dt_evento_15417)
),
redistribuicoes as
(
    select nr_processo,
           id_processo_evento,
           36 as cd_evento,
           cd_oj_destino as cd_oj_tj,
           dth_incl_oj
      from redistribuicoes_base
     where date_format(dth_incl_oj, 'yyyyMM') = '{anomes}'
)
select distinct nr_processo,
       cast(3 as int) cd_sist_origem,
       cast(2 as int) cd_tb_origem,
       cast(id_processo_evento as string) cd_seq_tb_origem,
       cast(cd_evento as int) cd_evento,
       cast(cd_oj_tj as int) cd_oj_tj,
       dth_incl_oj
  from distribuicoes
union all
select distinct nr_processo,
       cast(3 as int) cd_sist_origem,
       cast(2 as int) cd_tb_origem,
       cast(id_processo_evento as string) cd_seq_tb_origem,
       cast(cd_evento as int) cd_evento,
       cast(cd_oj_tj as int) cd_oj_tj,
       dth_incl_oj
  from redistribuicoes;
