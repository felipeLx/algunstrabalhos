with cte_evento_15417 as (
select pe.id_processo, max(pe.dth_evento) as dt_evento_15417
  from eproc1g.processo_evento pe
  join eproc1g.evento_judicial ej on pe.id_evento_judicial = ej.id_evento_judicial
  join eproc1g.mni_movimento mm on ej.cod_mni_movimento = mm.cod_mni_movimento
  left join eproc1g.evento_judicial_mni_complemento_tabelado ejmct on ej.id_evento_judicial = ejmct.id_evento_judicial
  left join eproc1g.mni_complemento_tabelado mct on ejmct.cod_mni_complemento_tabelado = mct.cod_mni_complemento_tabelado
  left join eproc1g.mni_complemento mc on mct.cod_mni_complemento = mc.cod_mni_complemento
 where (mm.cod_mni_movimento in (123, 982)
   and mc.cod_mni_complemento = 18
   and ejmct.cod_mni_complemento_tabelado = 380)
    or mm.cod_mni_movimento = 15417
 group by pe.id_processo
)
select cast(regexp_replace(p.num_processo, '[^0-9]', '') as decimal(20, 0)) as nr_processo,
       3 as cd_sist_origem,
       2 as cd_tb_origem, -- EPROC1G.PROCESSO_EVENTO
       cast(pe.id_processo_evento as varchar) as cd_seq_tb_origem,
       cast(mm.cod_mni_movimento as int) as cd_evento,
       cast(pe.dth_evento as timestamp) as dth_evento,
       0 nivel_sigilo,
       pi.ident_principal cpf_resp_evento,
       case
          when m.breadcrumb like '1:%' and pi.ident_principal is not null then 1
          when m.breadcrumb like '14:%' and pi.ident_principal is not null then 0
          else 2
       end cd_tp_resp_evento,
       case when status_evento = 'A' then true else false end as fl_evento_ativo
  from eproc1g.processo p
  join eproc1g.processo_evento pe on p.id_processo = pe.id_processo
  join eproc1g.evento_judicial ej on pe.id_evento_judicial = ej.id_evento_judicial
  join eproc1g.mni_movimento mm on ej.cod_mni_movimento = mm.cod_mni_movimento
  left join eproc1g.usuario u on pe.id_usuario = u.id_usuario
  left join eproc1g.pessoa_identificacao pi on u.id_pessoa = pi.id_pessoa
  left join ouro.tb_tpu_movimentos m on mm.cod_mni_movimento = m.cd_mov
  left join cte_evento_15417 e15417 on pe.id_processo = e15417.id_processo
 where format_datetime(cast(pe.dth_evento as timestamp), 'yyyyMM') = '{anomes}'
   and pe.sin_declinado = 'N'
   and pi.tipo_identificacao in ('CPF')
   and (e15417.dt_evento_15417 is null
        or pe.dth_evento > e15417.dt_evento_15417)
