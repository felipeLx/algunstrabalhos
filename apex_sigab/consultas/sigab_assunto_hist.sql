with cte_ass_ejud as (
select 
    'EJUD' sistema,
    ass.codass cd_assunto,
    upper(ass.descrsint) ds_assunto,
    from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as dt_criacao, 
    from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as dt_atualizacao
from ejud.assunto ass
), cte_ass_eproc as (
select 
    'EPROC2G' sistema,
    id_assunto_judicial cd_assunto,
    upper(des_assunto) ds_assunto,
    from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as dt_criacao, 
    from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as dt_atualizacao
from eproc2g.assunto_judicial
), cte_base as (
select sistema, cd_assunto, ds_assunto, dt_criacao, dt_atualizacao from cte_ass_ejud
union all
select sistema, cd_assunto, ds_assunto, dt_criacao, dt_atualizacao from cte_ass_eproc
)
select 
    cast(b.sistema as string) as sistema,
    cast(b.cd_assunto as int) as cd_assunto,
    cast(b.ds_assunto as string) as ds_assunto,
    cast(coalesce(s.dt_criacao, b.dt_criacao) as timestamp) as dt_criacao,
    cast(
        case 
            when s.cd_assunto is null then b.dt_criacao
            when s.ds_assunto <> b.ds_assunto then current_timestamp()
            else s.dt_atualizacao
        end as timestamp
    ) as dt_atualizacao
from cte_base b
left join dw_apex.sigab_assunto_hist s 
    on s.cd_assunto = b.cd_assunto 
   and s.sistema = b.sistema
where s.cd_assunto is null or s.ds_assunto <> b.ds_assunto;