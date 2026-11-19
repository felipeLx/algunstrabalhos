with proc_cnj as (
    select
        p.id_processo,
        coalesce(cast(t.cod_cnj as string), cast(p.num_processo as string)) as cod_cnj
    from eproc1g.processo p
    left join (
        select id_processo, max(cod_cnj) as cod_cnj
        from eproc1g.md_tjrj_processo
        group by id_processo
    ) t on t.id_processo = p.id_processo
),
acervo as (
    select distinct cod_cnj
    from apex.seproc_acervo_atual
    where cd_sistema = 3
),
base as (
    select distinct
        pl.id_processo,
        lo.id_localizador,
        upper(trim(l.des_localizador)) as des_localizador,
        upper(trim(l.sig_localizador)) as sig_localizador,
        pc.cod_cnj
    from eproc1g.processo_localizador pl
    left join eproc1g.localizador_orgao lo
        on lo.id_localizador_orgao = pl.id_localizador_orgao
    left join eproc1g.localizador l
        on l.id_localizador = lo.id_localizador
    join proc_cnj pc
        on pc.id_processo = pl.id_processo
    join acervo a
        on a.cod_cnj = pc.cod_cnj
    where pl.sin_ativo = 'S'
),
agrupado as (
    select
        id_processo,
        cod_cnj,
        array_sort(
            collect_list(
                named_struct(
                    'id_localizador', id_localizador,
                    'des_localizador', des_localizador,
                    'sig_localizador', sig_localizador
                )
            )
        ) as locs
    from base
    group by id_processo, cod_cnj
)
select
    id_processo,
    cod_cnj,
    array_join(transform(locs, x -> cast(x.id_localizador as string)), ' / ') as ids_localizador,
    array_join(transform(locs, x -> x.des_localizador), ' / ') as des_localizador,
    array_join(transform(locs, x -> x.sig_localizador), ' / ') as sig_localizador
from agrupado
