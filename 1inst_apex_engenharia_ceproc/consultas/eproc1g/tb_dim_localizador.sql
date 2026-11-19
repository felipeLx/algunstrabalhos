-- Dimensao comum (snapshot atual) de localizadores
-- Query somente SELECT para uso no job Spark/Airflow
with fontes as (
    select sig_localizador
    from dw_apex.tb_localizador_processo_1inst

    union all

    select sig_localizador
    from dw_apex.tb_localizador_processo_2inst
),
base as (
    select
        split(sig_localizador, ' / ') as sig_localizador_arr
    from fontes
),
explodido as (
    select
        upper(trim(pe.sig_localizador)) as sig_localizador
    from base
    lateral view explode(sig_localizador_arr) pe as sig_localizador
)
select
    sig_localizador
from explodido
where sig_localizador is not null
  and sig_localizador <> ''
group by sig_localizador
