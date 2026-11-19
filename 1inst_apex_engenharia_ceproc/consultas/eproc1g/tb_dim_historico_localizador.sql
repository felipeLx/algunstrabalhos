-- Dimensao historica (append-only) de localizadores
-- Query somente SELECT para uso no job Spark/Airflow
-- Requer --write-mode append no script de carga
with fontes as (
    select
        '1INST' as fl_origem_localizador,
        ids_localizador,
        des_localizador,
        sig_localizador
    from dw_apex.tb_localizador_processo_1inst

   -- union all

    --select
      --  '2INST' as fl_origem_localizador,
       -- ids_localizador,
        --des_localizador,
        --sig_localizador
    --from dw_apex.tb_localizador_processo_2inst
),
base as (
    select
        fl_origem_localizador,
        split(ids_localizador, ' / ') as ids_localizador_arr,
        split(des_localizador, ' / ') as des_localizador_arr,
        split(sig_localizador, ' / ') as sig_localizador_arr
    from fontes
),
explodido as (
    select distinct
        b.fl_origem_localizador,
        trim(cast(z.ids_localizador_arr as string)) as id_localizador,
        upper(trim(cast(z.des_localizador_arr as string))) as des_localizador,
        upper(trim(cast(z.sig_localizador_arr as string))) as sig_localizador
    from base b
    lateral view explode(arrays_zip(ids_localizador_arr, des_localizador_arr, sig_localizador_arr)) ez as z
    where z.ids_localizador_arr is not null
      and trim(cast(z.ids_localizador_arr as string)) <> ''
),
historico_atual as (
    select
        coalesce(trim(cast(fl_origem_localizador as string)), '1INST') as fl_origem_localizador,
        trim(cast(id_localizador as string)) as id_localizador,
        upper(trim(cast(des_localizador as string))) as des_localizador,
        upper(trim(cast(sig_localizador as string))) as sig_localizador
    from dw_apex.tb_dim_historico_localizador
),
novos as (
    select
        e.fl_origem_localizador,
        e.id_localizador,
        e.des_localizador,
        e.sig_localizador
    from explodido e
    left anti join historico_atual h
        on e.fl_origem_localizador <=> h.fl_origem_localizador
       and e.id_localizador <=> h.id_localizador
       and e.des_localizador <=> h.des_localizador
       and e.sig_localizador <=> h.sig_localizador
),
ids_historico as (
    select distinct
        fl_origem_localizador,
        id_localizador
    from historico_atual
),
des_historico as (
    select distinct
        fl_origem_localizador,
        id_localizador,
        des_localizador
    from historico_atual
    where des_localizador is not null
),
sig_historico as (
    select distinct
        fl_origem_localizador,
        id_localizador,
        sig_localizador
    from historico_atual
    where sig_localizador is not null
)
select
    n.fl_origem_localizador,
    n.id_localizador,
    n.des_localizador,
    n.sig_localizador,
    date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as dt_hr_carga,
    case
        when ih.id_localizador is null then 'NOVO_ID_LOCALIZADOR'
        when dh.id_localizador is not null and sh.id_localizador is null then 'ALTEROU_SIG_LOCALIZADOR'
        when dh.id_localizador is null and sh.id_localizador is not null then 'ALTEROU_DES_LOCALIZADOR'
        when dh.id_localizador is not null and sh.id_localizador is not null then 'ALTEROU_DES_E_SIG_LOCALIZADOR'
        else 'ALTEROU_DES_E_SIG_LOCALIZADOR'
    end as fl_tipo_mudanca
from novos n
left join ids_historico ih
    on n.fl_origem_localizador = ih.fl_origem_localizador
   and n.id_localizador = ih.id_localizador
left join des_historico dh
    on n.fl_origem_localizador = dh.fl_origem_localizador
   and n.id_localizador = dh.id_localizador
   and n.des_localizador = dh.des_localizador
left join sig_historico sh
    on n.fl_origem_localizador = sh.fl_origem_localizador
   and n.id_localizador = sh.id_localizador
   and n.sig_localizador = sh.sig_localizador
