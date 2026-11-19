select *, cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS timestamp_ntz) AS dt_hr_carga
from (
    select
        s.*,
        row_number() over (
            partition by id_processo
            order by dt_atribuicao desc
        ) rn
    from dw_apex.sigab_atribuicao_hist s
)
where rn = 1;