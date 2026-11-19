select *, cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS timestamp_ntz) AS dt_hr_carga
from (
    select
        s.*,
        row_number() over (
            partition by id_processo, cod_cnj
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_acervo_hist s
)
where rn = 1
and in_acervo = 1;

