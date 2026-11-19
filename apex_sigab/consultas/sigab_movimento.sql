select
    sistema,
    cd_movimento,
    cd_movimento_sistema,
    ds_movimento,
    dt_criacao,
    dt_atualizacao
from (
    select
        s.*,
        row_number() over (
            partition by sistema, cd_movimento
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_movimento_hist s
)
where rn = 1;