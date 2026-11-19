select 
    cd_orgao,
    ds_orgao,
    dt_criacao,
    dt_atualizacao
from (
    select 
        s.*,
        row_number() over (
            partition by cd_orgao 
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_orgao_hist s
)
where rn = 1;