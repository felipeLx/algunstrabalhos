select 
    sistema,
    cd_classe,
    ds_classe,
    dt_criacao,
    dt_atualizacao
from (
    select 
        s.*,
        row_number() over (
            partition by sistema, cd_classe 
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_classe_hist s
)
where rn = 1;