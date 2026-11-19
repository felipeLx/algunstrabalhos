select 
    sistema,
    cd_assunto,
    ds_assunto,
    dt_criacao,
    dt_atualizacao
from (
    select 
        s.*,
        row_number() over (
            partition by sistema, cd_assunto 
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_assunto_hist s
)
where rn = 1;