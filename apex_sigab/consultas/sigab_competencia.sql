select 
    sistema,
    cd_competencia,
    ds_competencia,
    dt_criacao,
    dt_atualizacao
from (
	select 
        s.*,
        row_number() over (
            partition by sistema, cd_competencia 
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_competencia_hist s
)
where rn = 1;