select
    cd_orgao,
    ds_orgao,
    cpf,
    matricula,
    nome,
    login,
    email,
    in_ativo,
    dt_criacao,
    dt_atualizacao
from (
    select
        s.*,
        row_number() over (
            partition by cpf, cd_orgao
            order by dt_atualizacao desc
        ) rn
    from dw_apex.sigab_usuario_gabinete_hist s
)
where rn = 1;