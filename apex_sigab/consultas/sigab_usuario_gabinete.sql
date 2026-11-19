select
    cast(cd_orgao as int) as cd_orgao,
    cast(ds_orgao as string) as ds_orgao,
    cast(cpf as string) as cpf,
    cast(matricula as string) as matricula,
    cast(nome as string) as nome,
    cast(login as string) as login,
    cast(email as string) as email,
    cast(in_ativo as int) as in_ativo,
    cast(dt_criacao as timestamp) as dt_criacao,
    cast(dt_atualizacao as timestamp) as dt_atualizacao,
    cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as timestamp) as dt_hr_carga
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