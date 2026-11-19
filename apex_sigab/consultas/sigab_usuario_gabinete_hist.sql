with cte_base as (
    select distinct
        cast(loc.codlocal as bigint) as cd_orgao,
        upper(trim(org.nm_orgao)) as ds_orgao,
        trim(usu.cpf_usu) as cpf,
        trim(usu.matr_usu) as matricula,
        upper(trim(usu.nome_usu)) as nome,
        upper(trim(usu.cod_usu)) as login,
        upper(trim(usu.cod_usu || '@TJRJ.JUS.BR')) as email,
        cast(1 as int) as in_ativo,
        cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as timestamp) as dt_criacao,
        cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as timestamp) as dt_atualizacao
    from seg.usuario usu
    join ejud.usuariolocalfisico loc on usu.cod_usu = loc.idusu
    join dim_2inst_ouro.tb_dim_orgao org on org.cod_orgao = loc.codlocal
    left join ejud.documentosituacaovirtual sit_doc on sit_doc.idusubloq = usu.cod_usu
    where loc.indlocalprinc = 'S' and loc.codlocal in (9183, 9093, 9141, 9194, 9187, 8480) 
    and (upper(trim(usu.cargo)) = 'MAGISTRADO' or sit_doc.idusubloq is not null)
),
cte_antigos as (
    select 
        cast(cd_orgao as bigint) as cd_orgao,
        cast(ds_orgao as string) as ds_orgao,
        cast(cpf as string) as cpf,
        cast(matricula as string) as matricula,
        cast(nome as string) as nome,
        cast(login as string) as login,
        cast(email as string) as email,
        cast(in_ativo as int) as in_ativo,
        cast(dt_criacao as timestamp) as dt_criacao,
        cast(dt_atualizacao as timestamp) as dt_atualizacao
    from dw_apex.sigab_usuario_gabinete_hist
),
cte_merge as (
    select
        b.cd_orgao,
        b.ds_orgao,
        b.cpf,
        b.matricula,
        b.nome,
        b.login,
        b.email,
        cast(1 as int) as in_ativo,
        coalesce(s.dt_criacao, b.dt_criacao) as dt_criacao,
        cast(case
            when s.cpf is null then b.dt_criacao
            when not (s.matricula <=> b.matricula)
              or not (s.nome <=> b.nome)
              or not (s.login <=> b.login)
              or not (s.email <=> b.email)
                then from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')
            else s.dt_atualizacao
        end as timestamp) dt_atualizacao
    from cte_base b
    left join cte_antigos s on s.cpf = b.cpf and s.cd_orgao = b.cd_orgao
    where s.cpf is null 
    or not (s.matricula <=> b.matricula)
    or not (s.nome <=> b.nome)
    or not (s.login <=> b.login)
    or not (s.email <=> b.email)
    union all
    select
        s.cd_orgao,
        s.ds_orgao,
        s.cpf,
        s.matricula,
        s.nome,
        s.login,
        s.email,
        cast(0 as int) as in_ativo,
        s.dt_criacao,
        cast(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as timestamp) as dt_atualizacao
    from cte_antigos s
    left join cte_base b on b.cpf = s.cpf and b.cd_orgao = s.cd_orgao
    where b.cpf is null and s.in_ativo = 1
)
select 
    cast(cd_orgao as bigint) as cd_orgao,
    cast(ds_orgao as string) as ds_orgao,
    cast(cpf as string) as cpf,
    cast(matricula as string) as matricula,
    cast(nome as string) as nome,
    cast(login as string) as login,
    cast(email as string) as email,
    cast(in_ativo as int) as in_ativo,
    cast(dt_criacao as timestamp) as dt_criacao,
    cast(dt_atualizacao as timestamp) as dt_atualizacao
from cte_merge;