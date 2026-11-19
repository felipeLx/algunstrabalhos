with seg_usuario as (
    select distinct cpf_usu
    from seg_usuario where cargo = 'Magistrado'
), eproc_magistrado as (
    select
        id_pessoa,
        cod_magistrado,
        num_antiguidade,
        nome_magistrado,
        dth_inclusao,
        dth_desativacao,
        tipo_identificacao,
        ident_principal,
        id_orgao,
        des_orgao,
        codcargo,
        codjustorig,
        ROW_NUMBER() OVER (PARTITION BY id_pessoa, codcargo ORDER BY dth_inclusao DESC) as rn
from (
    select
        mag.id_pessoa,
        mag.cod_magistrado,
        mag.num_antiguidade,
        mag.nome_magistrado,
        mag.dth_inclusao,
        mag.dth_desativacao,
        id.tipo_identificacao,
        id.ident_principal,
        o.id_orgao,
        o.des_orgao,
        case
            when upper(o.des_orgao) like 'DES%' then 2
	    when upper(o.des_orgao) like 'JDS%' or mag.cod_magistrado in (7777874,7777875) then 3
            else 0
        end CODCARGO,
        2 as codjustorig
    from eproc2g_magistrado mag
    join eproc2g_juizo_magistrado jm on jm.cod_magistrado = mag.cod_magistrado
    join eproc2g_orgao o on o.id_orgao = jm.id_orgao
    join (select id_pessoa, tipo_identificacao, ident_principal from eproc2g_pessoa_identificacao where tipo_identificacao = 'CPF') id
    on id.id_pessoa = mag.id_pessoa
    ) X where CODCARGO in (2,3)
)
select
    eproc.cod_magistrado,
    eproc.nome_magistrado,
    eproc.num_antiguidade,
    eproc.id_pessoa,
    eproc.ident_principal as CPF,
    eproc.dth_inclusao,
    eproc.dth_desativacao,
    eproc.codcargo,
    eproc.codjustorig,
    rn
from eproc_magistrado eproc
left join seg_usuario on eproc.ident_principal = cpf_usu
where eproc.rn = 1;
