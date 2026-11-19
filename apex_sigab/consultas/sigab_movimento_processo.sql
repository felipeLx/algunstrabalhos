with cte_base as (
--EJUD
select distinct
    mov.coddoc id_processo,
    ac.cod_cnj,
    loc.codlocal cd_orgao,
    'EJUD' sistema,
    upper(coalesce(usu.cpf_usu, mov.usuincl)) cpf_movimento,
    mov.codfase cd_movimento,
    cast(mov.dthrmov as timestamp) dt_movimento,
    date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'),'yyyy-MM-dd HH:mm:ss') dt_criacao
from ejud.movimento mov
join dw_apex.sigab_acervo ac on ac.id_processo = mov.coddoc
join ejud.movimentolocal loc on loc.coddoc = mov.coddoc and loc.dthrmov = mov.dthrmov
left join seg.usuario usu on usu.cod_usu = mov.usuincl
where ac.sistema = 'EJUD'
union all
--EPROC
select distinct
    mov.id_processo,
    ac.cod_cnj,
    coalesce(org.codlocalparametrizado, 0) cd_orgao,
    'EPROC2G' sistema,
    upper(pes.ident_principal) cpf_movimento,
    mov.id_evento_judicial cd_movimento,
    cast(mov.dth_evento as timestamp) dt_movimento,
    date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'),'yyyy-MM-dd HH:mm:ss') dt_criacao
from eproc2g.processo_evento mov
join dw_apex.sigab_acervo ac on ac.id_processo = mov.id_processo
left join eproc2g.usuario usu on usu.id_usuario = mov.id_usuario
left join dw_2inst_prata.tb_associacao_localfisico org on org.codlocalsistema = mov.id_orgao_evento and org.sistema = 'EPROC2G'
left join (
    select 
        id_pessoa,
        ident_principal,
        row_number() over (
            partition by id_pessoa 
            order by case when tipo_identificacao = 'CPF' then 0 else 1 end, seq_identificacao asc
        ) rn
    from eproc2g.pessoa_identificacao
) pes on pes.id_pessoa = usu.id_pessoa and pes.rn = 1
)
select 
    b.*
from cte_base b
left join dw_apex.sigab_movimento_processo s
    on s.id_processo = b.id_processo
    and s.cod_cnj = b.cod_cnj
    and s.cd_movimento = b.cd_movimento
    and s.dt_movimento = b.dt_movimento
    and s.cpf_movimento = b.cpf_movimento
    and s.sistema = b.sistema
where s.id_processo is null;