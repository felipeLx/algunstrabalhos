with base_processo as
(
    select pdp.nr_processo,
           pp.id_proc,
           pp.cod_proc,
           pp.cod_cnj,
           p.id_processo
      from dw_1inst_prata.tb_datajud_processo pdp
      join proccorp.processo pp
        on cast(regexp_replace(cast(pp.cod_cnj as string), '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
      join pje.tb_processo p
      on cast(regexp_replace(cast(pp.cod_cnj as string), '[^0-9]', '') as decimal(20, 0)) = cast(regexp_replace(cast(p.nr_processo as string), '[^0-9]', '') as decimal(20, 0))
     where pp.cod_tip_proc = 13
       and pp.cod_proc is not null
),
id_processo as
(
    select nr_processo,
           cast(id_processo as string) as numero
      from base_processo
     where id_proc is not null
),
id_proc as
(
    select nr_processo,
        cast(cast(id_proc as decimal(20, 0)) as string) as numero
      from base_processo
     where cod_proc is not null
),
cod_cnj as
(
    select nr_processo,
           cast(cod_cnj as string) as numero
      from base_processo
     where cod_cnj is not null
)
select distinct
       nr_processo,
       numero
  from (
        select nr_processo, numero from id_processo
        union all
        select nr_processo, numero from id_proc
        union all
        select nr_processo, numero from cod_cnj
       ) un
 where numero is not null
   and trim(numero) <> '';
