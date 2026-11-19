-- Regra validada com negocio:
-- `tb_processo_trf.id_proc_referencia` representa o "processo de referencia".
-- Quando preenchido, o processo atual e tratado como incidental (`PI`) e o
-- processo referenciado como principal (`PP`). Nao usamos
-- `tb_processo_trf_conexao`, pois ela mistura prevencao/dependencia e pode
-- trazer multiplos vinculos ativos para o mesmo incidente.
-- Para evitar relacao quebrada no envio, mantemos apenas pares em que as duas
-- pontas possuem numero CNJ valido e existem em `tb_datajud_processo`.
-- Apesar do nome local `nr_processo_incidental`, este campo representa o
-- `numeroProcesso` do XSD: o outro processo da relacao. Por isso, na linha
-- `PI` ele recebe o principal; na linha `PP` ele recebe o incidental.
with processos_datajud as (
    select distinct
           nr_processo
      from dw_1inst_prata.tb_datajud_processo
),
processos_raw as (
    select
           pt.id_processo_trf,
           regexp_replace(coalesce(cast(p.nr_processo as varchar), ''), '[^0-9]', '') as nr_processo_limpo,
           cast(cj.cd_classe_judicial as integer) as classe_processual,
           pt.id_proc_referencia
      from pje.tb_processo_trf pt
      join pje.tb_processo p
        on p.id_processo = pt.id_processo_trf
      join pje.tb_classe_judicial cj
        on cj.id_classe_judicial = pt.id_classe_judicial
),
processos as (
    select
           id_processo_trf,
           cast(nr_processo_limpo as decimal(20, 0)) as nr_processo,
           classe_processual,
           id_proc_referencia
      from processos_raw
     where length(nr_processo_limpo) = 20
),
relacoes as (
    select distinct
           inc.nr_processo as nr_processo_incidente,
           ref.nr_processo as nr_processo_principal,
           inc.classe_processual as classe_incidente
      from processos inc
      join processos ref
        on ref.id_processo_trf = inc.id_proc_referencia
      join processos_datajud dpi
        on dpi.nr_processo = inc.nr_processo
      join processos_datajud dpp
        on dpp.nr_processo = ref.nr_processo
     where inc.id_proc_referencia is not null
       and inc.nr_processo <> ref.nr_processo
)
select
       nr_processo_incidente as nr_processo,
       nr_processo_principal as nr_processo_incidental,
       cast('PI' as varchar) as tipo_relacao,
       classe_incidente as classe_processual
  from relacoes
union all
select
       nr_processo_principal as nr_processo,
       nr_processo_incidente as nr_processo_incidental,
       cast('PP' as varchar) as tipo_relacao,
       classe_incidente as classe_processual
  from relacoes;
