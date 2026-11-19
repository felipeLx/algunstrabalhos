with base_vinculo as
(
    select distinct
           pdp.nr_processo,
           cast(32 as int) as cd_tb_origem, -- pje.tb_processo_trf_conexao
           cast(tpc.id_processo_trf_conexao as varchar) as cd_seq_tb_origem,
           regexp_replace(tp_conexo.nr_processo, '[^0-9]', '') as nr_processo_vinculado_raw,
           upper(trim(tpc.tp_tipo_conexao)) as vinculo,
           coalesce(
               tpc.dt_possivel_prevencao,
               tpt_conexo.dt_distribuicao,
               tpt_origem.dt_distribuicao,
               tpc.dt_registro,
               tpc.dt_ultima_pesquisa,
               tpc.dt_valida_prevencao,
               timestamp '1900-01-01 00:00:00'
           ) as dt_ref
      from dw_1inst_prata.tb_datajud_processo pdp
      join pje.tb_processo tp_origem
        on cast(regexp_replace(tp_origem.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
      join pje.tb_processo_trf_conexao tpc
        on tpc.id_processo_trf = tp_origem.id_processo
      join pje.tb_processo tp_conexo
        on tp_conexo.id_processo = tpc.id_processo_trf_conexo
      left join pje.tb_processo_trf tpt_origem
        on tpt_origem.id_processo_trf = tpc.id_processo_trf
      left join pje.tb_processo_trf tpt_conexo
        on tpt_conexo.id_processo_trf = tpc.id_processo_trf_conexo
     where tpc.in_ativo = true
)
select nr_processo,
       cd_tb_origem,
       cd_seq_tb_origem,
       try_cast(nr_processo_vinculado_raw as decimal(20, 0)) as nr_processo_vinculado,
       vinculo
  from base_vinculo
 where format_datetime(cast(dt_ref as timestamp), 'yyyyMM') = '${anomes}'
   and regexp_like(nr_processo_vinculado_raw, '^[0-9]{20}$')
   and try_cast(nr_processo_vinculado_raw as decimal(20, 0)) is not null
   and try_cast(nr_processo_vinculado_raw as decimal(20, 0)) <> nr_processo
   and vinculo is not null;
