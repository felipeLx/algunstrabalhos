with cte_prioridade as
(
    select distinct
           pdp.nr_processo,
           cast(30 as int) as cd_tb_origem,
           cast(ppp.id_proc_prioridade_processo as string) as cd_seq_tb_origem,
           upper(trim(pp.ds_prioridade)) as ds_prioridade_norm
      from dw_1inst_prata.tb_datajud_processo pdp
      join pje.tb_processo tp
        on cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
      join pje.tb_processo_trf tpt
        on tp.id_processo = tpt.id_processo_trf
      join pje.tb_proc_prioridde_processo ppp
        on ppp.id_processo_trf = tpt.id_processo_trf
      join pje.tb_prioridade_processo pp
        on ppp.id_prioridade_processo = pp.id_prioridade_processo
     where tpt.cd_processo_status = 'D'
       and pp.in_ativo = true
)
select nr_processo,
       cd_tb_origem,
       cd_seq_tb_origem,
       case
           when ds_prioridade_norm like 'IDOSO%' then 'ID'
           when ds_prioridade_norm in ('RÉU PRESO', 'REU PRESO') then 'RP'
           when ds_prioridade_norm in ('PESSOA EM SITUAÇÃO DE RUA', 'PESSOA EM SITUACAO DE RUA') then 'PSR'
           when ds_prioridade_norm in ('PESSOA COM DEFICIÊNCIA', 'PESSOA COM DEFICIENCIA') then 'PCD'
           when ds_prioridade_norm in ('DOENÇA TERMINAL', 'DOENCA TERMINAL') then 'DG'
           when ds_prioridade_norm in ('RÉU HOSPITALIZADO', 'REU HOSPITALIZADO') then 'OUT'
           when ds_prioridade_norm in ('SAÚDE', 'SAUDE') then 'OUT'
           when ds_prioridade_norm in ('NÚCLEO 4.0', 'NUCLEO 4.0') then 'OUT'
           when ds_prioridade_norm in ('JUÍZO 100% DIGITAL', 'JUIZO 100% DIGITAL') then 'OUT'
           else 'OUT'
       end as tipo_prioridade
  from cte_prioridade;
