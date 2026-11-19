select distinct
       pdp.nr_processo,
       cast(29 as int) as cd_tb_origem,
       cast(tpa.id_processo_assunto as varchar) as cd_seq_tb_origem,
       cast(try_cast(tat.cd_assunto_trf as integer) as integer) as codigo_assunto,
       coalesce(tpa.in_assunto_principal, false) as principal
  from dw_1inst_prata.tb_datajud_processo pdp
  join pje.tb_processo tp
    on cast(regexp_replace(tp.nr_processo, '[^0-9]', '') as decimal(20, 0)) = pdp.nr_processo
  join pje.tb_processo_trf tpt
    on tp.id_processo = tpt.id_processo_trf
  join pje.tb_processo_assunto tpa
    on tpa.id_processo_trf = tpt.id_processo_trf
  join pje.tb_assunto_trf tat
    on tpa.id_assunto_trf = tat.id_assunto_trf
 where tpt.cd_processo_status = 'D'
   and tat.in_ativo = true
   and try_cast(tat.cd_assunto_trf as integer) is not null;
