	select 
		ejud.CODORGJULG                              as codorgjulg_ejud,
		ejud.DE_PARA                                 as de_para,
		ejud.NOME                                    as nome_ejud,
		eproc.COD_ORGAO                              as cod_orgao,
		eproc.DES_ORGAO                              as nome_eproc,
		eproc.SIG_ORGAO                              as sigla,
		coalesce(ejud.CODORGJULGCNJ, eproc.ID_ORGAO_CNJ) as codorgjulg_cnj,
		ejud.CODCOMPT                                as codcompt,
		ejud.CODLOCAL                                as codlocal,
		ejud.CODMAG_PRES                             as codmag_pres,
		ejud.IDUSUSECR                               as idususecr,
		ejud.CODMAGPRESEXERC                         as codmag_presexerc,
		ejud.LINKBALCAOVIRTUAL                       as balcao_virtual,
		ejud.LOGRADOURO                              as logradouro,
		ejud.TELEFONE                                as telefone,
		ejud.EMAIL                                   as email
	from dim_orgaojulgador_ejud ejud
	left join dim_orgaojulgador_eproc eproc on ejud.CODORGJULG = eproc.COD_ORGAO

