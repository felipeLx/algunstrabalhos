with secretaria_dir_publica as (
	select 
        lfparam.codlocalsistema, lfparam.codlocalparametrizado, lfparam.des_orgao, lfparam.localfisico as localfisico from (
		select CAST(o.id_orgao AS INT) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%CDIRPUB' AND substring(o.sig_orgao from 1 for 1) BETWEEN '0' AND '9'
		union all 
		select cast(o.cod_orgao_secretaria as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%CDIRPUB' AND substring(o.sig_orgao from 1 for 1) BETWEEN '0' AND '9' AND O.COD_ORGAO_SECRETARIA is not NULL) lfparam
), secretaria_dir_privado as (
		select lfparam.codlocalsistema, lfparam.codlocalparametrizado, lfparam.des_orgao, lfparam.localfisico as localfisico from (
		select cast(o.id_orgao as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%CDIRPRIV' AND substring(o.sig_orgao from 1 for 1) BETWEEN '0' AND '9'
		union all 
		select cast(o.cod_orgao_secretaria as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%CDIRPRIV' AND substring(o.sig_orgao from 1 for 1) BETWEEN '0' AND '9' AND O.COD_ORGAO_SECRETARIA is not NULL) lfparam 
), nucleo_digital as (
	select lfparam.codlocalsistema, lfparam.codlocalparametrizado, lfparam.des_orgao, lf.descr as localfisico from (
	select cast(id_orgao as INT) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where id_orgao = 18140
	union all 
	select cast(cod_orgao_secretaria as int) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where id_orgao = 18140) lfparam
	left join ejud.localfisico lf on lfparam.CODLOCALPARAMETRIZADO = cast(lf.codlocal as int)
), gabinetes_nucleo as (
	select
		id_orgao as codlocalsistema,
		case 
			when id_orgao = 18300 or id_orgao = 1087534 then 14140
			when id_orgao = 18301 or id_orgao = 1087535 then 10233
			when id_orgao = 18302 or id_orgao = 1087536 then 16842
		end as codlocalparametrizado,
		DES_ORGAO,
		SIG_ORGAO
	from
		eproc2g.orgao where id_orgao in (18300, 18301, 18302, 1087534, 1087535, 1087536)
), vice_presidencia as (
	SELECT 
		id_orgao AS codlocalsistema, 
		case 
			when id_orgao = 519 then 3142
			when id_orgao = 520 then 3144
			when id_orgao = 521 then 3148
			when id_orgao = 3581 then 2592
			when id_orgao = 3582 then 2593
			when id_orgao = 3583 then 2594		
			else id_orgao
		end as codlocalparametrizado,
	CASE 
	    WHEN SIG_ORGAO LIKE '%1VP%' THEN '1VP'
	    WHEN SIG_ORGAO LIKE '%2VP%' THEN '2VP'
	    WHEN SIG_ORGAO LIKE '%3VP%' THEN '3VP'
	    ELSE 'OUTRO'
	END AS localassociado
	FROM eproc2g.orgao
	WHERE SIG_ORGAO LIKE '%1VP%' or SIG_ORGAO LIKE '%2VP%' or SIG_ORGAO like '%3VP%'
), orgao_especial as (
		select lfparam.codlocalsistema, lfparam.codlocalparametrizado, lfparam.des_orgao, lfparam.localfisico as localfisico from (
		select cast(o.id_orgao as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where ID_ORGAO = 526
		union all 
		select cast(o.cod_orgao_secretaria as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where ID_ORGAO = 526 AND O.COD_ORGAO_SECRETARIA is not NULL) lfparam 
), presidencia as (
    SELECT id_orgao AS codlocalsistema, id_orgao codlocalparametrizado, sig_orgao
    FROM eproc2g.orgao
    WHERE SIG_ORGAO LIKE '%PRES%'
), outros as (
	select
		id_orgao as codlocalsistema,
		case 
			when id_orgao = 10852 then 8972
			when id_orgao = 12103 then 12273
			when id_orgao = 1087542 then 3537
			else id_orgao
		end as codlocalparametrizado,
		DES_ORGAO,
		SIG_ORGAO
	from
		eproc2g.orgao where id_orgao in (10852, 12103, 1087542, 9999)
), gabinete_jds_magistrado as (
	select distinct
		lfparam.CODLOCALSISTEMA
		,lfparam.codlocalparametrizado
		,lf.descr 
	from
		eproc2g.juizo_magistrado jm
	join (select cast(id_orgao as INT) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where UPPER(sig_orgao) like 'JDS%' and id_orgao not in (18302, 1087536)
	union all 
	select cast(cod_orgao_secretaria as INT) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where UPPER(sig_orgao) like 'JDS%' AND COD_ORGAO_SECRETARIA is not NULL and id_orgao not in (18302, 1087536)) lfparam 
		on jm.id_orgao = lfparam.CODLOCALPARAMETRIZADO
	left join ejud.localfisico lf on lfparam.CODLOCALPARAMETRIZADO = cast(lf.codlocal as int)
), gabinete_magistrado as (
	select distinct
		lfparam.CODLOCALSISTEMA
		,lfparam.codlocalparametrizado
		,lf.descr 
	from
		eproc2g.juizo_magistrado jm
	join (select cast(id_orgao as INT) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where UPPER(sig_orgao) like 'DES%'
	union all 
	select cast(cod_orgao_secretaria as INT) as CODLOCALSISTEMA, cast(id_orgao as INT) as CODLOCALPARAMETRIZADO, des_orgao, sig_orgao 
	from eproc2g.orgao where UPPER(sig_orgao) like 'DES%' AND COD_ORGAO_SECRETARIA is not NULL) lfparam 
		on jm.id_orgao = lfparam.CODLOCALPARAMETRIZADO
	left join ejud.localfisico lf on lfparam.CODLOCALPARAMETRIZADO = lf.codlocal
), seccao_dir_privado as (
		select lfparam.codlocalsistema, lfparam.codlocalparametrizado, lfparam.des_orgao, lfparam.localfisico as localfisico from (
		select cast(o.id_orgao as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%SECAODIRPRIV' and cast(o.id_orgao as int) <> cast(o.cod_orgao_secretaria as int)
		union all 
		select cast(o.cod_orgao_secretaria as int) as CODLOCALSISTEMA, cast(lf.codlocal as int) as CODLOCALPARAMETRIZADO, o.des_orgao, o.sig_orgao, lf.descr as localfisico
		from eproc2g.orgao o 
		left join ejud.orgaojulgador oj on o.id_orgao = oj.codorgjulg
		left join ejud.localfisico lf on oj.codlocal = cast(lf.codlocal as int)
		where UPPER(o.sig_orgao) like '%SECAODIRPRIV' and O.COD_ORGAO_SECRETARIA is not NULL) lfparam 
), LOCAL_EPROC2G as (
	select 
		unif_EPROC2G.codlocalsistema,
		unif_EPROC2G.codlocalparametrizado,
		unif_EPROC2G.localassociado,
		unif_EPROC2G.sistema,
		CASE 
			WHEN pro.id_orgao_destino is not null then 'S'
			else 'N'
		end as ind_movfisico
	from (
		select codlocalsistema, codlocalparametrizado, 'CDIRPUB' as localassociado, 'EPROC2G' as sistema from secretaria_dir_publica
		union ALL
		select codlocalsistema, codlocalparametrizado, 'CDIRPRIV' as localassociado, 'EPROC2G' as sistema from secretaria_dir_privado
		union ALL
		select codlocalsistema, codlocalparametrizado, 'GABINETE' as localassociado, 'EPROC2G' as sistema from gabinete_magistrado
		union ALL
		select codlocalsistema, codlocalparametrizado, 'GABJDS' as localassociado, 'EPROC2G' as sistema from gabinete_jds_magistrado
		union all
		select codlocalsistema, codlocalparametrizado, 'NUEF' as localassociado, 'EPROC2G' as sistema from nucleo_digital
		union all
		select codlocalsistema, codlocalparametrizado, 'GABNUCLEODIGITAL' as localassociado, 'EPROC2G' as sistema from gabinetes_nucleo
		union all
		select codlocalsistema, codlocalparametrizado, localassociado as localassociado, 'EPROC2G' as sistema from vice_presidencia
		union all
		select codlocalsistema, codlocalparametrizado, 'SGJUD' as localassociado, 'EPROC2G' as sistema from orgao_especial
		union all
		select codlocalsistema, codlocalparametrizado, 'PRESIDENCIA' as localassociado, 'EPROC2G' as sistema from presidencia
		union all
		select codlocalsistema, codlocalparametrizado, 'OUTROS' as localassociado, 'EPROC2G' as sistema from outros
		union all
		select codlocalsistema, codlocalparametrizado, 'SECCAO_DIR_PRIVADO' as localassociado, 'EPROC2G' as sistema from seccao_dir_privado
	) unif_eproc2g
	left join (select distinct id_orgao_destino from eproc2g.processo_remessa_orgao) pro
		on unif_EPROC2G.codlocalsistema = pro.id_orgao_destino
), LOCAL_EJUD as (
	select 
		LF.CODLOCAL as CODLOCALSISTEMA, 
		LF.CODLOCAL as codlocalparametrizado,
		'EJUD' as localassociado, 
		'EJUD' as sistema,
		CASE 
			WHEN MF.CODLOCAL is not null then 'S'
			else 'N'
		end as ind_movfisico
	from ejud.localfisico LF
	left join (select distinct CODLOCAL from ejud.MOVIMENTOFISICO) MF on LF.CODLOCAL = MF.CODLOCAL
	where lf.codlocal != 9999
)
select * from LOCAL_EPROC2G
union all
select * from LOCAL_EJUD;