INSERT INTO insp2inst.dim_orgao (
	cod_orgao,
    cod_orgao_cnj,
    nm_orgao_scol,
    nm_orgao,
    cod_competencia,
    nm_competencia,
    nm_sigla,
	ind_ejud,
	ind_eproc2g,
	ind_distribuicao,
    num_ordinal
)

select * from (
with orgao_ejud AS (
    SELECT 
        oj.codorgjulg AS codorg,
        case 
        	when mr.codorgjulg is not null then 'S' else 'N'
        end as ind_distr,
       	oj.codorgjulgcnj as codorgcnj
    FROM ejud.orgaojulgador oj
    LEFT JOIN ejud.camaracivelespecializada c ON oj.codorgjulg = c.codorgjulgciv
    JOIN scol.orgao so ON oj.codorgjulg = so.cod_org 
    left join (select distinct codorgjulg from ejud.movimentorelator) mr on oj.codorgjulg = mr.codorgjulg
), ORGAO_EPROC as (
    SELECT 
        id_orgao AS codorg,
        case 
        	when hdj.id_orgao_colegiado is not null then 'S' else 'N'
        end as ind_distr,
        eo.id_orgao_cnj as codorgcnj
    FROM eproc2g.orgao eo
    JOIN scol.orgao so 
      ON eo.id_orgao = so.cod_org
    left join (
    	select distinct id_orgao_colegiado from (
	    	select distinct id_orgao_colegiado from eproc2g.log_processo where id_orgao_colegiado is not null 
	    	union all
	    	select distinct id_orgao_colegiado from eproc2g.processo where id_orgao_colegiado is not null 
    	) x
    ) hdj on hdj.id_orgao_colegiado = eo.id_orgao
), ORGAO_UNIDOS as (
    SELECT DISTINCT
        COALESCE(ejud.codorg, eproc.codorg) AS codorg,
        coalesce(ejud.codorgcnj, eproc.codorgcnj) AS codorgcnj,
	    CASE 
	        WHEN ejud.ind_distr = 'S' OR eproc.ind_distr = 'S' 
	        THEN 'S' 
	        ELSE 'N' 
	    END AS ind_distr,
	    CASE 
	        WHEN ejud.codorg IS NOT NULL THEN 'S' 
	        ELSE 'N' 
	    END AS ind_ejud,
	    CASE 
	        WHEN eproc.codorg IS NOT NULL THEN 'S' 
	        ELSE 'N' 
	    END AS ind_eproc
    FROM orgao_ejud ejud
    FULL OUTER JOIN orgao_eproc eproc ON ejud.codorg = eproc.codorg
), ORGAO_SCOL as (
	select 
		SC.cod_org, 
		coalesce(SC.cod_org_cnj, ou.codorgcnj) as codorgcnj, 
		SC.nome, 
		SC.nome_shs,
        OU.ind_ejud,
        OU.ind_eproc,
        OU.IND_DISTR
    FROM SCOL.orgao SC
    JOIN ORGAO_UNIDOS OU 
      ON OU.codorg = SC.cod_org
), EJUD_COMPETENCIA as (
	select 
		oj.codorgjulg,
		OJ.nomebnmp,
		p.nome as orgaojulgador,
		c.codcompt as codcompt,
		oj.codmagpres,
		c.descr as competencia,
		OJ.CODPESS
	from ejud.orgaojulgador oj
	left join ejud.competencia c on oj.codcompt = c.codcompt
	join ejud.pessoa p on oj.codpess = p.codpess
), ORGAO_SCOL_NOMESIGLA as (
SELECT
    SC.cod_org, 
    SC.codorgcnj, 
    C.CODCOMPT::INT,
    C.COMPETENCIA,
    SC.ind_ejud,
    SC.ind_eproc,
    SC.IND_DISTR,
    SC.NOME,
    C.CODPESS,
    CASE 
	    WHEN sc.nome ~ '[0-9]+' 
	         THEN LPAD(SUBSTRING(sc.nome FROM '[0-9]+'), 2, '0')
	    ELSE NULL
	END AS sigla_numeral,
    CASE
        WHEN c.codcompt = 1 THEN 'SETOE'
        WHEN c.codcompt = 2 THEN 'DECON'
        WHEN c.codcompt = 3 THEN 'SECIV'
        WHEN c.codcompt = 4 THEN 'SECRI'
        WHEN c.codcompt = 5 THEN 'GCCIV'
        WHEN c.codcompt = 6 THEN 'GCCRI'
        WHEN c.codcompt = 7 THEN 'CCIV'
        WHEN c.codcompt = 8 THEN 'CCRI'
        WHEN c.codcompt = 9 THEN '3VP'
        WHEN c.codcompt = 10 THEN 'TRCRI'
        WHEN c.codcompt = 11 THEN 'TRCIV'
        WHEN c.codcompt = 12 THEN '2VP'
        WHEN c.codcompt = 14 AND SC.COD_ORG IN (6739,6740) THEN 'TRFAZ'
        WHEN c.codcompt = 14 AND SC.COD_ORG = 9816 THEN 'TRFAZEXT'
        --CODCOMTP 16 N EXISTE
        WHEN c.codcompt = 17 AND sc.cod_org = 9377 THEN 'SECIV'
        WHEN c.codcompt = 17 AND sc.cod_org = 9378 THEN 'SECON'
        --CODCOMTP 18 N EXISTE
        WHEN c.codcompt = 19 AND sc.cod_org = 9525 THEN 'TRUCIV'
        WHEN c.codcompt = 20 AND sc.cod_org = 9940 THEN 'TRUFAZ'
        WHEN c.codcompt = 21 THEN '2VP'
        --NAO EXISTE ORGAO CODCOMP 22
        WHEN c.codcompt = 23 THEN 'CPRI'
        WHEN c.codcompt = 24 THEN 'CPUB'
        WHEN c.codcompt = 25 THEN 'CEMP'
        WHEN c.codcompt = 26 THEN 'SEPUB'
        WHEN c.codcompt = 27 THEN 'SEPRI'
        WHEN c.codcompt = 28 THEN 'CEREU'
        WHEN c.codcompt = 29 THEN 'NUDIG'
    END AS sigla,
	CASE 
		WHEN sc.nome ~ '[0-9]+' and c.codcompt != 7 THEN SUBSTRING(sc.nome FROM '[0-9]+')
		when c.nomebnmp ~ '[0-9]+' and c.codcompt = 7 THEN SUBSTRING(c.nomebnmp FROM '[0-9]+')
	    ELSE NULL
	END AS nome_numeral, 
	CASE
	    WHEN C.CODCOMPT in (5,6) THEN 'º'
	    ELSE 'ª'
	END AS ARTIGO,
	case
		when C.CODCOMPT = 1 then 'Orgão Especial'
		when C.CODCOMPT = 5 then 'Secretaria do X Grupo de Câmaras Cíveis'
		when C.CODCOMPT = 6 and c.codmagpres is not null then 'X Grupo de Câmara Criminal'
		when C.CODCOMPT = 6 and c.codmagpres is null then 'Secretaria do X Grupo de Câmara Criminais'
		when C.CODCOMPT = 7 then 'X Câmara Cível'
		when C.CODCOMPT = 8 then 'X Câmara Criminal'
		when C.CODCOMPT = 10 then 'X Turma Recursal Criminal'
		when C.CODCOMPT = 11 then 'X Turma Recursal Cível'
		when C.CODCOMPT = 14 then 'X Turma Recursal Fazendária'
		when C.CODCOMPT = 23 then 'X Câmara de Direito Privado'
		when C.CODCOMPT = 24 then 'X Câmara de Direito Público'
		when C.CODCOMPT = 25 then 'X Câmara de Direito Empresarial'
		when C.CODCOMPT = 26 then 'Seção de Direito Público'
		when C.CODCOMPT = 27 then 'Seção de Direito Privado'
		else SC.nome
		end NOME_PADRONIZADO
FROM ORGAO_SCOL SC
left JOIN EJUD_COMPETENCIA C ON SC.cod_org = C.CODORGJULG
)
select
	COD_ORG as cod_orgao,
	CODORGCNJ as cod_orgao_cnj,
	NOME as nm_orgao_scol,
	case 
		when NOME_NUMERAL is not null then REPLACE(NOME_PADRONIZADO, 'X', NOME_NUMERAL || ARTIGO) 
		else NOME 
	end AS nm_orgao,
	CODCOMPT as cod_competencia,
	COMPETENCIA as nm_competencia,
	case
		when SIGLA_NUMERAL is null then SIGLA
		when CODCOMPT = 9 then SIGLA
		when CODCOMPT = 21 then SIGLA
		when COD_ORG = 522 then 'CM'
		when COD_ORG = 522 then 'GABPRES'
		when CODCOMPT in (12,21) then '2VP'
		when CODCOMPT in (29) then 'NUDIG'
		else SIGLA_NUMERAL ||  SIGLA
	end as nm_sigla,
	IND_EJUD as ind_ejud,
	IND_EPROC as ind_eproc2g,
	IND_DISTR as ind_distribuicao,
	CASE
		when CODCOMPT IN (6,21) then 	ROW_NUMBER() OVER (PARTITION BY CODCOMPT ORDER BY CODPESS::INTEGER ASC)
		when codcompt != 6 then ROW_NUMBER() OVER (PARTITION BY CODCOMPT ORDER BY NOME_NUMERAL::INTEGER ASC)
		else null
	end as NUM_ORDINAL
from ORGAO_SCOL_NOMESIGLA
union all
select 
	0 as cod_orgao,
	null cod_orgao_cnj,
	'NÃO ENCONTRADO' AS   nm_orgao_scol,
	'NÃO ENCONTRADO' AS   nm_orgao,
	null AS cod_competencia,
	null AS nm_competencia,
	null AS nm_sigla,
	'?' AS ind_ejud,
	'?' AS ind_eproc2g,
	'?' as ind_distribuicao,
	NULL AS num_ordinal) x

ON CONFLICT (cod_orgao)
DO UPDATE SET
    cod_orgao_cnj = EXCLUDED.cod_orgao_cnj,
    nm_orgao_scol = EXCLUDED.nm_orgao_scol,
    nm_orgao = EXCLUDED.nm_orgao,
    cod_competencia = EXCLUDED.cod_competencia,
    nm_competencia = EXCLUDED.nm_competencia,
    nm_sigla = EXCLUDED.nm_sigla,
    ind_ejud = EXCLUDED.ind_ejud,
    ind_eproc2g = EXCLUDED.ind_eproc2g,
    ind_distribuicao = EXCLUDED.ind_distribuicao;