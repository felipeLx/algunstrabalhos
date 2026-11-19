WITH MOV_RELATOR AS (
    SELECT 
        CODDOC,
        DTHRMOV,
        CODORGJULG,
        CODMAGREL as COD_MAGISTRADO
    FROM ejud.movimentorelator
), LOCAL_ASSOCIADO AS (
    SELECT 
    	LF.CODLOCAL, 
    	LF.CODMAG as COD_MAGISTRADO
    FROM ejud.localfisico LF
    WHERE LF.CODLOCAL NOT IN (3142, 3144, 3148)  
    AND LF.CODMAG IS NOT NULL
), MOV_DISTRIBUICAO AS (
    SELECT 
    	MD.CODDOC, 
    	MD.DTHRMOV, 
    	MD.CODfase,
    	F.DESCR as fase,
    	MR.COD_MAGISTRADO,
    	'DISTRIBUIDO_REDISTRIBUIDO' as TP_movimento,
        11 AS ITEM,
        1 AS SUBITEM
    FROM ejud.movdistribuicao MD 
    JOIN MOV_RELATOR MR ON MR.CODDOC = MD.CODDOC 
    JOIN ejud.t_processo P ON MD.CODDOC = P.CODDOC
    join ejud.fase F on MD.CODfase = F.CODfase
	AND MR.DTHRMOV = MD.DTHRMOV
    AND MD.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    WHERE MD.CODfase IN (26,36)
), MOV_JULGAMENTO_COLEGIADO AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        'JULGAMENTO_COLEGIADO' as TP_movimento,
        11 AS ITEM,
        2 AS SUBITEM,
        row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    JOIN ejud.faseexterna FE ON M.CODfase = FE.CODfase and FE.CODfaseEXT = 50193
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    WHERE M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    ) X where RN = 1
), MOV_JULGAMENTO_MONOCRATICO AS (
    SELECT 
        MJ.coddoc,
        MJ.dthrmov,
        MJ.codfase,
        F.DESCR as fase,
        MJ.codmagrel as COD_MAGISTRADO,
        'JULGAMENTO_MONOCRATICO' as TP_movimento,
        11 AS ITEM,
        3 AS SUBITEM
    FROM ejud.movjulg MJ
    join ejud.t_processo P on MJ.CODDOC = P.CODDOC
    join ejud.fase F on MJ.CODfase = F.CODfase
    WHERE MJ.codfase = 193 AND MJ.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
), MOV_BAIXA AS (
    select * from (
	SELECT 
    	M.CODDOC, 
    	M.DTHRMOV,
    	m.codfase,
    	F.DESCR as fase,
    	mr.COD_MAGISTRADO,
    	'BAIXA' as TP_movimento,
        11 AS ITEM,
        4 AS SUBITEM,
    	row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    join ejud.t_processo P on M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    LEFT JOIN ejud.movimentofisico MF ON M.CODDOC = MF.CODDOC AND M.DTHRMOV = MF.DTHRMOV
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV
    WHERE (
			--movimentos de BAIXA:
			--22: Baixa Definitiva
			(m.codfase = 22)
			--870: Autos Eliminados
			OR (m.codfase = 870)
			--50014: Remessa a Microfilmagem;
			OR (m.codfase = 50014)
			--861: Arquivamento com complemento 246: Definitivo;
			OR (m.codfase = 861 AND m.codcompl1 = 246)
			--50002: Certidao com complemento 1 50118: Processo Findo OU 66103: Descarte
			OR (m.codfase = 50002 AND m.codcompl1 in (50118,66103))
		) and M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
	) X where RN = 1
), MOV_DESPACHO as (
    SELECT 
        md.CODDOC, 
        md.DTHRMOV, 
        md.CODfase,
        F.DESCR as fase,
        md.CODMAG as COD_MAGISTRADO,
        'DESPACHO' as TP_movimento,
        12 AS ITEM,
        1 AS SUBITEM
    FROM ejud.movdespa md
    join ejud.fase f on md.codfase = f.codfase
    WHERE MD.CODfase = 11009 and md.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
), MOV_DECISAO as (
	SELECT 
        MD.CODDOC, 
        MD.DTHRMOV, 
        MD.CODfase,
        F.DESCR as fase,
        MD.CODMAG as COD_MAGISTRADO,
        'DECISAO_MONOCRATICA' as TP_movimento,
        12 AS ITEM,
        2 AS SUBITEM
    FROM ejud.movdespa MD
    join ejud.fase f on md.codfase = f.codfase
    WHERE MD.CODfase = 3 and md.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
), MOV_ACORDAO_VOTO as (
    SELECT 
        MOV.CODDOC, 
        MOV.DTHRMOV, 
        F.CODfase,
        F.DESCR as fase,
        LA.COD_MAGISTRADO,
        'VOTO' as TP_movimento,
        12 AS ITEM,
        3 AS SUBITEM
    FROM ejud.movimento MOV
    JOIN ejud.gabineteprocesso PG ON MOV.CODDOC = PG.CODDOC AND MOV.DTHRMOV = PG.DTHRMOV
    JOIN ejud.fase F ON F.CODfase = MOV.CODfase
    JOIN ejud.faseexterna FE ON F.CODfase = FE.CODfase
    left join LOCAL_ASSOCIADO LA on PG.CODLOCALRESP = LA.CODLOCAL
    WHERE FE.CODfaseEXT IN (50100,50102) and mov.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
), MOV_DISTRIBUICAO_PAD AS (
	SELECT 
	    MD.CODDOC, 
	    MD.DTHRMOV, 
	    MD.CODfase,
	    F.DESCR as fase,
	    MR.COD_MAGISTRADO,
	    'DISTRIBUIDO_REDISTRIBUIDO' as TP_movimento,
	    13 AS ITEM,
	    2 as SUBITEM
	FROM ejud.movdistribuicao MD 
	JOIN MOV_RELATOR MR ON MR.CODDOC = MD.CODDOC 
	JOIN ejud.t_processo P ON MD.CODDOC = P.CODDOC
	JOIN ejud.fase F ON MD.CODfase = F.CODfase
	    AND MR.DTHRMOV = MD.DTHRMOV
	WHERE MD.CODfase IN (26,36) 
	  AND P.CODTIPPROC IN (101, 118, 221)
	  AND MD.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
), MOV_FINALIZADO_PAD AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        'FINALIZADO' as TP_movimento,
	    13 AS ITEM,
	    3 as SUBITEM,
	    row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    where M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND P.CODTIPPROC IN (101, 118, 221)
    and ((m.codfase = 22)
			--870: Autos Eliminados
			OR (m.codfase = 870)
			--50011: Registro do Acordao;
			OR (m.codfase = 50011)
			--50014: Remessa a Microfilmagem;
			OR (m.codfase = 50014)
			--861: Arquivamento com complemento 246: Definitivo;
			OR (m.codfase = 861 AND m.codcompl1 = 246)
		)
	) x where rn = 1
), MOV_JULGAMENTO_COLEGIADO_PAD AS (
    SELECT 
        MJ.coddoc,
        MJ.dthrmov,
        MJ.codfase,
        F.DESCR as fase,
        MJ.codmagrel as COD_MAGISTRADO,
        'JULGAMENTO_COLEGIADO' as TP_movimento,
	    13 AS ITEM,
	    5 as SUBITEM
    FROM ejud.movjulg MJ
    join ejud.t_processo P on MJ.CODDOC = P.CODDOC
    join ejud.fase F on MJ.CODfase = F.CODfase
    WHERE MJ.CODfase = 193
    and MJ.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND P.CODTIPPROC IN (101, 118, 221)
), MOV_JULGAMENTO_MONOCRATICO_PAD AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        'JULGAMENTO_MONOCRATICO' as TP_movimento,
	    13 AS ITEM,
	    4 as SUBITEM,
        row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.coddoc = P.coddoc
    join ejud.fase F on M.CODfase = F.codfase
    JOIN ejud.faseexterna FE ON M.CODfase = FE.CODfase and FE.codfaseext = 50193
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    WHERE M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND P.CODTIPPROC IN (101, 118, 221)
    ) X where RN = 1
), movimentos_unidos as (
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_DISTRIBUICAO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_JULGAMENTO_COLEGIADO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_JULGAMENTO_MONOCRATICO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_BAIXA
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_DESPACHO
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_DECISAO
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_ACORDAO_VOTO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_DISTRIBUICAO_PAD
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_JULGAMENTO_COLEGIADO_PAD
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, tp_movimento, item, subitem from MOV_JULGAMENTO_MONOCRATICO_PAD
)
SELECT 
	cast(MU.CODDOC as DECIMAL(30,0)) as CODDOC,
	MU.DTHRMOV as DTH_movimento,
	cast(MU.CODfase as int) as COD_fase,
	cast(MU.fase as varchar(255)) as fase,
	cast(MU.COD_MAGISTRADO as int) as COD_MAGISTRADO,
	cast(LA.CODLOCAL as int) as COD_ORGAO_GABINETE,
	CAST(MU.TP_movimento as varchar(255)) as TP_movimento,
	cast(MU.ITEM as int) as ITEM,
	cast(MU.SUBITEM as int) as SUBITEM,
	cast('EJUD' as varchar(255)) as IND_SISTEMA,
	current_timestamp as DTH_ETL
FROM movimentos_unidos MU
left join local_associado LA on MU.COD_MAGISTRADO = LA.COD_MAGISTRADO;
