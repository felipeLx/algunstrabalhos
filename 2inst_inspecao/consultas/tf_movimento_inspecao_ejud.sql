with LOCAL_ASSOCIADO AS (
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
    	MR.CODORGJULG,
    	'DISTRIBUIDO_REDISTRIBUIDO' as TP_movimento,
        11 AS ITEM,
        1 AS SUBITEM,
        6 AS ITEM_V2,
        1 AS SUBITEM_v2
    FROM ejud.movdistribuicao MD 
    JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR ON MR.CODDOC = MD.CODDOC AND MR.DTHRMOV = MD.DTHRMOV
    JOIN ejud.t_processo P ON MD.CODDOC = P.CODDOC
    join ejud.fase F on MD.CODfase = F.CODfase
    AND MD.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND MD.DTHRMOV <= current_timestamp()
    WHERE MD.CODfase IN (26,36)
), MOV_JULGAMENTO_COLEGIADO AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        MR.CODORGJULG,
        'JULGAMENTO_COLEGIADO' as TP_movimento,
        11 AS ITEM,
        2 AS SUBITEM,
        6 AS ITEM_V2,
        2 AS SUBITEM_v2,
        row_number() over(partition by M.coddoc, M.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    JOIN ejud.faseexterna FE ON M.CODfase = FE.CODfase and FE.CODfaseEXT = 50193
    JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    WHERE M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND M.DTHRMOV <= current_timestamp()
    ) X where RN = 1
), MOV_JULGAMENTO_MONOCRATICO AS (
    SELECT 
        MJ.coddoc,
        MJ.dthrmov,
        MJ.codfase,
        F.DESCR as fase,
        MJ.codmagrel as COD_MAGISTRADO,
        MJ.CODORGJULG,
        'JULGAMENTO_MONOCRATICO' as TP_movimento,
        11 AS ITEM,
        3 AS SUBITEM,
        6 AS ITEM_V2,
        3 AS SUBITEM_v2
    FROM ejud.movjulg MJ
    join ejud.t_processo P on MJ.CODDOC = P.CODDOC
    join ejud.fase F on MJ.CODfase = F.CODfase
    WHERE MJ.codfase = 193 AND MJ.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND MJ.DTHRMOV <= current_timestamp()
), MOV_BAIXA AS (
    select * from (
	SELECT 
    	M.CODDOC, 
    	M.DTHRMOV,
    	m.codfase,
    	F.DESCR as fase,
    	mr.COD_MAGISTRADO,
    	mr.codorgjulg,
    	'BAIXA' as TP_movimento,
        11 AS ITEM,
        4 AS SUBITEM,
        6 AS ITEM_V2,
        4 AS SUBITEM_v2,
    	row_number() over(partition by M.coddoc, M.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    join ejud.t_processo P on M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    LEFT JOIN ejud.movimentofisico MF ON M.CODDOC = MF.CODDOC AND M.DTHRMOV = MF.DTHRMOV
    JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV
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
		AND M.DTHRMOV <= current_timestamp()
	) X where RN = 1
), MOV_DESPACHO as (
    select * from (
        SELECT 
            md.CODDOC, 
            md.DTHRMOV, 
            md.CODfase,
            F.DESCR as fase,
            md.CODMAG as COD_MAGISTRADO,
            mr.CODORGJULG,
            'DESPACHO' as TP_movimento,
            12 AS ITEM,
            1 AS SUBITEM,
            cast(null as int) AS ITEM_V2,
            cast(null as int) AS SUBITEM_v2,
            row_number() over(partition by md.coddoc, md.dthrmov order by mr.dthrmov desc) RN
        FROM ejud.movdespa md
        join ejud.fase f on md.codfase = f.codfase
        LEFT JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR 
            on md.CODDOC = mr.CODDOC  and mr.dthrmov <= md.DTHRMOV
        WHERE md.CODfase = 11009 
          and md.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
          AND md.DTHRMOV <= current_timestamp()
    ) X where RN = 1
), MOV_DECISAO as (
    SELECT * FROM (
    SELECT 
        md.CODDOC, 
        md.DTHRMOV, 
        md.CODfase,
        f.DESCR as fase,
        md.CODMAG as COD_MAGISTRADO,
        mr.CODORGJULG,
        'DECISAO_MONOCRATICA' as TP_movimento,
        12 AS ITEM,
        2 AS SUBITEM,
        cast(null as int) AS ITEM_V2,
        cast(null as int) AS SUBITEM_v2,
        row_number() over(partition by md.coddoc, md.dthrmov order by mr.dthrmov desc) RN
    FROM ejud.movdespa md
    JOIN ejud.fase f 
        ON md.codfase = f.codfase
    LEFT JOIN  (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR
        ON md.CODDOC = mr.CODDOC 
       AND mr.dthrmov <= md.DTHRMOV
    WHERE md.CODfase = 3 
      AND md.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
      AND md.DTHRMOV <= current_timestamp()
    ) X WHERE RN = 1
), MOV_ACORDAO_VOTO as (
    select * from (
	SELECT 
        MOV.CODDOC, 
        MOV.DTHRMOV, 
        F.CODfase,
        F.DESCR as fase,
        LA.COD_MAGISTRADO,
        MR.CODORGJULG,
        'VOTO' as TP_movimento,
        12 AS ITEM,
        3 AS SUBITEM,
        cast(null as int) AS ITEM_V2,
        cast(null as int) AS SUBITEM_v2,
        row_number() over(partition by MOV.CODDOC, MOV.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movimento MOV
    JOIN ejud.gabineteprocesso PG ON MOV.CODDOC = PG.CODDOC AND MOV.DTHRMOV = PG.DTHRMOV
    JOIN ejud.fase F ON F.CODfase = MOV.CODfase
    JOIN ejud.faseexterna FE ON F.CODfase = FE.CODfase
    left JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on MR.CODDOC = MOV.CODDOC and MR.dthrmov <= MOV.DTHRMOV
    left join LOCAL_ASSOCIADO LA on PG.CODLOCALRESP = LA.CODLOCAL
    WHERE FE.codfaseext IN (50100,50102) 
    and mov.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 second
    and mov.dthrmov <= current_timestamp()
    ) X where RN = 1
), MOV_DISTRIBUICAO_PAD AS (
	SELECT 
	    MD.CODDOC, 
	    MD.DTHRMOV, 
	    MD.CODfase,
	    F.DESCR as fase,
	    MR.COD_MAGISTRADO,
	    MR.CODORGJULG,
	    'DISTRIBUIDO_REDISTRIBUIDO_PAD' as TP_movimento,
	    13 AS ITEM,
	    2 as SUBITEM,
        7 AS ITEM_V2,
        2 AS SUBITEM_v2
	FROM ejud.movdistribuicao MD 
	LEFT JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR ON MR.CODDOC = MD.CODDOC AND MR.DTHRMOV = MD.DTHRMOV
	JOIN ejud.t_processo P ON MD.CODDOC = P.CODDOC
	JOIN ejud.fase F ON MD.CODfase = F.CODfase  
	WHERE MD.CODfase IN (26,36) 
	  AND P.CODTIPPROC IN (101, 118, 221)
	  AND MD.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
	  and MD.DTHRMOV <= current_timestamp()
), MOV_FINALIZADO_PAD AS (
    select * from (
    SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        MR.CODORGJULG,
        'FINALIZADO_PAD' as TP_movimento,
        13 AS ITEM,
        3 as SUBITEM,
        cast(null as int) AS ITEM_V2,
        cast(null as int) AS SUBITEM_v2,
        row_number() over(partition by M.coddoc, M.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.CODDOC = P.CODDOC
    join ejud.fase F on M.CODfase = F.CODfase
    LEFT JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    where m.dthrmov >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    and m.dthrmov <= current_timestamp()
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
    select * from (
	SELECT 
        MJ.coddoc,
        MJ.dthrmov,
        MJ.codfase,
        F.DESCR as fase,
        MJ.codmagrel as COD_MAGISTRADO,
        MR.CODORGJULG,
        'JULGAMENTO_COLEGIADO_PAD' as TP_movimento,
	    13 AS ITEM,
	    5 as SUBITEM,
        7 AS ITEM_V2,
        4 AS SUBITEM_v2,
	    row_number() over(partition by MJ.coddoc, MJ.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movjulg MJ
    join ejud.t_processo P on MJ.CODDOC = P.CODDOC
    join ejud.fase F on MJ.CODFASE = F.CODFASE
    LEFT JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on MJ.CODDOC = MR.CODDOC and MR.dthrmov <= MJ.DTHRMOV 
    WHERE MJ.CODfase = 193
    and MJ.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    AND P.CODTIPPROC IN (101, 118, 221)
    and mj.dthrmov <= current_timestamp
    ) X where RN = 1
), MOV_JULGAMENTO_MONOCRATICO_PAD AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODfase,
        F.DESCR as fase,
        MR.COD_MAGISTRADO,
        MR.CODORGJULG,
        'JULGAMENTO_MONOCRATICO_PAD' as TP_movimento,
	    13 AS ITEM,
	    4 as SUBITEM,
        7 AS ITEM_V2,
        3 AS SUBITEM_v2,
        row_number() over(partition by M.coddoc, M.DTHRMOV order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    JOIN ejud.t_processo P ON M.coddoc = P.coddoc
    join ejud.fase F on M.CODfase = F.codfase
    JOIN ejud.faseexterna FE ON M.CODfase = FE.CODfase and FE.codfaseext = 50193
    LEFT JOIN (SELECT CODDOC, DTHRMOV, CODORGJULG, CODMAGREL as COD_MAGISTRADO FROM ejud.movimentorelator) MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    WHERE M.DTHRMOV >= date_trunc('day', add_months(current_timestamp(), -12)) - INTERVAL 1 SECOND 
    and m.dthrmov <= current_timestamp
    AND P.CODTIPPROC IN (101, 118, 221)
    ) X where RN = 1
), movimentos_unidos as (
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_DISTRIBUICAO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_JULGAMENTO_COLEGIADO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_JULGAMENTO_MONOCRATICO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_BAIXA
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_DESPACHO
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_DECISAO
	union all 
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_ACORDAO_VOTO
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_DISTRIBUICAO_PAD
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_JULGAMENTO_COLEGIADO_PAD
	union all
	select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_JULGAMENTO_MONOCRATICO_PAD
    union all
    select coddoc, dthrmov, codfase, fase, cod_magistrado, codorgjulg, tp_movimento, item, subitem, item_v2, subitem_v2 from MOV_FINALIZADO_PAD
)
SELECT 
	current_timestamp as DTH_REFERENCIA,
	cast(MU.CODDOC as DECIMAL(30,0)) as CODDOC,
	MU.DTHRMOV as DTH_MOVIMENTO,
	cast(MU.CODfase as int) as COD_FASE,
	cast(MU.fase as varchar(255)) as FASE,
	cast(MU.COD_MAGISTRADO as int) as COD_MAGISTRADO,
	cast(MU.codorgjulg AS int) as COD_ORGAOJULGADOR,
	cast(LA.CODLOCAL as int) as COD_ORGAO_GABINETE,
	CAST(MU.TP_movimento as varchar(255)) as TP_MOVIMENTO,
	cast(MU.ITEM as int) as ITEM,
	cast(MU.SUBITEM as int) as SUBITEM,
    cast(MU.ITEM_V2 as int) as ITEM_V2,
	cast(MU.SUBITEM_V2 as int) as SUBITEM_V2,
	cast('EJUD' as varchar(255)) as IND_SISTEMA,
	current_timestamp as DTH_ETL
FROM movimentos_unidos MU
left join local_associado LA on MU.COD_MAGISTRADO = LA.COD_MAGISTRADO;
