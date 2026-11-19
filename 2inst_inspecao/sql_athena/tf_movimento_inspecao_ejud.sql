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
    	MD.CODFASE,
    	F.DESCR as FASE,
    	MR.COD_MAGISTRADO,
    	'DISTRIBUIDO_REDISTRIBUIDO' as TP_MOVIMENTO,
    	'ITEM_11_1' AS ITEM
    FROM ejud.movdistribuicao MD 
    JOIN MOV_RELATOR MR ON MR.CODDOC = MD.CODDOC 
    JOIN ejud.T_PROCESSO P ON MD.CODDOC = P.CODDOC
    join EJUD.FASE F on MD.CODFASE = F.CODFASE
	AND MR.DTHRMOV = MD.DTHRMOV
    WHERE MD.CODFASE IN (26,36)
), MOV_JULGAMENTO_COLEGIADO AS (
    select * from (
	SELECT 
        M.CODDOC,
        M.DTHRMOV,
        M.CODFASE,
        F.DESCR as FASE,
        MR.COD_MAGISTRADO,
        'JULGAMENTO_COLEGIADO' as TP_MOVIMENTO,
        'ITEM_11_2' AS ITEM,
        row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM EJUD.MOVIMENTO M
    JOIN ejud.T_PROCESSO P ON M.CODDOC = P.CODDOC
    join EJUD.FASE F on M.CODFASE = F.CODFASE
    JOIN EJUD.FASEEXTERNA FE ON M.CODFASE = FE.CODFASE and FE.CODFASEEXT = 50193
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV 
    ) X where RN = 1
), MOV_JULGAMENTO_MONOCRATICO AS (
    SELECT 
        MJ.coddoc,
        MJ.dthrmov,
        MJ.codfase,
        F.DESCR as FASE,
        MJ.codmagrel as COD_MAGISTRADO,
        'JULGAMENTO_MONOCRATICO' as TP_MOVIMENTO,
        'ITEM_11_3' AS ITEM
    FROM EJUD.movjulg MJ
    join EJUD.T_PROCESSO P on MJ.CODDOC = P.CODDOC
    join EJUD.FASE F on MJ.CODFASE = F.CODFASE
    WHERE MJ.codfase = 193 
), MOV_BAIXA AS (
    select * from (
	SELECT 
    	M.CODDOC, 
    	M.DTHRMOV,
    	m.codfase,
    	F.DESCR as FASE,
    	mr.COD_MAGISTRADO,
    	'BAIXA' as TP_MOVIMENTO,
    	'ITEM_11_4' AS ITEM,
    	row_number() over(partition by MR.coddoc order by MR.dthrmov desc) RN
    FROM ejud.movimento M
    join ejud.T_PROCESSO P on M.CODDOC = P.CODDOC
    join EJUD.FASE F on M.CODFASE = F.CODFASE
    LEFT JOIN ejud.movimentofisico MF ON M.CODDOC = MF.CODDOC AND M.DTHRMOV = MF.DTHRMOV
    JOIN MOV_RELATOR MR on M.CODDOC = MR.CODDOC and MR.dthrmov <= M.DTHRMOV
    WHERE (
			--Movimentos de BAIXA:
			--22: Baixa Definitiva
			(m.codfase = 22)
			--870: Autos Eliminados
			OR (m.codfase = 870)
			--50011: Registro do Acordao;
--			OR (m.codfase = 50011)
			--50014: Remessa a Microfilmagem;
			OR (m.codfase = 50014)
			--861: Arquivamento com complemento 246: Definitivo;
			OR (m.codfase = 861 AND m.codcompl1 = 246)
			--50002: Certidao com complemento 1 50118: Processo Findo OU 66103: Descarte
			OR (m.codfase = 50002 AND m.codcompl1 in (50118,66103))
--			60: Expedição de documento com complemento 1 50007: Oficio E tendo o complemento 2
--			OR (m.codfase = 60 AND m.codcompl1 = 50007 AND m.codcompl2 IN (50119,66356))
--			60: Expedição de documento com complemento 1 66103: Descarte E complemento 2 296: Trânsito em Julgado / Custas / Desentranhamento de peças;
--			OR (m.codfase = 60 AND m.codcompl1 = 66103 AND m.codcompl2 = 296)
--			123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 2:  50052: Baixa definitiva; 50078: Interposicao de RE/RESP; ¿ 50079: Interposicao de RO;¿ 50080: Para autuar Embargos Infringentes;
--			OR (m.codfase IN (123,50123) AND m.codcompl2 IN (50052,50078,50079,50080))
--			123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 um dos seguintes: 50104, 50105, 50648, 50649
--			OR (m.codfase IN (123,50123) AND m.codcompl2 IN (50104, 50105, 50648, 50649))
--			123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 
--			OR (m.codfase in (123,50123) AND m.codcompl1 = 50723 AND mf.codlocal = 515)
--			 123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1:
--			OR (m.codfase in (123,50123) AND m.codcompl1 in (50653,50655,50656) AND mf.codlocal IN (88,2017,3763))
--			123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com destino:
--			OR (m.codfase in (123,50123) AND mf.codlocal IN (503,519,520,521,549,2085,2593,2594,3146,3147,3148,3149,3150,3151,3205,3206,3582,3583,3584,3585,3875,5765,6919))
--			Inclusão de novos locais através da SS2022.0106136:
--			OR (m.codfase in (123,50123) AND mf.codlocal IN (3144,10682,10683,10684,10685,10686,4580,8972))
		) 
	) X where RN = 1
), MOV_DESPACHO as (
    SELECT 
        md.CODDOC, 
        md.DTHRMOV, 
        md.CODFASE,
        F.DESCR as FASE,
        md.CODMAG as COD_MAGISTRADO,
        'DESPACHO' as TP_MOVIMENTO,
        'ITEM_12_1' AS ITEM
    FROM EJUD.movdespa md
    join ejud.fase f on md.codfase = f.codfase
    WHERE MD.CODFASE = 11009
--), MOV_DECISAO as
), MOV_DECISAO as (
	SELECT 
        MD.CODDOC, 
        MD.DTHRMOV, 
        MD.CODFASE,
        F.DESCR as FASE,
        MD.CODMAG as COD_MAGISTRADO,
        'DECISAO_MONOCRATICA' as TP_MOVIMENTO,
        'ITEM_12_2' as ITEM
    FROM EJUD.movdespa MD
    join ejud.fase f on md.codfase = f.codfase
    WHERE MD.CODFASE = 3
), MOV_ACORDAO_VOTO as (
    SELECT 
        MOV.CODDOC, 
        MOV.DTHRMOV, 
        F.CODFASE,
        F.DESCR as FASE,
        LA.COD_MAGISTRADO,
        'VOTO' as TP_MOVIMENTO,
        'ITEM_12_3' as ITEM
    FROM EJUD.MOVIMENTO MOV
    JOIN EJUD.GABINETEPROCESSO PG ON MOV.CODDOC = PG.CODDOC AND MOV.DTHRMOV = PG.DTHRMOV
    JOIN EJUD.FASE F ON F.CODFASE = MOV.CODFASE
    JOIN EJUD.FASEEXTERNA FE ON F.CODFASE = FE.CODFASE
    left join LOCAL_ASSOCIADO LA on PG.CODLOCALRESP = LA.CODLOCAL
    WHERE FE.CODFASEEXT IN (50100,50102)
), MOVIMENTOS_UNIDOS as (
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_DISTRIBUICAO
	union all
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_JULGAMENTO_COLEGIADO
	union all
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_JULGAMENTO_MONOCRATICO
	union all
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_BAIXA
	union all 
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_DESPACHO
	union all 
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_DECISAO
	union all 
	select CODDOC, DTHRMOV, CODFASE, FASE, COD_MAGISTRADO, TP_MOVIMENTO, ITEM from MOV_ACORDAO_VOTO
)
SELECT 
	cast(MU.CODDOC as DECIMAL(30,0)) as CODDOC,
	MU.DTHRMOV as DTH_MOVIMENTO,
	cast(MU.CODFASE as int) as COD_FASE,
	cast(MU.FASE as varchar(255)) as FASE,
	cast(MU.COD_MAGISTRADO as int) as COD_MAGISTRADO,
	cast(LA.CODLOCAL as int) as COD_ORGAO_GABINETE,
	CAST(MU.TP_MOVIMENTO as varchar(255)) as TP_MOVIMENTO,
	cast(MU.ITEM as varchar(255)) as ITEM,
	cast('EJUD' as varchar(255)) as IND_SISTEMA,
	current_timestamp as DTH_ETL
FROM MOVIMENTOS_UNIDOS MU
left join LOCAL_ASSOCIADO LA on MU.COD_MAGISTRADO = LA.COD_MAGISTRADO;
