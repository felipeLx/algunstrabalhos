with acervo as ( 
	select  
		DATAREFERENCIA as DTH_REFERENCIA,
		CODDOC,
		DATAULTDIS as DTH_ULTIMA_DISTRIBUICAO,
		CODULTREL as COD_ULTIMO_RELATOR,
		CODULTORGJULG as COD_ULTIMO_ORGAO_JULGADOR
	from dw2inst.acervo_ejud
), mov_distribuicao AS (
    SELECT 
    	md.coddoc, 
    	md.dthrmov, 
    	md.codfase
    FROM acervo a
    join ejud.movdistribuicao md on a.coddoc = md.coddoc
    WHERE md.DTHRMOV <= a.DTH_REFERENCIA
    AND codfase IN (26, 36, 60012)
), mov_distribuicao_cancelada AS (
    SELECT 
    	mcr.coddoc, 
    	mcr.dthrmov, 
    	mcr.dthrmovdistrcance
    FROM acervo a
    join ejud.movcancedistr mcr on a.coddoc = mcr.coddoc
    WHERE mcr.DTHRMOV <= a.dth_referencia
), mov_distribuicao_primeira_valida AS (
    SELECT * FROM (
        SELECT 
            md.coddoc,
            md.dthrmov,
            mr.codmagrel,
            mr.codorgjulg,
            ROW_NUMBER() OVER (PARTITION BY md.coddoc ORDER BY md.dthrmov asc) AS rn
        FROM mov_distribuicao md
        LEFT JOIN mov_distribuicao_cancelada mdc ON md.coddoc = mdc.coddoc AND md.dthrmov = mdc.dthrmovdistrcance
        join ejud.movimentorelator mr on md.coddoc = mr.coddoc and md.dthrmov = mr.dthrmov
        WHERE mdc.dthrmovdistrcance IS NULL
    ) WHERE rn = 1
), mov_suspensao AS (
    select * from (
		SELECT
	        m.coddoc,
	        m.dthrmov,
	        m.codfase,
	        m.codcompl1,
	        m.codcompl2,
	        m.codcompl3,
	        ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	    from ACERVO A 
	    join EJUD.MOVIMENTO M on A.CODDOC = M.CODDOC
	    WHERE m.dthrmov <= A.DTH_REFERENCIA
	    AND (
	        (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
	        OR (m.codfase IN (3, 20) AND m.codcompl1 = 25)
	        OR (m.codfase IN (3, 20, 29) AND m.codcompl1 = 1013 AND COALESCE(m.codcompl2, 0) = 1015)
	        OR (m.codfase = 861 AND m.codcompl1 IN (690, 50068))
	        OR (m.codfase = 861 AND m.codcompl1 IN (359, 360, 361, 362, 363, 445, 50069)
	            AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
	        OR (m.codfase = 50002 AND m.codcompl1 IN (306, 12065))
	    	)
    ) x where rn =  1
), mov_reativacao AS (
    SELECT
        m.coddoc,
        m.dthrmov,
        m.codfase,
        m.codcompl1,
        m.codcompl2,
        m.codcompl3
    from ACERVO A 
    join EJUD.MOVIMENTO M on A.CODDOC = M.CODDOC
    WHERE m.dthrmov <= A.DTH_REFERENCIA
    AND (
        (m.codfase = 50002 AND m.codcompl1 = 12066)
        OR m.codfase IN (893, 24, 193)
        )
), mov_reperussao AS (
	select * from (
		SELECT
    		m.coddoc,
    		m.dthrmov,
    		m.codfase,
    		m.codcompl1,
    		m.codcompl2,
    		m.codcompl3,
    	case
    		when ms.dthrterminosobre >= a.dth_referencia then null
    		else ms.dthrterminosobre
    	end as dthrterminosobre
		FROM acervo a
		JOIN ejud.movimento m ON a.coddoc = m.coddoc
		JOIN ejud.movsobrestamento ms ON ms.coddoc = m.coddoc AND ms.dthrmov = m.dthrmov
		WHERE m.dthrmov <= a.dth_referencia
	  	AND m.codfase = 50040
	) x where dthrterminosobre IS NULL
), mov_suspensao_repercussao AS (
	    SELECT
	        x.coddoc,
	        MIN(x.dthrmov) AS datasusp,
	        MIN(x.codfase) as codfase_susp,
	        min(x.codcompl1) as codcompl1_susp,
	        min(x.codcompl2) as codcompl2_susp,
	        min(x.codcompl3) as codcompl3_susp,
	        MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END) AS datarepercussao,
	        MIN(CASE WHEN x.src = 'R' THEN x.codfase END) AS codfase_reper,
	        MIN(CASE WHEN x.src = 'R' THEN x.codcompl1 END) AS codcompl1_reper,
	        MIN(CASE WHEN x.src = 'R' THEN x.codcompl2 END) AS codcompl2_reper,
	        MIN(CASE WHEN x.src = 'R' THEN x.codcompl3 END) AS codcompl3_reper
	    FROM (
	        SELECT s.coddoc, s.dthrmov, s.codfase, s.codcompl1, s.codcompl2, s.codcompl3, 'S' AS src 
	        	FROM mov_suspensao s LEFT JOIN mov_reativacao r
	            ON r.coddoc = s.coddoc AND r.dthrmov > s.dthrmov   -- há reativação após a suspensão?
	        	WHERE r.coddoc IS NULL        -- se sim, esse s não vale como “vigente”
	        UNION ALL
	        SELECT r.coddoc, r.dthrmov, r.codfase, r.codcompl1, r.codcompl2, r.codcompl3, 'R' AS src 
	        	FROM mov_reperussao r
	    ) x
	GROUP BY x.coddoc
), mov_julgamento AS (
    select * from (
		SELECT
	        m.coddoc,
	        m.dthrmov,
	        m.codfase,
	        m.codcompl1,
	        m.codcompl2,
	        m.codcompl3,
	        ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	    from ACERVO A 
	    join EJUD.MOVIMENTO M on A.CODDOC = M.CODDOC
	    WHERE m.dthrmov <= A.DTH_REFERENCIA
	    AND m.codfase in (193, 24) 
    ) x WHERE rn = 1
), mov_localfisico AS (
    select * from (
		select mf.coddoc, mf.dthrmov, mf.codlocal, ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) rn 
	    from acervo a
	    join ejud.movimentofisico mf on a.coddoc = mf.coddoc and A.DTH_REFERENCIA <= MF.DTHRMOV
    ) x where rn = 1
), mov_localvirtual AS (
	select * from (
	    select lvd.coddoc, lvd.dthrentr, lvd.codlocalvirt, lvd.codlocal, ROW_NUMBER() OVER (PARTITION BY lvd.coddoc ORDER BY lvd.dthrentr DESC) rn
	    from acervo a
	    join ejud.localvirtualdocumento lvd on a.coddoc = lvd.coddoc and A.DTH_REFERENCIA <= lvd.dthrentr
	) x where rn = 1
), mov_conclusao as (
    select * from (
		SELECT
	        m.coddoc,
	        m.dthrmov,
	        m.codfase,
	        m.codcompl1,
	        m.codcompl2,
	        m.codcompl3,
	        ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	    from ACERVO A 
	    join EJUD.MOVIMENTO M on A.CODDOC = M.CODDOC
	    WHERE m.dthrmov <= A.DTH_REFERENCIA
	    AND m.codfase = 51 
    ) x WHERE rn = 1
), mov_julgamento_monocratico as (
	SELECT *
	    FROM (
	        SELECT 
	            m.coddoc,
	            m.dthrmov,
	            m.codfase,
	            m.codcompl1,
	            m.codcompl2,
	            m.codcompl3,
	            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	        FROM acervo a
	    join ejud.movimento m on a.coddoc = m.coddoc and m.dthrmov <= a.dth_referencia
	    where m.codfase = 24
	    ) t
    WHERE rn = 1
), mov_julgamento_colegiado as (
	SELECT *
	    FROM (
	    	SELECT 
	            m.coddoc,
	            m.dthrmov,
	            m.codfase,
	            m.codcompl1,
	            m.codcompl2,
	            m.codcompl3,
	            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	        FROM acervo a
	    join ejud.movimento m on a.coddoc = m.coddoc and m.dthrmov <= a.dth_referencia
	    where m.codfase = 193
	    ) X WHERE rn = 1
), mov_movimento as (
    select * from (
		select 
	        M.coddoc, 
	        M.dthrmov, 
	        M.codfase,
	        M.codcompl1,
	        M.codcompl2,
	        M.codcompl3,
	        ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	    from ACERVO A
	    join ejud.movimento M on A.CODDOC = M.CODDOC and M.DTHRMOV < A.DTH_REFERENCIA
	) X where rn = 1
--), cod_magistrado_cargo_primeira_distribuicao as
), mov_distribuicao_primeira_cargo as (
	select * from (
	select  
		mdpv.codmagrel, 
		CASE 
			WHEN lm.codcargo = 3 and lm.codjustorig = 2 THEN 9
			WHEN lm.codcargo = 2 and lm.codjustorig = 2 THEN 2
		else lm.codcargo END as CODCARGO,
		ROW_NUMBER() OVER (PARTITION BY LM.codmag ORDER BY lm.dthralt DESC) rn
	from mov_distribuicao_primeira_valida mdpv
	left join ejud.logmagistrado lm on mdpv.codmagrel = lm.codmag and LM.CODCARGO is not null and LM.CODJUSTORIG is not null and LM.TIPOPER != 'E'
	where lm.dthralt <= mdpv.dthrmov
	) X where RN = 1
), mov_distribuicao_ultima_cargo as (
	select * from (
		select 
			A.COD_ULTIMO_RELATOR,
			CASE 
				WHEN lm.codcargo = 3 and lm.codjustorig = 2 THEN 9
				WHEN lm.codcargo = 2 and lm.codjustorig = 2 THEN 2
			else lm.codcargo END as CODCARGO,
			ROW_NUMBER() OVER (PARTITION BY LM.codmag ORDER BY lm.dthralt DESC) rn
		from acervo a
		join ejud.logmagistrado lm on A.COD_ULTIMO_RELATOR = lm.codmag and LM.CODCARGO is not null and LM.CODJUSTORIG is not NULL and LM.TIPOPER != 'E'
		where lm.dthralt <= A.DTH_ULTIMA_DISTRIBUICAO 
	) X where RN = 1	
)
SELECT
	A.DTH_REFERENCIA,
	A.CODDOC,
	--INDICADORES DA ULTIMA DISTRIBUICAO
	A.DTH_ULTIMA_DISTRIBUICAO as DTH_DISTRIBUICAO_ULTIMA,
	A.COD_ULTIMO_RELATOR as COD_MAGISTRADO_RELATOR_ULTIMO,
	A.COD_ULTIMO_ORGAO_JULGADOR as COD_ORGAO_JULGADOR_ULTIMO,
	mduc.codcargo as COD_CARGO_ULTIMO_RELATOR,
	--INDICADORES DA PRIMEIRA DISTRIBUICAO
	mdpv.dthrmov as DTH_DISTRIBUICAO_PRIMEIRA, 
	mdpv.codmagrel as COD_MAGISTRADO_RELATOR_PRIMEIRO, 
	mdpv.codorgjulg as COD_ORGAO_JULGADOR_PRIMEIRO, 
	mdpc.codcargo as COD_CARGO_PRIMEIRO_RELATOR, 
	--INDICADORES DE ULTIMO MOVIMENTO
    M.DTHRMOV AS DTH_MOVIMENTO_ULTIMO,
    M.CODFASE AS COD_FASE_ULTIMO,
    M.CODCOMPL1 as COD_COMPLEMENTO1_ULTIMO,
    M.CODCOMPL2 as COD_COMPLEMENTO2_ULTIMO,
    M.CODCOMPL3 as COD_COMPLEMENTO3_ULTIMO,
	--INDICADORES DE SUSPENSAO
    MSR.datasusp as DTH_SUSPENSAO,
	MSR.CODFASE_SUSP as COD_FASE_SUSPENSAO,
	MSR.CODCOMPL1_SUSP as COD_COMPLEMENTO1_SUSPENSAO,
	MSR.CODCOMPL2_SUSP as COD_COMPLEMENTO2_SUSPENSAO,
	MSR.CODCOMPL3_SUSP as COD_COMPLEMENTO3_SUSPENSAO,
	CASE WHEN MSR.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
	--INDICADORES DE SOBRESTAMENTO
	MSR.datarepercussao as DTH_REPERCUSSAO,
	MSR.CODFASE_REPER as COD_FASE_REPERCUSSAO,
	MSR.CODCOMPL1_REPER as COD_COMPLEMENTO1_REPERCUSSAO,
	MSR.CODCOMPL2_REPER as COD_COMPLEMENTO2_REPERCUSSAO,
	MSR.CODCOMPL3_REPER as COD_COMPLEMENTO3_REPERCUSSAO,
	CASE WHEN MSR.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS IND_SOBRESTAMENTO,
	--INDICADORES DE JULGAMENTO
	MJ.DTHRMOV as DTH_JULGAMENTO,
	MJ.CODFASE as COD_FASE_JULGAMENTO,
	MJ.CODCOMPL1 as COD_COMPLEMENTO1_JULGAMENTO,
	MJ.CODCOMPL2 as COD_COMPLEMENTO2_JULGAMENTO,
	MJ.CODCOMPL3 as COD_COMPLEMENTO3_JULGAMENTO,
	CASE WHEN MJ.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento,
	--INDICADORES DE LOCAL
    mlf.dthrmov as DTH_ULTIMO_LOCAL,
	mlf.codlocal as COD_ULTIMO_LOCAL,
	--INDICADORES DE LOCAL VIRTUAL
	mlv.dthrentr as DTH_ULTIMO_LOCAL_VIRTUAL,
	mLV.CODLOCALVIRT as COD_ULTIMO_LOCAL_VIRTUAL,
	--INDICADORES DE CONCLUSAO
	MC.DTHRMOV as DTH_ULTIMA_CONCLUSAO,
	MC.CODFASE as COD_FASE_CONCLUSAO,
	MC.CODCOMPL1 as COD_COMPLEMENTO1_ULTIMA_CONCLUSAO,
	MC.CODCOMPL2 as COD_COMPLEMENTO2_ULTIMA_CONCLUSAO,
	MC.CODCOMPL3 as COD_COMPLEMENTO3_ULTIMA_CONCLUSAO,
--	CASE WHEN MC.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS IND_CONCLUSAO, REMOVIDO POIS NÃO NECESSARIAMENTE É CONCLUSAO
	--INDICADORES DE JULGAMENTO MONOCRATICO
	MJM.DTHRMOV as DTH_ULTIMA_JULGAMENTO_MONOCRATICO,
	MJM.CODFASE as COD_FASE_JULGAMENTO_MONOCRATICO,
	MJM.CODCOMPL1 as COD_COMPLEMENTO1_JULGAMENTO_MONOCRATICO,
	MJM.CODCOMPL2 as COD_COMPLEMENTO2_JULGAMENTO_MONOCRATICO,
	MJM.CODCOMPL3 as COD_COMPLEMENTO3_JULGAMENTO_MONOCRATICO,
	CASE WHEN MJM.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS IND_JULGAMENTO_MONOCRATICO,
	--INDICADORES DE JULGAMENTO EM COLEGIADO
	MJC.DTHRMOV as DTH_ULTIMA_JULGAMENTO_COLEGIADO,
	MJC.CODFASE as COD_FASE_JULGAMENTO_COLEGIADO,
	MJC.CODCOMPL1 as COD_COMPLEMENTO1_JULGAMENTO_COLEGIADO,
	MJC.CODCOMPL2 as COD_COMPLEMENTO2_JULGAMENTO_COLEGIADO,
	MJC.CODCOMPL3 as COD_COMPLEMENTO3_JULGAMENTO_COLEGIADO,
	CASE WHEN MJC.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS IND_JULGAMENTO_COLEGIADO
from ACERVO A
left join mov_movimento M on A.coddoc = M.CODDOC
left join mov_suspensao_repercussao MSR on A.coddoc = MSR.CODDOC
left join mov_julgamento MJ on A.coddoc = MJ.CODDOC
left join mov_localfisico mlf on A.CODDOC = MLF.CODDOC
left join mov_localvirtual mlv on A.CODDOC = MLV.CODDOC
left join mov_conclusao MC on A.CODDOC =  MC.CODDOC
left join mov_julgamento_monocratico mjm on A.CODDOC = MJM.CODDOC
left join mov_julgamento_colegiado MJC on A.CODDOC = MJC.CODDOC
left join mov_distribuicao_primeira_valida MDPV on A.CODDOC = MDPV.CODDOC
left join mov_distribuicao_primeira_cargo mdpc on MDPV.codmagrel = mdpc.codmagrel
left join mov_distribuicao_ultima_cargo mduc on a.COD_ULTIMO_RELATOR = mduc.COD_ULTIMO_RELATOR;