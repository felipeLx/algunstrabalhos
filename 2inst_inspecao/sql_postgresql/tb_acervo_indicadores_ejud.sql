with ACERVO as ( 
	select  
		DTH_REFERENCIA,
		COD_DOCUMENTO,
		DTH_ULTIMA_DISTRIBUICAO,
		COD_ULTIMO_RELATOR,
		COD_ULTIMO_ORGAO_JULGADOR
	from stg2inst.tb_acervo_ejud
), SUSPENSAO AS (
    SELECT
        m.coddoc,
        m.dthrmov,
        m.codfase,
        m.codcompl1,
        m.codcompl2,
        m.codcompl3
    FROM stg2inst.tb_movimento_suspensao_ejud m
    JOIN acervo a ON a.COD_DOCUMENTO = m.coddoc
    WHERE m.dthrmov <= a.DTH_REFERENCIA
      AND (
          (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
          OR (m.codfase IN (3, 20) AND m.codcompl1 = 25)
          OR (m.codfase IN (3, 20, 29) AND m.codcompl1 = 1013 AND COALESCE(m.codcompl2, 0) = 1015)
          OR (m.codfase = 861 AND m.codcompl1 IN (690, 50068))
          OR (m.codfase = 861 AND m.codcompl1 IN (359, 360, 361, 362, 363, 445, 50069)
              AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
          OR (m.codfase = 50002 AND m.codcompl1 IN (306, 12065))
      )
), REATIVACAO AS (
	    -- Eventos que "quebram" a suspensão; não filtra por data fixa nem coddoc específico
	    SELECT
	        m.coddoc,
	        m.dthrmov
	    FROM stg2inst.tb_movimento_suspensao_ejud m
	    JOIN acervo a	      ON a.COD_DOCUMENTO = m.coddoc
	    WHERE m.dthrmov <= a.DTH_REFERENCIA
	      AND (
	          (m.codfase = 50002 AND m.codcompl1 = 12066)
	          OR m.codfase IN (893, 24, 193)
	      )
	), REPERCUSSAO AS (
	    -- Sobrestamento (com dthrterminosobre disponível)
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
	FROM stg2inst.tb_movimento_suspensao_ejud m
	JOIN acervo a	      ON a.COD_DOCUMENTO = m.coddoc
	JOIN ejud.movsobrestamento ms	      ON ms.coddoc = m.coddoc	     AND ms.dthrmov = m.dthrmov
	WHERE m.dthrmov <= a.DTH_REFERENCIA
		) x where dthrterminosobre IS NULL
	), mov_suspensao AS (
	    -- “Ponto” mais recente de suspensão que NÃO foi reativado depois + ponto de repercussão
	    SELECT
	        x.coddoc,
	        MIN(x.dthrmov) AS datasusp,
	        MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END) AS datarepercussao
	    FROM (
	        SELECT s.coddoc, s.dthrmov, 'S' AS src
	          FROM SUSPENSAO s
	          LEFT JOIN REATIVACAO r
	            ON r.coddoc = s.coddoc
	           AND r.dthrmov > s.dthrmov   -- há reativação após a suspensão?
	         WHERE r.coddoc IS NULL        -- se sim, esse s não vale como “vigente”
	        UNION ALL
	        SELECT r.coddoc, r.dthrmov, 'R' AS src
	          FROM REPERCUSSAO r
	    ) x
	    GROUP BY x.coddoc
	), ultimo_julgamento AS (
    SELECT *
    FROM (
        SELECT 
            m.coddoc,
            m.dthrmov,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
        FROM acervo a
    join stg2inst.tb_movimento_julgamento_ejud m on a.cod_documento = m.coddoc and m.dthrmov <= a.dth_referencia
    ) t
    WHERE rn = 1
), mov_localfisico AS (
    select coddoc, dthrmov, codlocal from stg2inst.tb_movimentofisico_acervo_ejud
), mov_localvirtual AS (
    select coddoc, dthrentr, codlocalvirt, codlocal from stg2inst.tb_localvirtualdocumento_acervo_ejud
), mov_conclusao as (
	SELECT CODDOC, DTHRMOV, RN
	    FROM (
	        SELECT 
	            m.coddoc,
	            m.dthrmov,
	            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	        FROM acervo a
	    join stg2inst.tb_movimento_conclusao_ejud m on a.cod_documento = m.coddoc and m.dthrmov <= a.dth_referencia
	    ) t
    WHERE rn = 1
), mov_julgamento_monocratico as (
	SELECT *
	    FROM (
	        SELECT 
	            m.coddoc,
	            m.dthrmov,
	            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	        FROM acervo a
	    join stg2inst.tb_movimento_julgamento_ejud m on a.cod_documento = m.coddoc and m.dthrmov <= a.dth_referencia
	    where m.codfase = 24
	    ) t
    WHERE rn = 1
), mov_julgamento_colegiado as (
	SELECT *
	    FROM (
	        SELECT 
	            m.coddoc,
	            m.dthrmov,
	            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
	        FROM acervo a
	    join stg2inst.tb_movimento_julgamento_ejud m on a.cod_documento = m.coddoc and m.dthrmov <= a.dth_referencia
	    where m.codfase = 193
	    ) t
    WHERE rn = 1
), mov as (
    select 
        coddoc, 
        dthrmov, 
        codfase
    from stg2inst.tb_movimento_acervo_ejud
)
select 
	A.DTH_REFERENCIA,
	A.COD_DOCUMENTO,
	A.DTH_ULTIMA_DISTRIBUICAO,
	A.COD_ULTIMO_RELATOR,
	A.COD_ULTIMO_ORGAO_JULGADOR,
    M.DTHRMOV AS DTH_ULTIMO_MOVIMENTO,
    M.CODFASE AS COD_FASE,
	MS.datasusp as DTH_SUSPENSAO,
	CASE WHEN MS.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
	MS.datarepercussao as DTH_SOBRESTAMENTO,
	CASE WHEN MS.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS IND_SOBRESTAMENTO,
	UJ.DTHRMOV as DTH_JULGAMENTO,
	CASE WHEN UJ.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento,
    mlf.dthrmov as DTH_ULTIMO_LOCAL,
	mlf.codlocal as COD_ULTIMO_LOCAL,
	mlv.dthrentr as DTH_ULTIMO_LOCAL_VIRTUAL,
	mLV.CODLOCALVIRT as COD_ULTIMO_LOCAL_VIRTUAL,
	MC.DTHRMOV as DTH_ULTIMA_CONCLUSAO,
	CASE WHEN MC.DTHRMOV IS NOT NULL THEN 'S' ELSE 'N' END AS IND_CONCLUSAO,
	MJM.DTHRMOV as DTH_ULTIMO_JULG_MONOCRATICO,
	MJC.DTHRMOV as DTH_ULTIMO_JULG_COLEGIADO
from ACERVO A
left join mov M on A.COD_DOCUMENTO = M.CODDOC
left join mov_suspensao MS on A.COD_DOCUMENTO = MS.CODDOC
left join ultimo_julgamento UJ on A.COD_DOCUMENTO = UJ.CODDOC
left join mov_localfisico mlf on A.COD_DOCUMENTO = mLF.CODDOC
left join mov_localvirtual mlv on A.COD_DOCUMENTO = mLV.CODDOC
left join mov_conclusao MC on A.COD_DOCUMENTO =  MC.CODDOC
left join mov_julgamento_monocratico mjm on A.COD_DOCUMENTO = MJM.CODDOC
left join mov_julgamento_colegiado MJC on A.COD_DOCUMENTO = MJC.CODDOC;