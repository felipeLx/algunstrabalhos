with ind_processo_eproc2g AS (
	SELECT *
	FROM (
		SELECT 
			p.id_processo,
			dc.nome AS nome_campo,
			dcv.dado_comp_valor,
			pdc.id_dado_comp_valor,
			dcv.sin_exibe_rotulo,
			ROW_NUMBER() OVER (PARTITION BY p.id_processo, dc.nome ORDER BY pdc.dth_inclusao DESC) AS rn
		FROM eproc2g.processo p
		left JOIN eproc2g.processo_dado_complementar pdc
			ON p.id_processo = pdc.id_processo
		left JOIN eproc2g.dado_complementar_valor dcv
			ON pdc.id_dado_complementar = dcv.id_dado_complementar
			AND pdc.id_dado_comp_valor = dcv.id_dado_comp_valor
		left JOIN eproc2g.dado_complementar dc
			ON dcv.id_dado_complementar = dc.id_dado_complementar
		WHERE dc.nome IN ('IDOSO','IDOSO_MAIOR_80','DOENCA_GRAVE','PESSOA_COM_DEFICIENCIA')
	) x
	WHERE rn = 1
), ind_processo_eproc2g_tratado as (
	SELECT
	    u.id_processo,
	    MAX(CASE WHEN u.nome_campo = 'IDOSO' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS ind_prioridade_idoso,
	    MAX(CASE WHEN u.nome_campo = 'IDOSO_MAIOR_80' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS ind_prioridade_idoso_80,
	    MAX(CASE WHEN u.nome_campo = 'DOENCA_GRAVE' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS ind_doenca_grave,
	    MAX(CASE WHEN u.nome_campo = 'PESSOA_COM_DEFICIENCIA' AND u.rn = 1 AND u.sin_exibe_rotulo = 'S' THEN 'S' ELSE 'N' END) AS ind_prioridade_deficiente   
	FROM ind_processo_eproc2g u
	WHERE u.rn = 1
	GROUP BY u.id_processo
), dim_processo_eproc2g as (
	select 
		p.id_processo as cod_documento,
		lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 1  FOR 7), 7, '0') || '-' ||
	    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 8  FOR 2), 2, '0') || '.' ||
	    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 10 FOR 4), 4, '0') || '.' ||
	    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 14 FOR 1), 1, '0') || '.' ||
	    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 15 FOR 2), 2, '0') || '.' ||
	    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 17 FOR 4), 4, '0') AS cod_cnj,
	    case
	    	when id_sigilo != 5 then lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 1  FOR 7), 7, '0') || '-' ||
		    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 8  FOR 2), 2, '0') || '.' ||
		    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 10 FOR 4), 4, '0') || '.' ||
		    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 14 FOR 1), 1, '0') || '.' ||
		    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 15 FOR 2), 2, '0') || '.' ||
		    lpad(substring(lpad(p.num_processo::Text, 20, '0') FROM 17 FOR 4), 4, '0') 
		    when id_sigilo = 5 then 'RESTRITO'
	    end AS cod_cnj_sigilo,
	    p.id_sigilo as id_sigilo,
	    p.id_classe_judicial as cod_classe_interna,
	    cj.cod_classe as cod_classe_externa,
	    rcjm.cod_mni_classe_judicial as cod_classe_cnj,
	    cj.des_classe as nm_classe,
	    'X' as ind_originario,
	    pet.ind_prioridade_idoso as ind_prioridade_idoso,
	    pet.ind_prioridade_idoso_80 as ind_prioridade_idoso_80,
	    pet.ind_doenca_grave as ind_doenca_grave,
	    'X' as ind_crime_adm_publico,
	    pet.ind_prioridade_deficiente as ind_prioridade_deficiente,
		'X' as ind_menor,
		'X' as ind_menor_recolhido,
	    'EPROC2G' as ind_sistema,
		p.dth_autuacao
	from eproc2g.processo p
	join eproc2g.classe_judicial cj on p.id_classe_judicial = cj.id_classe_judicial
	join eproc2g.competencia_judicial c on p.cod_competencia = c.cod_competencia
	join eproc2g.relacao_classe_judicial_mni rcjm on p.id_classe_judicial = rcjm.id_classe_judicial 
	join ind_processo_eproc2g_tratado pet on p.id_processo = pet.id_processo
	and (c.sin_criminal = rcjm.sin_competencia_criminal or c.sin_civel = rcjm.sin_competencia_civel)
	and rcjm.sin_ativo = 'S'
	where p.dth_autuacao <= '{v_dateref_str}' 
)
SELECT 
    CAST(COD_DOCUMENTO AS NUMERIC(30,0)) AS COD_DOCUMENTO,
    CAST(COD_CNJ AS VARCHAR(30)) AS COD_CNJ,
    CAST(COD_CNJ_SIGILO AS VARCHAR(30)) AS COD_CNJ_SIGILO,
    CAST(ID_SIGILO AS INTEGER) AS ID_SIGILO,
    CAST(COD_CLASSE_INTERNA AS INTEGER) AS COD_CLASSE_INTERNA,
    CAST(COD_CLASSE_EXTERNA AS INTEGER) AS COD_CLASSE_EXTERNA,
    CAST(COD_CLASSE_CNJ AS INTEGER) AS COD_CLASSE_CNJ,
    CAST(NM_CLASSE AS VARCHAR(100)) AS NM_CLASSE,
    CAST(IND_ORIGINARIO AS CHAR(1)) AS IND_ORIGINARIO,
    CAST(IND_PRIORIDADE_IDOSO AS CHAR(1)) AS IND_PRIORIDADE_IDOSO,
    CAST(IND_PRIORIDADE_IDOSO_80 AS CHAR(1)) AS IND_PRIORIDADE_IDOSO_80,
    CAST(IND_DOENCA_GRAVE AS CHAR(1)) AS IND_DOENCA_GRAVE,
    CAST(IND_PRIORIDADE_DEFICIENTE AS CHAR(1)) AS IND_PRIORIDADE_DEFICIENTE,
	CAST(IND_MENOR AS CHAR(1)) AS IND_MENOR,
    CAST(IND_MENOR_RECOLHIDO AS CHAR(1)) AS IND_MENOR_RECOLHIDO,
    CAST(DTH_AUTUACAO AS TIMESTAMP) AS DTH_AUTUACAO,
    CAST(IND_CRIME_ADM_PUBLICO AS CHAR(1)) AS IND_CRIME_ADM_PUBLICO,
    CAST(IND_SISTEMA AS VARCHAR(7)) AS IND_SISTEMA
from dim_processo_eproc2g