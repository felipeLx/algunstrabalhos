WITH MOV_JULGMONOCRATICO AS (
	SELECT md.CodDoc, md.DtHrMov, md.CodMag, f.codfase, fe.codfaseext
	FROM ejud.MovDespa md
	JOIN ejud.MovimentoRelator mr
	  ON mr.CodDoc = md.CodDoc
	 AND mr.DtHrMov = (
	        SELECT MAX(mr2.DtHrMov)
	        FROM ejud.MovimentoRelator mr2
	        WHERE mr2.CodDoc = md.CodDoc
	          AND mr2.DtHrMov <= md.DtHrMov
	     )
	left join ejud.fase f on md.codfase = f.codfase
	left join ejud.faseexterna fe on f.codfase = fe.codfase
	WHERE md.CodFase IN (3, 24)
	AND MD.DTHRMOV <= current_timestamp	
    AND MD.DTHRMOV > date_trunc('day', current_timestamp)- INTERVAL '1 second' - INTERVAL '12 months' 
)
SELECT 
    current_timestamp AS dth_referencia,
    MJ.CODDOC as cod_documento, 
    'ITEM_11_2' AS id_item,
    MJ.CodMag as cod_magistrado,
    MJ.codfase as cod_movimento_interno,
    MJ.codfaseext as cod_movimento_externo,
    MJ.dthrmov as dth_movimento,
    'EJUD' AS ind_sistema
FROM MOV_JULGMONOCRATICO MJ
WHERE extract(year from MJ.DTHRMOV) = 2025 and extract(month from mj.dthrmov) = 8
and mj.coddoc in (23973671, 23995105, 24066081)