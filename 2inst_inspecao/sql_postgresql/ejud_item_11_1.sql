WITH MOV_DISTRIBUICAO AS (
    SELECT CODDOC, DTHRMOV, CODFASE 
    FROM ejud.movdistribuicao
    WHERE CODFASE IN (26,36)
    AND DTHRMOV <= current_timestamp 
    AND DTHRMOV > date_trunc('day', current_timestamp)- INTERVAL '1 second' - INTERVAL '12 months'
), MOV_RELATOR AS (
    SELECT 
        CODDOC,
        DTHRMOV,
        CODORGJULG,
        CODMAGREL
    FROM ejud.movimentorelator
    WHERE DTHRMOV <= current_timestamp	
    AND DTHRMOV > date_trunc('day', current_timestamp)- INTERVAL '1 second' - INTERVAL '12 months'
) 
SELECT 
    current_timestamp AS dth_referencia,
    MOV_RELATOR.CODDOC as cod_documento, 
    'ITEM_11_1' as id_item,
    MOV_RELATOR.CODMAGREL AS cod_magistrado,
    MOV_DISTRIBUICAO.CODFASE as cod_movimento,
    MOV_DISTRIBUICAO.DTHRMOV as dth_movimento,
    'EJUD' AS IND_SISTEMA
FROM MOV_DISTRIBUICAO 
JOIN  MOV_RELATOR ON MOV_RELATOR.CODDOC = MOV_DISTRIBUICAO.CODDOC 
AND MOV_RELATOR.DTHRMOV = MOV_DISTRIBUICAO.DTHRMOV;