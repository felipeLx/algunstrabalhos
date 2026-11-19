SELECT *
FROM (
    SELECT 
	    m.coddoc,
	    m.dthrmov,
	    m.codfase,
	    m.codcompl1,
	    m.codcompl2,
	    m.codcompl3,
	    m.numprazo,
	    m.dthrincl,
	    m.usuincl,
	    m.numfolha,
	    m.descrmotivalt,
	    m.obs,
        ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
    from stg2inst.ejud_acervo a
LEFT JOIN ejud.movimento m ON m.coddoc = a.cod_documento and m.dthrmov <= a.dth_referencia
) t
WHERE rn = 1