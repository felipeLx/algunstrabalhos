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
    m.obs
FROM ejud.movimento m
WHERE m.dthrmov <= '{v_dateref_str}' 
    AND m.codfase in (193, 24)


