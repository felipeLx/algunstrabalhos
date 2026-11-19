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
    AND (
        (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
        OR (m.codfase IN (3, 20) AND m.codcompl1 = 25)
        OR (m.codfase IN (3, 20, 29) AND m.codcompl1 = 1013 AND COALESCE(m.codcompl2, 0) = 1015)
        OR (m.codfase = 861 AND m.codcompl1 IN (690, 50068))
        OR (m.codfase = 861 AND m.codcompl1 IN (359, 360, 361, 362, 363, 445, 50069)
            AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
        OR (m.codfase = 50002 AND m.codcompl1 IN (306, 12065))
    )