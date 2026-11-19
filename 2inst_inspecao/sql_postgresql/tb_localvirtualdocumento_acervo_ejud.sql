    SELECT
        t.coddoc,
        t.dthrentr,
        t.codlocalvirt,
        t.codlocal
    FROM (
        SELECT
            flv.coddoc,
            flv.dthrentr,
            flv.codlocalvirt,
            flv.codlocal,
            ROW_NUMBER() OVER (PARTITION BY flv.coddoc ORDER BY flv.dthrentr DESC) AS rn
        FROM stg2inst.ejud_acervo a
        JOIN ejud.localvirtualdocumento flv ON a.cod_documento = flv.coddoc and flv.dthrentr <= a.dth_referencia
    ) t
    WHERE t.rn = 1