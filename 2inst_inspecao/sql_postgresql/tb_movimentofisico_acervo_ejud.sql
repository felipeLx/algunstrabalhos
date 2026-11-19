SELECT coddoc, dthrmov, codlocal
FROM (
    SELECT 
        mf.coddoc,
        mf.dthrmov,
        mf.codlocal,
        ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) rn
    from stg2inst.ejud_acervo a
LEFT JOIN ejud.movimentofisico mf ON mf.coddoc = a.cod_documento and mf.dthrmov <= a.dth_referencia
) t
WHERE rn = 1