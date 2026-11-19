SELECT 
    COALESCE(ej.codlocal, ep.codlocal) AS codlocal,
    replace(replace(replace(REPLACE(UPPER(COALESCE(ej.localfisico, ep.localfisico)) , '3AVP', '3VP'),'3ª VP', '3VP'),'2AVP','2VP'),'2ª VP', '2VP') AS localfisico,
    CASE WHEN ej.codlocal IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud,
    CASE WHEN ep.codlocal IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc2g
FROM (
    SELECT 
        codlocal, 
        descr AS localfisico
    FROM ejud.localfisico
    where codlocal != 9999
) ej
FULL OUTER JOIN (
    SELECT 
        id_orgao AS codlocal, 
        des_orgao AS localfisico
    FROM eproc2g.orgao
) ep
ON ej.codlocal = ep.codlocal
UNION ALL
SELECT 
    0 AS codlocal,
    'NÃO ENCONTRADO' AS localfisico,
    'N' AS ind_ejud,
    'N' AS ind_eproc2g