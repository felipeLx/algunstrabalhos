
 WITH GABINETE AS (
        SELECT MAG.CODMAG, LF.CODLOCAL, LF.DESCR as LOCALFISICO
        FROM localfisico LF
        JOIN magistrado MAG
            ON LF.CODMAG = MAG.CODMAG
        --1VP, 2VP e 3VP não são gabinetes mas possuem mais de um magistrado
        WHERE LF.CODLOCAL NOT IN (3142, 3144, 3148)
    ),
    SECRETARIA AS (
        SELECT LF.CODLOCAL, LF.DESCR as LOCALFISICO
        FROM localfisico LF
        join orgaojulgador OJ on LF.CODLOCAL = OJ.CODLOCAL
    ),
    LOCALFISICO_BASE AS (
        SELECT
            CODLOCAL,
            replace(replace(replace(REPLACE(UPPER(COALESCE(DESCR)) , '3AVP', '3VP'),'3ª VP', '3VP'),'2AVP','2VP'),'2ª VP', '2VP') AS NOME,
            INDLOCALEXT  
        FROM ejud.localfisico
    )
    SELECT
        LF.CODLOCAL AS COD_LOCAL,
        LF.NOME,
        CASE
            WHEN G.CODLOCAL IS NOT NULL THEN 'S'
            ELSE 'N'
        END AS INDGABINETE,
        CASE
            WHEN S.CODLOCAL IS NOT NULL THEN 'S'
            ELSE 'N'
        END AS INDSECRETARIA,
        LF.INDLOCALEXT
    FROM LOCALFISICO_BASE LF
    LEFT JOIN GABINETE G ON LF.CODLOCAL = G.CODLOCAL
    LEFT JOIN SECRETARIA S ON LF.CODLOCAL = S.CODLOCAL    
