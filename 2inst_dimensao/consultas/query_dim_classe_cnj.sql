WITH ejud_classe_cnj AS (
	select * from (
	SELECT 
		Codextcnj as codclassecnj,
		descr as classe,
		row_number() over (partition by Codextcnj order by codtipproc asc) rn
	FROM ejud.tipoprocesso where codextcnj is not null) x where rn = 1
),
eproc2g_classe_cnj AS (
    SELECT 
        cod_mni_classe_judicial AS codclassecnj, 
        des_mni_classe_judicial AS classe
    FROM eproc2g.mni_classe_judicial
    WHERE cod_mni_classe_judicial IS NOT NULL
), classecnj AS (
    SELECT 
        cast(COALESCE(ep.codclassecnj, ej.codclassecnj) as int) AS codclassecnj,
        UPPER(COALESCE(ep.classe, ej.classe)) AS classe,
        CASE WHEN ej.codclassecnj IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud,
        CASE WHEN ep.codclassecnj IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc2g
    FROM ejud_classe_cnj ej
    FULL OUTER JOIN eproc2g_classe_cnj ep 
        ON ej.codclassecnj = ep.codclassecnj
)
SELECT * FROM classecnj
UNION ALL
SELECT 
    0 AS codclassecnj,
    'NÃO ENCONTRADO' AS classe,
    'N' AS ind_ejud,
    'N' AS ind_eproc2g