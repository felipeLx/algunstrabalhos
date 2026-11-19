select * from (
with GABINETE_EJUD AS (
  SELECT
    MAG.CODMAG,
    LF.CODLOCAL,
    LF.DESCR AS LOCALFISICO
  FROM ejud.localfisico LF
  JOIN ejud.magistrado MAG
    ON LF.CODMAG = MAG.CODMAG
  -- 1VP, 2VP e 3VP não são gabinetes mas possuem mais de um magistrado
  WHERE LF.CODLOCAL NOT IN (3142, 3144, 3148)
), SECRETARIA_EJUD AS (
  SELECT DISTINCT
    LF.CODLOCAL,
    LF.DESCR AS LOCALFISICO
  FROM ejud.localfisico LF
  JOIN ejud.orgaojulgador OJ
    ON LF.CODLOCAL = OJ.CODLOCAL
), LOCALFISICO_EJUD AS (
  SELECT
    CODLOCAL,
    REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(COALESCE(DESCR, ''))),'3AVP','3VP'),'3ª VP','3VP'),'2AVP','2VP'),'2ª VP','2VP') AS LOCALFISICO,
    INDLOCALEXT
  FROM ejud.localfisico 
), DIM_LOCALFISICO_EJUD AS (
	SELECT
	  LF.CODLOCAL,
	  LF.LOCALFISICO,
	  CASE WHEN G.CODLOCAL IS NOT NULL THEN 'S' ELSE 'N' END AS IND_GABINETE,
	  CASE WHEN S.CODLOCAL IS NOT NULL THEN 'S' ELSE 'N' END AS IND_SECRETARIA,
	  LF.INDLOCALEXT as IND_LOCALEXTERNO
	FROM LOCALFISICO_EJUD LF
	LEFT JOIN GABINETE_EJUD G
	  ON LF.CODLOCAL = G.CODLOCAL
	LEFT JOIN LOCALFISICO_EJUD S
	  ON LF.CODLOCAL = S.CODLOCAL
), ORGAO_EPROC2G as (
	SELECT 
	    COALESCE(o2.id_orgao, o1.id_orgao) AS id_orgao,
	    COALESCE(o2.des_orgao, o1.des_orgao) AS des_orgao,
		case
			WHEN o1.id_orgao IS NOT NULL THEN 'S'
	        else 'N'
	    END AS ind_localexterno,
	    CAST(NULL AS varchar(255)) AS ind_gabinete,
	    CAST(NULL AS varchar(255)) AS ind_secretaria
	FROM eproc2g.orgao o2
	FULL OUTER JOIN eproc1g.orgao o1
	    ON o2.id_orgao = o1.id_orgao
)
select 
	COALESCE(LF.codlocal, o.id_orgao) as codlocal,
	coalesce(LF.localfisico, o.des_orgao) as localfisico,
	LF.ind_gabinete,
	LF.ind_secretaria,
	LF.ind_localexterno,
	CASE WHEN LF.codlocal IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud,
    CASE WHEN O.id_orgao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc2g,
    current_timestamp as DTH_ETL
from DIM_LOCALFISICO_EJUD LF
full outer JOIN ORGAO_EPROC2G O on LF.CODLOCAL = O.ID_ORGAO
UNION ALL
SELECT 
    0 AS codlocal,
    'NÃO ENCONTRADO' AS localfisico,
    'N' as ind_secretaria,
    'N' as ind_gabinete,
    'N' as ind_localexterno,
    'N' AS ind_ejud,
    'N' AS ind_eproc2g,
    current_timestamp as DTH_ETL
) where 1=1;