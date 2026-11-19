WITH base AS (

  SELECT
    CAST(codfasenivel2     AS BIGINT) AS EJUD,
    CAST(codfasenivel2cnj  AS BIGINT) AS CNJ,
    TRIM(descrfasenivel2)            AS DESCRICAO
  FROM glue_catalog.ejud.integracaofasecompl_ejud_cnj
  WHERE codfasenivel2 IS NOT NULL
    AND codfasenivel2cnj IS NOT NULL
    AND descrfasenivel2 IS NOT NULL
    AND TRIM(descrfasenivel2) <> ''

  UNION ALL

  SELECT
    CAST(codcompl1nivel3    AS BIGINT) AS EJUD,
    CAST(codcompl1nivel3cnj AS BIGINT) AS CNJ,
    TRIM(descrcompl1nivel3)           AS DESCRICAO
  FROM glue_catalog.ejud.integracaofasecompl_ejud_cnj
  WHERE codcompl1nivel3 IS NOT NULL
    AND codcompl1nivel3cnj IS NOT NULL
    AND descrcompl1nivel3 IS NOT NULL
    AND TRIM(descrcompl1nivel3) <> ''

  UNION ALL

  SELECT
    CAST(codcompl2nivel4    AS BIGINT) AS EJUD,
    CAST(codcompl2nivel4cnj AS BIGINT) AS CNJ,
    TRIM(descrcompl2nivel4)           AS DESCRICAO
  FROM glue_catalog.ejud.integracaofasecompl_ejud_cnj
  WHERE codcompl2nivel4 IS NOT NULL
    AND codcompl2nivel4cnj IS NOT NULL
    AND descrcompl2nivel4 IS NOT NULL
    AND TRIM(descrcompl2nivel4) <> ''

  UNION ALL

  SELECT
    CAST(codcompl3nivel5    AS BIGINT) AS EJUD,
    CAST(codcompl3nivel5cnj AS BIGINT) AS CNJ,
    TRIM(descrcompl3nivel5)           AS DESCRICAO
  FROM glue_catalog.ejud.integracaofasecompl_ejud_cnj
  WHERE codcompl3nivel5 IS NOT NULL
    AND codcompl3nivel5cnj IS NOT NULL
    AND descrcompl3nivel5 IS NOT NULL
    AND TRIM(descrcompl3nivel5) <> ''
)

SELECT DISTINCT
  EJUD, CNJ, DESCRICAO
FROM base
ORDER BY EJUD, CNJ;

