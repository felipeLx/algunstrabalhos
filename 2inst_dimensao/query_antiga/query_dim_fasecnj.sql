SELECT DISTINCT
  CAST(codfasenivel2 AS INTEGER) AS ejud,
  CAST(codfasenivel2cnj AS INTEGER) AS cnj,
  TRIM(descrfasenivel2) AS descricao
FROM integracaofasecompl_ejud_cnj
WHERE codfasenivel2 IS NOT NULL
  AND codfasenivel2cnj IS NOT NULL
  AND descrfasenivel2 IS NOT NULL
  AND TRIM(descrfasenivel2) != ''

UNION ALL

SELECT DISTINCT
  CAST(codcompl1nivel3 AS INTEGER) AS ejud,
  CAST(codcompl1nivel3cnj AS INTEGER) AS cnj,
  TRIM(descrcompl1nivel3) AS descricao
FROM integracaofasecompl_ejud_cnj
WHERE codcompl1nivel3 IS NOT NULL
  AND codcompl1nivel3cnj IS NOT NULL
  AND descrcompl1nivel3 IS NOT NULL
  AND TRIM(descrcompl1nivel3) != ''

UNION ALL

SELECT DISTINCT
  CAST(codcompl2nivel4 AS INTEGER) AS ejud,
  CAST(codcompl2nivel4cnj AS INTEGER) AS cnj,
  TRIM(descrcompl2nivel4) AS descricao
FROM integracaofasecompl_ejud_cnj
WHERE codcompl2nivel4 IS NOT NULL
  AND codcompl2nivel4cnj IS NOT NULL
  AND descrcompl2nivel4 IS NOT NULL
  AND TRIM(descrcompl2nivel4) != ''

UNION ALL

SELECT DISTINCT
  CAST(codcompl3nivel5 AS INTEGER) AS ejud,
  CAST(codcompl3nivel5cnj AS INTEGER) AS cnj,
  TRIM(descrcompl3nivel5) AS descricao
FROM integracaofasecompl_ejud_cnj
WHERE codcompl3nivel5 IS NOT NULL
  AND codcompl3nivel5cnj IS NOT NULL
  AND descrcompl3nivel5 IS NOT NULL
  AND TRIM(descrcompl3nivel5) != ''

ORDER BY ejud, cnj;

