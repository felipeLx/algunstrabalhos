-- Stage 1: produce base_distribuicao (acervo bruto).
-- Exclusion logic moved to stage 2 as flag columns (ind_excl_*).
-- Output: coddoc, dataultdis, datareferencia
WITH
proc_atual AS (
    SELECT coddoc, codtipproc, codcompt FROM proc_atual_stg
),
local_atual AS (
    SELECT coddoc
    FROM (
        SELECT
            mf.coddoc,
            mf.codlocal,
            ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn
        FROM movimentofisico mf
        WHERE mf.dthrmov <= '{v_dateref_str}'
    ) t
    WHERE rn = 1
      AND codlocal = 3763
),
ultima_distribuicao AS (
    SELECT coddoc, dthrmov AS dataultdis
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM movimento m
        WHERE m.codfase IN (26, 36)
          AND m.dthrmov <= '{v_dateref_str}'
    ) t
    WHERE rn = 1
),
base_distribuicao AS (
    SELECT ud.coddoc, ud.dataultdis
    FROM ultima_distribuicao ud
    JOIN proc_atual pa ON ud.coddoc = pa.coddoc
    JOIN local_atual la ON ud.coddoc = la.coddoc
)
SELECT coddoc, dataultdis, '{v_dateref_str}' AS datareferencia
FROM base_distribuicao
