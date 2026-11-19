-- ============================================================
-- DIMENSION: tb_dm_integracao_ejud_cnj
-- Our own EJUD→CNJ flat lookup table
--
-- Replaces the triple-join chain:
--   movimento → faseexterna → integracaofasecompl → CNJ code
-- With a single join:
--   movimento → tb_dm_integracao_ejud_cnj → CNJ code
--
-- Run on Athena. This is the SQL equivalent of create_dim_integracao.py.
-- A data engineer can add this to the dimension branch for daily refresh.
-- ============================================================

-- HOW TO UPDATE WHEN CNJ CHANGES:
--   1. If MPS updates integracaofasecompl → re-run this query
--   2. If new faseext codes need override → add to the CASE statement
--   3. If new void codes → add to NOT IN list
--   4. If new wildcards → add to CASE for codcompl nullification
WITH
-- Step 1: faseexterna with overrides
faseexterna_override AS (
    SELECT
        codfase,
        CASE codfaseext
            WHEN 60006 THEN 3      -- Decisão em Petição  → Decisão
            WHEN 60005 THEN 11009  -- Despacho em Petição → Despacho
            ELSE codfaseext
        END AS codfaseext
    FROM ejud.faseexterna
    WHERE codfaseext NOT IN (
        60007,  -- Baixa provisória (void)
        50010,  -- Baixa diligência (EJUD custom, old)
        60011   -- Pedido Desarquivamento (internal 3VP)
    )
),

-- Step 2: active integracao with wildcard cleanup
integ_clean AS (
    SELECT
        codfasenivel2,
        codfasenivel2cnj,
        CASE WHEN codcompl1nivel3 IN (59998, 59999) THEN NULL ELSE codcompl1nivel3 END AS codcompl1nivel3,
        CASE WHEN codcompl1nivel3 IN (59998, 59999) THEN codfasenivel2cnj ELSE codcompl1nivel3cnj END AS codcompl1nivel3cnj,
        CASE WHEN codcompl2nivel4 IN (59998, 59999) THEN NULL ELSE codcompl2nivel4 END AS codcompl2nivel4,
        codcompl2nivel4cnj,
        CASE WHEN codcompl3nivel5 IN (59998, 59999) THEN NULL ELSE codcompl3nivel5 END AS codcompl3nivel5,
        codcompl3nivel5cnj,
        ultimonivelcnj,
        descrfasenivel2,
        CASE WHEN codcompl1nivel3 IN (59998, 59999) THEN NULL ELSE descrcompl1nivel3 END AS descrcompl1nivel3,
        descrcompl2nivel4,
        descrcompl3nivel5
    FROM ejud.integracaofasecompl_ejud_cnj
    WHERE indativo = 'S'
),

-- Step 3: join faseexterna → integracao (specific complement rows)
specific_rows AS (
    SELECT
        fe.codfase,
        fe.codfaseext,
        i.codcompl1nivel3  AS codcompl1,
        i.codcompl2nivel4  AS codcompl2,
        i.codcompl3nivel5  AS codcompl3,
        COALESCE(i.codcompl1nivel3cnj, i.codfasenivel2cnj) AS cod_mni_cnj,
        i.codcompl2nivel4cnj AS cnj_comp1,
        i.codcompl3nivel5cnj AS cnj_comp2,
        i.codfasenivel2cnj   AS base_cod_mni,
        i.ultimonivelcnj,
        i.descrfasenivel2    AS descr_fase_cnj,
        i.descrcompl1nivel3  AS descr_compl1,
        i.descrcompl2nivel4  AS descr_compl2,
        i.descrcompl3nivel5  AS descr_compl3
    FROM faseexterna_override fe
    INNER JOIN integ_clean i ON fe.codfaseext = i.codfasenivel2
),

-- Step 4: base/fallback rows (NULL complements → nivel-2 code)
base_rows AS (
    SELECT DISTINCT
        fe.codfase,
        fe.codfaseext,
        CAST(NULL AS decimal(6,0)) AS codcompl1,
        CAST(NULL AS decimal(6,0)) AS codcompl2,
        CAST(NULL AS decimal(6,0)) AS codcompl3,
        i.codfasenivel2cnj  AS cod_mni_cnj,
        CAST(NULL AS varchar) AS cnj_comp1,
        CAST(NULL AS varchar) AS cnj_comp2,
        i.codfasenivel2cnj  AS base_cod_mni,
        CAST(2 AS decimal(1,0)) AS ultimonivelcnj,
        i.descrfasenivel2    AS descr_fase_cnj,
        CAST(NULL AS varchar) AS descr_compl1,
        CAST(NULL AS varchar) AS descr_compl2,
        CAST(NULL AS varchar) AS descr_compl3
    FROM faseexterna_override fe
    INNER JOIN integ_clean i ON fe.codfaseext = i.codfasenivel2
),

-- Step 5: faseext codes with NO integracao match → force-to-parent
orphan_faseext AS (
    SELECT DISTINCT
        fe.codfase,
        fe.codfaseext,
        CAST(NULL AS decimal(6,0)) AS codcompl1,
        CAST(NULL AS decimal(6,0)) AS codcompl2,
        CAST(NULL AS decimal(6,0)) AS codcompl3,
        CAST(fe.codfaseext AS varchar) AS cod_mni_cnj,
        CAST(NULL AS varchar) AS cnj_comp1,
        CAST(NULL AS varchar) AS cnj_comp2,
        CAST(fe.codfaseext AS varchar) AS base_cod_mni,
        CAST(2 AS decimal(1,0)) AS ultimonivelcnj,
        f.descr              AS descr_fase_cnj,
        CAST(NULL AS varchar) AS descr_compl1,
        CAST(NULL AS varchar) AS descr_compl2,
        CAST(NULL AS varchar) AS descr_compl3
    FROM faseexterna_override fe
    LEFT JOIN (SELECT DISTINCT codfasenivel2 FROM integ_clean) i
        ON fe.codfaseext = i.codfasenivel2
    LEFT JOIN ejud.fase f ON fe.codfase = f.codfase
    WHERE i.codfasenivel2 IS NULL
),

-- Step 6: union all
combined AS (
    SELECT * FROM specific_rows
    UNION
    SELECT * FROM base_rows
    UNION
    SELECT * FROM orphan_faseext
)

-- Step 7: final output with EJUD descriptions
SELECT DISTINCT
    c.codfase,
    c.codcompl1,
    c.codcompl2,
    c.codcompl3,
    f.descr              AS descr_fase_ejud,
    ec1.descr            AS descr_compl1_ejud,
    ec2.descr            AS descr_compl2_ejud,
    ec3.descr            AS descr_compl3_ejud,
    c.codfaseext,
    c.cod_mni_cnj,
    c.cnj_comp1,
    c.cnj_comp2,
    c.base_cod_mni,
    c.ultimonivelcnj,
    c.descr_fase_cnj,
    c.descr_compl1,
    c.descr_compl2,
    c.descr_compl3,
    current_timestamp    AS data_carga
FROM combined c
LEFT JOIN ejud.fase f ON c.codfase = f.codfase
LEFT JOIN ejud.complemento ec1 ON c.codcompl1 = ec1.codcompl
LEFT JOIN ejud.complemento ec2 ON c.codcompl2 = ec2.codcompl
LEFT JOIN ejud.complemento ec3 ON c.codcompl3 = ec3.codcompl
;
