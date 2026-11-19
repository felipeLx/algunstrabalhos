WITH
-- ============================================================
-- SECTION 1: BASE DEFINITION
-- Processes currently distributed (codfase 26/36), physically
-- at local 3763, with codcompt in (10,11,14,15,19,20,22) and
-- codtipproc != 288.
-- ============================================================
-- Most recent movimentofisico per coddoc must be 3763 at dateref
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
-- Most recent logprocesso entry per coddoc, applying process-type filters
proc_atual AS (
    SELECT
            p.coddoc,
            p.codtipproc,
            p.codcompt
        FROM t_processo p
        join local_atual la on p.coddoc = la.coddoc
        WHERE codtipproc != 288
        and codcompt IN (10, 11, 14, 15, 19, 20, 22)
),
-- Most recent movement with codfase 26 or 36 per coddoc
ultima_distribuicao AS (
    SELECT coddoc, dthrmov AS dataultdis
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM movdistribuicao m
        join proc_atual la on m.coddoc = la.coddoc
        WHERE m.dthrmov <= '{v_dateref_str}' and la.coddoc is not null
    ) t
    WHERE rn = 1
),
-- Preliminary base: intersection of distribution, process, and physical location filters
base_distribuicao AS (
    SELECT ud.coddoc, ud.dataultdis
    FROM ultima_distribuicao ud
    JOIN proc_atual pa ON ud.coddoc = pa.coddoc
),
-- ============================================================
-- SECTION 2: EXCLUSION FILTERS
-- All rules are checked for events after the last codfase 26/36
-- (dataultdis) up to the reference date.
-- ============================================================
-- Rule 1: Simple terminal phases (archived, sent to finalization, etc.)
excl_fase_simples AS (
    SELECT DISTINCT m.coddoc
    FROM movimento m
    JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
    WHERE m.dthrmov <= '{v_dateref_str}'
      AND m.dthrmov >= bd.dataultdis
      AND m.codfase IN (861, 870, 22, 50011, 50014)
),
-- Rule 2: Phase 50002 with terminal complementos
excl_findo AS (
    SELECT DISTINCT m.coddoc
    FROM movimento m
    JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
    WHERE m.dthrmov <= '{v_dateref_str}'
      AND m.dthrmov >= bd.dataultdis
      AND m.codfase = 50002
      AND (
              m.codcompl1 IN (50117, 50118, 60435)
          OR  COALESCE(m.codcompl2, 0) = 50118
      )
),
-- Rule 3: Phase 60 with codcompl1=50007 AND codcompl2=50119 (expedição)
excl_expedicao AS (
    SELECT DISTINCT m.coddoc
    FROM movimento m
    JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
    WHERE m.dthrmov <= '{v_dateref_str}'
      AND m.dthrmov >= bd.dataultdis
      AND m.codfase = 60
      AND m.codcompl1 = 50007
      AND COALESCE(m.codcompl2, 0) = 50119
),
-- Rules 4 & 5: Phases 50123 or 123 with remessa complementos
excl_remessa AS (
    SELECT DISTINCT m.coddoc
    FROM movimento m
    JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
    WHERE m.dthrmov <= '{v_dateref_str}'
      AND m.dthrmov >= bd.dataultdis
      AND m.codfase IN (50123, 123)
      AND (
              m.codcompl1 IN (50104, 50648, 50105, 50649, 50723)
          OR  COALESCE(m.codcompl2, 0) IN (50052, 50079, 50080)
      )
),
-- Rule 6: Phase 123 with codcompl1=66653 AND codcompl2=66652
excl_remessa_esp AS (
    SELECT DISTINCT m.coddoc
    FROM movimento m
    JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
    WHERE m.dthrmov <= '{v_dateref_str}'
      AND m.dthrmov >= bd.dataultdis
      AND m.codfase = 123
      AND m.codcompl1 = 66653
      AND COALESCE(m.codcompl2, 0) = 66652
),
-- Rule 7: Any physical movement to exit locations after last distribution.
-- Even if the process returned to 3763 afterwards, any visit to these
-- locations after the last codfase 26/36 disqualifies it.
excl_local_saida AS (
    SELECT DISTINCT mf.coddoc
    FROM movimentofisico mf
    JOIN base_distribuicao bd ON mf.coddoc = bd.coddoc
    WHERE mf.dthrmov <= '{v_dateref_str}'
      AND mf.dthrmov >= bd.dataultdis
      AND mf.codlocal IN (
          503,  515,  519,  520,  521,  549,
          583,  584,
          419,  420,  421,  422,  423,  424,  425,  426,  427,  428,  429,  430,
          1713, 1714, 1715, 1771,
          2085, 2594, 2593,
          3146, 3147, 3148, 3149, 3150, 3151,
          3205, 3206,
          3582, 3583, 3584, 3585,
          3875,
          5765
      )
),
-- Rule 8: Suspension (Phase 3 or 11009 with codcompl1 25 or 11025)
-- WITHOUT a subsequent reactivation (Phase 849).
-- Only the most-recent suspension event per coddoc is checked.
excl_suspensao AS (
    SELECT DISTINCT s.coddoc
    FROM (
        -- Most recent suspension event after last distribution
        SELECT coddoc, dthrmov
        FROM (
            SELECT
                m.coddoc,
                m.dthrmov,
                ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
            FROM movimento m
            JOIN base_distribuicao bd ON m.coddoc = bd.coddoc
            WHERE m.dthrmov <= '{v_dateref_str}'
              AND m.dthrmov >= bd.dataultdis
              AND m.codfase  IN (3, 11009)
              AND m.codcompl1 IN (25, 11025)
        ) t
        WHERE rn = 1
    ) s
    -- Look for a fase 849 after the suspension event
    LEFT JOIN (
        SELECT m.coddoc, m.dthrmov
        FROM movimento m
        WHERE m.dthrmov <= '{v_dateref_str}'
          AND m.codfase = 849
    ) r ON r.coddoc = s.coddoc AND r.dthrmov > s.dthrmov
    WHERE r.coddoc IS NULL  -- no reactivation found → still suspended
),
-- ============================================================
-- SECTION 3: FILTERED BASE + DRIVING INTERFACES
-- ============================================================
-- Apply all exclusions to produce the definitive turmas acervo
base_acervo AS (
    SELECT bd.coddoc, bd.dataultdis
    FROM base_distribuicao bd
    LEFT JOIN excl_fase_simples e1 ON bd.coddoc = e1.coddoc
    LEFT JOIN excl_findo        e2 ON bd.coddoc = e2.coddoc
    LEFT JOIN excl_expedicao    e3 ON bd.coddoc = e3.coddoc
    LEFT JOIN excl_remessa      e4 ON bd.coddoc = e4.coddoc
    LEFT JOIN excl_remessa_esp  e5 ON bd.coddoc = e5.coddoc
    LEFT JOIN excl_local_saida  e6 ON bd.coddoc = e6.coddoc
    LEFT JOIN excl_suspensao    e7 ON bd.coddoc = e7.coddoc
    WHERE e1.coddoc IS NULL
      AND e2.coddoc IS NULL
      AND e3.coddoc IS NULL
      AND e4.coddoc IS NULL
      AND e5.coddoc IS NULL
      AND e6.coddoc IS NULL
      AND e7.coddoc IS NULL
),
-- Unified acervo interface: provides datareferencia for all downstream CTEs
acervo AS (
    SELECT coddoc, '{v_dateref_str}' AS datareferencia
    FROM base_acervo
),
-- Most recent movimentorelator per coddoc → magistrado and orgão julgador
mov_relator AS (
    SELECT coddoc, codmagrel, codorgjulg
    FROM (
        SELECT
            mr.coddoc,
            mr.codmagrel,
            mr.codorgjulg,
            ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov DESC) AS rn
        FROM movimentorelator mr
        JOIN base_acervo ba ON mr.coddoc = ba.coddoc
        WHERE mr.dthrmov <= '{v_dateref_str}'
    ) t
    WHERE rn = 1
),
-- ============================================================
-- SECTION 4: REPORTING CTEs
-- Same structure as query_acervo_relatorio.sql; all references to
-- acervo_ejud replaced with the acervo CTE defined above.
-- ============================================================
tmp_localvirtdoc AS (
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
        FROM acervo a
        JOIN localvirtualdocumento flv ON a.coddoc = flv.coddoc
            AND flv.dthrentr <= a.datareferencia
    ) t
    WHERE t.rn = 1
),
relatorio_base AS (
    SELECT
        '{v_dateref_str}'   AS dth_referencia,
        ba.coddoc,
        ba.dataultdis       AS dth_ultima_distribuicao,
        pm.nome             AS ultimo_relator,
        po.nome             AS ultimo_orgao_julgador,
        mr.codmagrel        AS codultrel,
        mr.codorgjulg       AS codorgjulg
    FROM base_acervo ba
    LEFT JOIN mov_relator mr ON ba.coddoc = mr.coddoc
    LEFT JOIN magistrado mag ON mr.codmagrel = mag.codmag
    LEFT JOIN pessoa pm      ON mag.codpess  = pm.codpess
    LEFT JOIN orgaojulgador oj ON mr.codorgjulg = oj.codorgjulg
    LEFT JOIN pessoa po      ON oj.codpess   = po.codpess
),
indicadores_processo AS (
    SELECT *
    FROM (
        SELECT
            p.coddoc,
            p.indeletr          AS ind_eletronico,
            p.codtipproc        AS cod_tipo_processo,
            p.codcompt          AS cod_competencia,
            p.indurgente        AS urgente,
            p.indpedlimin       AS pedido_liminar,
            p.inddefenpub       AS defensoria_publica,
            p.indjustgrati      AS justica_gratuita,
            p.indmenor          AS crianca_adolescente,
            p.indmp             AS funciona_mp,
            p.indsegrjust       AS segredo_justica,
            p.indprioidoso      AS idosos,
            p.indprioidoso80    AS idosos_80_mais,
            p.inddoencagrave    AS doenca_grave,
            p.indpriodefic      AS prioridade_deficiente,
            p.indreupresopje    AS reu_preso,
            p.indsitrua         AS situacao_rua,
            tp.descr            AS classe,
            p.numsequencial,
            p.dv,
            p.numano,
            p.codjustica,
            p.codregi,
            p.codlocregi,
            CASE WHEN p.indsupersigil = 'S' THEN 5
                 WHEN p.indsigil      = 'S' THEN 4
                 WHEN p.indsegrjust   = 'S' THEN 1
                 ELSE 0
            END AS nivel_sigilo,
            ROW_NUMBER() OVER (PARTITION BY p.coddoc ORDER BY p.dthralt DESC) AS rn
        FROM acervo a
        LEFT JOIN logprocesso p  ON a.coddoc = p.coddoc AND p.dthralt <= a.datareferencia
        LEFT JOIN tipoprocesso tp ON p.codtipproc = tp.codtipproc
        WHERE p.tipoper != 'E'
    ) sub
    WHERE rn = 1
),
complemento AS (
    SELECT
        codcompl,
        CASE WHEN CODCOMPL IN (9999, 59998) THEN NULL ELSE descr END AS descr,
        descrmnem,
        indativo
    FROM complemento
),
ultimo_movimento AS (
    SELECT *
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov           AS dth_ultimo_movimento,
            fe.codfaseext       AS cod_ultimo_movimento,
            f.descr             AS ultimo_movimento,
            c1.descr            AS comp1_ultimo_movimento,
            c2.descr            AS comp2_ultimo_movimento,
            c3.descr            AS comp3_ultimo_movimento,
            m.usuincl           AS usuario_inclusao,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
        FROM acervo a
        LEFT JOIN movimento m   ON m.coddoc = a.coddoc AND m.dthrmov <= a.datareferencia
        LEFT JOIN fase f        ON m.codfase   = f.codfase
        LEFT JOIN faseexterna fe ON f.codfase  = fe.codfase
        LEFT JOIN complemento c1 ON m.codcompl1 = c1.codcompl
        LEFT JOIN complemento c2 ON m.codcompl2 = c2.codcompl
        LEFT JOIN complemento c3 ON m.codcompl3 = c3.codcompl
    ) t
    WHERE rn = 1
),
ultimo_movimento_cnj AS (
    SELECT *
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov           AS dth_ultimo_mov_cnj,
            fe.codfaseext       AS cod_ultimo_mov_cnj,
            f.descr             AS ultimo_mov_cnj,
            c1.codcompl         AS comp1_cnj,
            c1.descr            AS comp1_ultimo_mov_cnj,
            c2.codcompl         AS comp2_cnj,
            c2.descr            AS comp2_ultimo_mov_cnj,
            c3.codcompl         AS comp3_cnj,
            c3.descr            AS comp3_ultimo_mov_cnj,
            m.usuincl           AS usuario_inclusao,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
        FROM acervo a
        LEFT JOIN movimento m   ON m.coddoc = a.coddoc AND m.dthrmov <= a.datareferencia
        LEFT JOIN fase f        ON m.codfase   = f.codfase
        LEFT JOIN faseexterna fe ON f.codfase  = fe.codfase
        LEFT JOIN complemento c1 ON m.codcompl1 = c1.codcompl
        LEFT JOIN complemento c2 ON m.codcompl2 = c2.codcompl
        LEFT JOIN complemento c3 ON m.codcompl3 = c3.codcompl
        WHERE m.codfase <> 67
    ) t
    WHERE rn = 1
),
ultimo_julgamento AS (
    SELECT *
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov           AS dth_ultimo_julgamento,
            f.descr             AS ultimo_julgamento,
            c1.descr            AS comp1_ultimo_julgamento,
            c2.descr            AS comp2_ultimo_julgamento,
            c3.descr            AS comp3_ultimo_julgamento,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
        FROM acervo a
        LEFT JOIN movimento m   ON a.coddoc = m.coddoc AND m.dthrmov <= a.datareferencia
        LEFT JOIN fase f        ON m.codfase   = f.codfase
        LEFT JOIN faseexterna fe ON f.codfase  = fe.codfase
        LEFT JOIN complemento c1 ON m.codcompl1 = c1.codcompl
        LEFT JOIN complemento c2 ON m.codcompl2 = c2.codcompl
        LEFT JOIN complemento c3 ON m.codcompl3 = c3.codcompl
        WHERE fe.codfaseext IN (193, 50193)
    ) t
    WHERE rn = 1
),
local_virtual AS (
    SELECT
        t.coddoc,
        t.dthrentr      AS dth_entrada_local_virtual,
        l.descr         AS local_virtual
    FROM acervo a
    LEFT JOIN tmp_localvirtdoc t ON a.coddoc = t.coddoc AND t.dthrentr <= a.datareferencia
    LEFT JOIN localvirtual l     ON t.codlocalvirt = l.codlocalvirt AND t.codlocal = l.codlocal
),
assuntos AS (
    SELECT
        pa.coddoc,
        CASE WHEN size(collect_list(pa.codass)) = 0 THEN NULL
             ELSE concat_ws(';', collect_list(pa.codass))
        END AS cod_assunto,
        CASE WHEN size(collect_list(ass.descr)) = 0 THEN NULL
             ELSE concat_ws(';', collect_list(ass.descr))
        END AS assunto
    FROM processoassunto pa
    LEFT JOIN assunto ass ON pa.codass = ass.codass
    GROUP BY pa.coddoc
),
vara_origem AS (
    SELECT CAST(a.coddoc  AS int) AS coddoc,
           CAST(aa.codvara AS int) AS cod_vara,
           p.nome                  AS vara
      FROM autoassociado aa
      LEFT JOIN vara v   ON CAST(aa.codvara    AS int) = CAST(v.codvara    AS int)
                        AND CAST(aa.codjustorig AS int) = CAST(v.codjustorig AS int)
      LEFT JOIN pessoa p ON CAST(v.codpess     AS int) = CAST(p.codpess     AS int)
      LEFT JOIN acervo a ON CAST(aa.coddoc     AS int) = CAST(a.coddoc      AS int)
     WHERE aa.numseq = 1
),
primeiro_relator AS (
    SELECT *
    FROM (
        SELECT
            mr.coddoc,
            mr.dthrmov  AS dth_distribuicao,
            pm.nome     AS relator,
            po.nome     AS orgao_julgador,
            ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov ASC) rn
        FROM movimentorelator mr
        LEFT JOIN magistrado mag ON mr.codmagrel  = mag.codmag
        LEFT JOIN pessoa pm      ON mag.codpess   = pm.codpess
        LEFT JOIN orgaojulgador oj ON mr.codorgjulg = oj.codorgjulg
        LEFT JOIN pessoa po      ON oj.codpess    = po.codpess
    ) t
    WHERE rn = 1
),
local_fisico AS (
    SELECT *
    FROM (
        SELECT
            mf.coddoc,
            mf.dthrmov  AS dth_entrada_local_fisico,
            lf.codlocal,
            lf.descr    AS local_fisico,
            ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) rn
        FROM acervo a
        LEFT JOIN movimentofisico mf ON mf.coddoc = a.coddoc AND mf.dthrmov <= a.datareferencia
        LEFT JOIN localfisico lf     ON mf.codlocal = lf.codlocal
    ) t
    WHERE rn = 1
),
parte AS (
    SELECT
        r.coddoc,
        array_join(
            filter(
                array(
                    /* Polo 1 (autores) */
                    COALESCE(array_join(
                        array_sort(collect_set(
                            CASE WHEN CAST(tp.codtippolo AS INT) = 1 THEN trim(cj.nome) END
                        )),
                        '; '
                    ), ''),
                    /* Polo 2 (réus) */
                    COALESCE(array_join(
                        array_sort(collect_set(
                            CASE WHEN CAST(tp.codtippolo AS INT) = 2 THEN trim(cj.nome) END
                        )),
                        '; '
                    ), '')
                ),
                x -> x <> ''
            ),
            ' x '
        ) AS partes
    FROM acervo r
    JOIN parte p          ON r.coddoc   = p.coddoc
    JOIN pessoa cj        ON cj.codpess = p.codpess
    JOIN parteprocesso pp ON pp.codparte = p.codparte
    JOIN tipoparte tp     ON tp.codtipparte = pp.codtipparte
    GROUP BY r.coddoc
),
-- ── Suspension / repercussion reporting CTEs ──────────────────────────────
SUSPENSAO AS (
    SELECT
        m.coddoc,
        m.dthrmov,
        m.codfase,
        m.codcompl1,
        m.codcompl2,
        m.codcompl3
    FROM movimento m
    JOIN acervo a ON a.coddoc = m.coddoc
    WHERE m.dthrmov <= a.datareferencia
      AND (
              (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
          OR  (m.codfase IN (3, 20)          AND m.codcompl1 = 25)
          OR  (m.codfase IN (3, 20, 29)      AND m.codcompl1 = 1013 AND COALESCE(m.codcompl2, 0) = 1015)
          OR  (m.codfase = 861               AND m.codcompl1 IN (690, 50068))
          OR  (m.codfase = 861               AND m.codcompl1 IN (359, 360, 361, 362, 363, 445, 50069)
                                             AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
          OR  (m.codfase = 50002             AND m.codcompl1 IN (306, 12065))
      )
),
REATIVACAO AS (
    SELECT
        m.coddoc,
        m.dthrmov
    FROM movimento m
    JOIN acervo a ON a.coddoc = m.coddoc
    WHERE m.dthrmov <= a.datareferencia
      AND (
              (m.codfase = 50002 AND m.codcompl1 = 12066)
          OR  m.codfase IN (893, 24, 193)
      )
),
REPERCUSSAO AS (
    SELECT * FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            m.codfase,
            m.codcompl1,
            m.codcompl2,
            m.codcompl3,
            CASE
                WHEN ms.dthrterminosobre >= a.datareferencia THEN NULL
                ELSE ms.dthrterminosobre
            END AS dthrterminosobre
        FROM movimento m
        JOIN acervo a          ON a.coddoc  = m.coddoc
        JOIN movsobrestamento ms ON ms.coddoc = m.coddoc AND ms.dthrmov = m.dthrmov
        WHERE m.dthrmov <= a.datareferencia
          AND m.codfase  = 50040
    ) x
    WHERE dthrterminosobre IS NULL
),
SUSP_REP_AGG AS (
    SELECT
        x.coddoc,
        MIN(x.dthrmov)                                AS datasusp,
        MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END) AS datarepercussao
    FROM (
        SELECT s.coddoc, s.dthrmov, 'S' AS src
          FROM SUSPENSAO s
          LEFT JOIN REATIVACAO r
            ON r.coddoc  = s.coddoc
           AND r.dthrmov > s.dthrmov
         WHERE r.coddoc IS NULL
        UNION ALL
        SELECT r.coddoc, r.dthrmov, 'R' AS src
          FROM REPERCUSSAO r
    ) x
    GROUP BY x.coddoc
),
susp_rep_final AS (
    SELECT
        acer.coddoc,
        -- Suspension
        a.datasusp                                      AS dth_suspensao,
        f_susp.descr                                    AS movimento_suspensao,
        c1_susp.descr                                   AS comp1_ultima_suspensao,
        c2_susp.descr                                   AS comp2_ultima_suspensao,
        c3_susp.descr                                   AS comp3_ultima_suspensao,
        CASE WHEN a.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
        -- Repercussão / Sobrestamento
        a.datarepercussao                               AS dth_repercussao,
        a.datarepercussao                               AS dth_sobrestamento,
        r.dthrterminosobre                              AS dthrterminosobre,
        f_rep.descr                                     AS movimento_sobrestamento,
        c1_rep.descr                                    AS comp1_ultimo_sobrestamento,
        c2_rep.descr                                    AS comp2_ultimo_sobrestamento,
        c3_rep.descr                                    AS comp3_ultimo_sobrestamento,
        CASE WHEN a.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestado
    FROM acervo acer
    LEFT JOIN SUSP_REP_AGG  a       ON acer.coddoc   = a.coddoc
    LEFT JOIN SUSPENSAO     s       ON s.coddoc       = a.coddoc AND s.dthrmov = a.datasusp
    LEFT JOIN fase          f_susp  ON s.codfase      = f_susp.codfase
    LEFT JOIN complemento   c1_susp ON s.codcompl1    = c1_susp.codcompl
    LEFT JOIN complemento   c2_susp ON s.codcompl2    = c2_susp.codcompl
    LEFT JOIN complemento   c3_susp ON s.codcompl3    = c3_susp.codcompl
    LEFT JOIN REPERCUSSAO   r       ON r.coddoc       = a.coddoc AND r.dthrmov = a.datarepercussao
    LEFT JOIN fase          f_rep   ON r.codfase      = f_rep.codfase
    LEFT JOIN complemento   c1_rep  ON r.codcompl1    = c1_rep.codcompl
    LEFT JOIN complemento   c2_rep  ON r.codcompl2    = c2_rep.codcompl
    LEFT JOIN complemento   c3_rep  ON r.codcompl3    = c3_rep.codcompl
),
-- ── Decisão CTEs ──────────────────────────────────────────────────────────
acervo_decisao AS (
    SELECT coddoc, datareferencia
    FROM acervo
),
movimentos_filtrados_decisao AS (
    SELECT
        m.coddoc,
        m.dthrmov,
        m.codfase,
        m.codcompl1,
        m.codcompl2,
        m.codcompl3,
        f.descr  AS ultima_decisao,
        c1.descr AS comp1_ultima_decisao,
        c2.descr AS comp2_ultima_decisao,
        c3.descr AS comp3_ultima_decisao
    FROM movimento m
    LEFT JOIN fase        f  ON m.codfase   = f.codfase
    LEFT JOIN complemento c1 ON m.codcompl1 = c1.codcompl
    LEFT JOIN complemento c2 ON m.codcompl2 = c2.codcompl
    LEFT JOIN complemento c3 ON m.codcompl3 = c3.codcompl
    JOIN acervo_decisao   a  ON a.coddoc    = m.coddoc
    WHERE m.codfase IN (3, 20)
      AND m.dthrmov <= a.datareferencia
),
ultimas_decisoes AS (
    SELECT
        mf.coddoc,
        mf.dthrmov              AS dth_ultima_decisao,
        mf.ultima_decisao,
        mf.comp1_ultima_decisao,
        mf.comp2_ultima_decisao,
        mf.comp3_ultima_decisao,
        ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn
    FROM movimentos_filtrados_decisao mf
),
decisao_final AS (
    SELECT
        u.coddoc,
        u.dth_ultima_decisao,
        u.ultima_decisao,
        u.comp1_ultima_decisao,
        u.comp2_ultima_decisao,
        u.comp3_ultima_decisao,
        CASE WHEN u.dth_ultima_decisao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao
    FROM ultimas_decisoes u
    WHERE u.rn = 1
),
-- ============================================================
-- SECTION 5: FINAL ASSEMBLY
-- Same columns as query_acervo_relatorio.sql / relatorio_final
-- ============================================================
relatorio_final AS (
    SELECT
        rb.dth_referencia,
        rb.coddoc,
        rb.dth_ultima_distribuicao,
        rb.ultimo_relator,
        rb.ultimo_orgao_julgador,
        ip.ind_eletronico,
        ip.cod_tipo_processo,
        ip.cod_competencia,
        ip.urgente,
        ip.pedido_liminar,
        ip.defensoria_publica,
        ip.justica_gratuita,
        ip.crianca_adolescente,
        ip.funciona_mp,
        ip.segredo_justica,
        ip.idosos,
        ip.idosos_80_mais,
        ip.doenca_grave,
        ip.prioridade_deficiente,
        ip.reu_preso,
        ip.situacao_rua,
        ip.classe,
        ip.numsequencial,
        ip.dv,
        ip.numano,
        ip.codjustica,
        ip.codregi,
        ip.codlocregi,
        um.dth_ultimo_movimento,
        um.cod_ultimo_movimento,
        um.ultimo_movimento,
        um.comp1_ultimo_movimento,
        um.comp2_ultimo_movimento,
        um.comp3_ultimo_movimento,
        um.usuario_inclusao,
        uj.dth_ultimo_julgamento,
        uj.ultimo_julgamento,
        uj.comp1_ultimo_julgamento,
        uj.comp2_ultimo_julgamento,
        uj.comp3_ultimo_julgamento,
        CASE WHEN uj.dth_ultimo_julgamento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgado,
        lv.dth_entrada_local_virtual,
        lv.local_virtual,
        asu.cod_assunto,
        asu.assunto,
        vo.cod_vara,
        vo.vara,
        usv.dth_suspensao,
        usv.movimento_suspensao,
        usv.comp1_ultima_suspensao,
        usv.comp2_ultima_suspensao,
        usv.dth_sobrestamento,
        usv.dthrterminosobre,
        usv.movimento_sobrestamento,
        usv.comp1_ultimo_sobrestamento,
        usv.comp2_ultimo_sobrestamento,
        usv.ind_sobrestado,
        pr.dth_distribuicao,
        pr.relator,
        pr.orgao_julgador,
        lf.dth_entrada_local_fisico,
        lf.local_fisico,
        CASE WHEN ip.nivel_sigilo = 5 THEN 'RESTRITO' ELSE
            lpad(ip.numsequencial, 7, '0') || '-' ||
            lpad(ip.dv, 2, '0')           || '.' ||
            ip.numano                      || '.' ||
            ip.codjustica                  || '.' ||
            lpad(ip.codregi, 2, '0')       || '.' ||
            lpad(ip.codlocregi, 4, '0')
        END AS num_processo,
        rb.codultrel    AS cod_ultimo_relator,
        rb.codorgjulg   AS cod_ultimo_orgao_julgador,
        usv.ind_suspenso,
        CASE WHEN ip.nivel_sigilo = 5 THEN 'RESTRITO' ELSE parte.partes END AS partes,
        usv.comp3_ultima_suspensao,
        usv.dth_repercussao,
        usv.comp3_ultimo_sobrestamento,
        ud.dth_ultima_decisao,
        ud.ultima_decisao,
        ud.comp1_ultima_decisao,
        ud.comp2_ultima_decisao,
        ud.comp3_ultima_decisao,
        COALESCE(ud.ind_decisao, 'N') AS ind_decisao,
        ip.nivel_sigilo,
        lf.codlocal     AS cod_ult_local,
        um1.dth_ultimo_mov_cnj      AS dth_ultimo_movimento_d01,
        um1.cod_ultimo_mov_cnj      AS cod_ultimo_movimento_d01,
        um1.ultimo_mov_cnj          AS ultimo_movimento_d01,
        um1.comp1_cnj               AS cod_comp1_d01,
        um1.comp1_ultimo_mov_cnj    AS comp1_ultimo_movimento_d01,
        um1.comp2_cnj               AS cod_comp2_d01,
        um1.comp2_ultimo_mov_cnj    AS comp2_ultimo_movimento_d01,
        um1.comp3_cnj               AS cod_comp3_d01,
        um1.comp3_ultimo_mov_cnj    AS comp3_ultimo_movimento_d01
    FROM relatorio_base rb
    LEFT JOIN indicadores_processo ip  ON rb.coddoc = ip.coddoc
    LEFT JOIN ultimo_movimento um      ON rb.coddoc = um.coddoc
    LEFT JOIN ultimo_movimento_cnj um1 ON rb.coddoc = um1.coddoc
    LEFT JOIN ultimo_julgamento uj     ON rb.coddoc = uj.coddoc
    LEFT JOIN local_virtual lv         ON rb.coddoc = lv.coddoc
    LEFT JOIN assuntos asu             ON rb.coddoc = asu.coddoc
    LEFT JOIN vara_origem vo           ON rb.coddoc = vo.coddoc
    LEFT JOIN susp_rep_final usv       ON rb.coddoc = usv.coddoc
    LEFT JOIN primeiro_relator pr      ON rb.coddoc = pr.coddoc
    LEFT JOIN local_fisico lf          ON rb.coddoc = lf.coddoc
    LEFT JOIN parte parte              ON rb.coddoc = parte.coddoc
    LEFT JOIN decisao_final ud         ON rb.coddoc = ud.coddoc
)
SELECT * FROM relatorio_final
