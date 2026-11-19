"""
Staged builder for turmas_relatorio_final.

Executes each CTE group as a separate SQL query with intermediate
materialization (persist + count). This breaks the single 700-line
query plan into 15 small plans, each of which can be GC'd before
the next one starts.

Preconditions (TempViews must be registered before calling):
  Heavy (filtered):  movimento, movimentorelator, movimentofisico,
                     logprocesso, movsobrestamento, processoassunto,
                     localvirtualdocumento
  Dimensions:        complemento, fase, faseexterna, magistrado, pessoa,
                     orgaojulgador, vara, localvirtual, assunto,
                     autoassociado, parte, parteprocesso, tipoparte,
                     tipoprocesso, localfisico
  Stage-1 result:    turmas_base  (coddoc, dataultdis, datareferencia)
"""

from pyspark.storagelevel import StorageLevel

_SL = StorageLevel.MEMORY_AND_DISK


def _mat(spark, name, sql, v_dateref_str, log):
    """Execute SQL, persist result, register TempView."""
    df = spark.sql(sql.format(v=v_dateref_str))
    df.persist(_SL)
    n = df.count()
    df.createOrReplaceTempView(name)
    log(f"  [{name}] {n:,} rows")
    return df


def build_turmas_staged(spark, v_dateref_str, log=print):
    """
    Build and return the final turmas DataFrame using staged materialization.
    Caller writes the returned DataFrame to parquet.
    """

    def mat(name, sql):
        return _mat(spark, name, sql, v_dateref_str, log)

    # ── 0: complemento lookup ────────────────────────────────────────────────
    log("step 0: complemento_stg")
    mat("complemento_stg", """
        SELECT codcompl,
               CASE WHEN codcompl IN (9999, 59998) THEN NULL ELSE descr END AS descr,
               descrmnem, indativo
        FROM complemento
    """)

    # ── 1: most recent movimentorelator per coddoc ───────────────────────────
    log("step 1: mov_relator_stg")
    mat("mov_relator_stg", """
        SELECT coddoc, codmagrel, codorgjulg
        FROM (
            SELECT mr.coddoc, mr.codmagrel, mr.codorgjulg,
                   ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov DESC) AS rn
            FROM movimentorelator mr
            WHERE mr.dthrmov <= '{v}'
        ) t WHERE rn = 1
    """)

    # ── 2: relatorio_base (magistrado / orgão names) ─────────────────────────
    log("step 2: relatorio_base_stg")
    mat("relatorio_base_stg", """
        SELECT '{v}' AS dth_referencia,
               tb.coddoc,
               tb.dataultdis AS dth_ultima_distribuicao,
               pm.nome       AS ultimo_relator,
               po.nome       AS ultimo_orgao_julgador,
               mr.codmagrel  AS codultrel,
               mr.codorgjulg AS codorgjulg
        FROM turmas_base tb
        LEFT JOIN mov_relator_stg mr ON tb.coddoc = mr.coddoc
        LEFT JOIN magistrado mag     ON mr.codmagrel  = mag.codmag
        LEFT JOIN pessoa pm          ON mag.codpess   = pm.codpess
        LEFT JOIN orgaojulgador oj   ON mr.codorgjulg = oj.codorgjulg
        LEFT JOIN pessoa po          ON oj.codpess    = po.codpess
    """)

    # ── 3: indicadores_processo ──────────────────────────────────────────────
    log("step 3: indicadores_stg")
    mat("indicadores_stg", """
        SELECT * FROM (
            SELECT p.coddoc,
                   p.indeletr        AS ind_eletronico,
                   p.codtipproc      AS cod_tipo_processo,
                   p.codcompt        AS cod_competencia,
                   p.indurgente      AS urgente,
                   p.indpedlimin     AS pedido_liminar,
                   p.inddefenpub     AS defensoria_publica,
                   p.indjustgrati    AS justica_gratuita,
                   p.indmenor        AS crianca_adolescente,
                   p.indmp           AS funciona_mp,
                   p.indsegrjust     AS segredo_justica,
                   p.indprioidoso    AS idosos,
                   p.indprioidoso80  AS idosos_80_mais,
                   p.inddoencagrave  AS doenca_grave,
                   p.indpriodefic    AS prioridade_deficiente,
                   p.indreupresopje  AS reu_preso,
                   p.indsitrua       AS situacao_rua,
                   tp.descr          AS classe,
                   p.numsequencial, p.dv, p.numano,
                   p.codjustica, p.codregi, p.codlocregi,
                   CASE WHEN p.indsupersigil = 'S' THEN 5
                        WHEN p.indsigil      = 'S' THEN 4
                        WHEN p.indsegrjust   = 'S' THEN 1
                        ELSE 0 END AS nivel_sigilo,
                   ROW_NUMBER() OVER (PARTITION BY p.coddoc ORDER BY p.dthralt DESC) AS rn
            FROM logprocesso p
            LEFT JOIN tipoprocesso tp ON p.codtipproc = tp.codtipproc
            WHERE p.tipoper != 'E'
              AND p.dthralt <= '{v}'
        ) sub WHERE rn = 1
    """)

    # ── 4: ultimo_movimento ──────────────────────────────────────────────────
    log("step 4: ult_mov_stg")
    mat("ult_mov_stg", """
        SELECT coddoc,
               dthrmov     AS dth_ultimo_movimento,
               codfaseext  AS cod_ultimo_movimento,
               fase_descr  AS ultimo_movimento,
               c1_descr    AS comp1_ultimo_movimento,
               c2_descr    AS comp2_ultimo_movimento,
               c3_descr    AS comp3_ultimo_movimento,
               usuincl     AS usuario_inclusao
        FROM (
            SELECT m.coddoc, m.dthrmov, fe.codfaseext,
                   f.descr  AS fase_descr,
                   c1.descr AS c1_descr,
                   c2.descr AS c2_descr,
                   c3.descr AS c3_descr,
                   m.usuincl,
                   ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
            FROM movimento m
            LEFT JOIN fase        f  ON m.codfase   = f.codfase
            LEFT JOIN faseexterna fe ON f.codfase   = fe.codfase
            LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
            LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
            LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
        ) t WHERE rn = 1
    """)

    # ── 5: ultimo_movimento_cnj (codfase != 67) ──────────────────────────────
    log("step 5: ult_mov_cnj_stg")
    mat("ult_mov_cnj_stg", """
        SELECT coddoc,
               dthrmov    AS dth_ultimo_mov_cnj,
               codfaseext AS cod_ultimo_mov_cnj,
               fase_descr AS ultimo_mov_cnj,
               codcompl1  AS comp1_cnj, c1_descr AS comp1_ultimo_mov_cnj,
               codcompl2  AS comp2_cnj, c2_descr AS comp2_ultimo_mov_cnj,
               codcompl3  AS comp3_cnj, c3_descr AS comp3_ultimo_mov_cnj,
               usuincl    AS usuario_inclusao_cnj
        FROM (
            SELECT m.coddoc, m.dthrmov, fe.codfaseext,
                   f.descr  AS fase_descr,
                   m.codcompl1, c1.descr AS c1_descr,
                   m.codcompl2, c2.descr AS c2_descr,
                   m.codcompl3, c3.descr AS c3_descr,
                   m.usuincl,
                   ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
            FROM movimento m
            LEFT JOIN fase        f  ON m.codfase   = f.codfase
            LEFT JOIN faseexterna fe ON f.codfase   = fe.codfase
            LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
            LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
            LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
            WHERE m.codfase <> 67
        ) t WHERE rn = 1
    """)

    # ── 6: ultimo_julgamento (codfaseext IN 193, 50193) ──────────────────────
    log("step 6: ult_julg_stg")
    mat("ult_julg_stg", """
        SELECT coddoc,
               dthrmov   AS dth_ultimo_julgamento,
               fase_descr AS ultimo_julgamento,
               c1_descr  AS comp1_ultimo_julgamento,
               c2_descr  AS comp2_ultimo_julgamento,
               c3_descr  AS comp3_ultimo_julgamento
        FROM (
            SELECT m.coddoc, m.dthrmov,
                   f.descr  AS fase_descr,
                   c1.descr AS c1_descr,
                   c2.descr AS c2_descr,
                   c3.descr AS c3_descr,
                   ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
            FROM movimento m
            LEFT JOIN fase        f  ON m.codfase  = f.codfase
            LEFT JOIN faseexterna fe ON f.codfase  = fe.codfase
            LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
            LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
            LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
            WHERE fe.codfaseext IN (193, 50193)
        ) t WHERE rn = 1
    """)

    # ── 7: local_virtual ─────────────────────────────────────────────────────
    log("step 7: local_virt_stg")
    mat("local_virt_stg", """
        SELECT t.coddoc,
               t.dthrentr AS dth_entrada_local_virtual,
               l.descr    AS local_virtual
        FROM (
            SELECT flv.coddoc, flv.dthrentr, flv.codlocalvirt, flv.codlocal,
                   ROW_NUMBER() OVER (PARTITION BY flv.coddoc ORDER BY flv.dthrentr DESC) AS rn
            FROM localvirtualdocumento flv
            WHERE flv.dthrentr <= '{v}'
        ) t
        LEFT JOIN localvirtual l ON t.codlocalvirt = l.codlocalvirt
                                AND t.codlocal     = l.codlocal
        WHERE t.rn = 1
    """)

    # ── 8: assuntos ──────────────────────────────────────────────────────────
    log("step 8: assuntos_stg")
    mat("assuntos_stg", """
        SELECT pa.coddoc,
               NULLIF(concat_ws(';', collect_list(CAST(pa.codass AS STRING))), '') AS cod_assunto,
               NULLIF(concat_ws(';', collect_list(ass.descr)), '')                 AS assunto
        FROM processoassunto pa
        LEFT JOIN assunto ass ON pa.codass = ass.codass
        GROUP BY pa.coddoc
    """)

    # ── 9: vara_origem ────────────────────────────────────────────────────────
    log("step 9: vara_origem_stg")
    mat("vara_origem_stg", """
        SELECT CAST(CAST(a.coddoc AS int) AS string) AS coddoc,
               CAST(aa.codvara AS int)               AS cod_vara,
               p.nome                                AS vara
        FROM autoassociado aa
        LEFT JOIN vara v ON CAST(aa.codvara    AS int) = CAST(v.codvara    AS int)
                        AND CAST(aa.codjustorig AS int) = CAST(v.codjustorig AS int)
        LEFT JOIN pessoa p ON CAST(v.codpess AS int) = CAST(p.codpess AS int)
        LEFT JOIN turmas_base a ON CAST(aa.coddoc AS int) = CAST(a.coddoc AS int)
        WHERE aa.numseq = 1
    """)

    # ── 10: primeiro_relator ─────────────────────────────────────────────────
    log("step 10: prim_relator_stg")
    mat("prim_relator_stg", """
        SELECT coddoc, dthrmov AS dth_distribuicao, relator, orgao_julgador
        FROM (
            SELECT mr.coddoc, mr.dthrmov,
                   pm.nome AS relator,
                   po.nome AS orgao_julgador,
                   ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov ASC) rn
            FROM movimentorelator mr
            LEFT JOIN magistrado  mag ON mr.codmagrel  = mag.codmag
            LEFT JOIN pessoa      pm  ON mag.codpess   = pm.codpess
            LEFT JOIN orgaojulgador oj ON mr.codorgjulg = oj.codorgjulg
            LEFT JOIN pessoa      po  ON oj.codpess    = po.codpess
        ) t WHERE rn = 1
    """)

    # ── 11: local_fisico ─────────────────────────────────────────────────────
    log("step 11: local_fisico_stg")
    mat("local_fisico_stg", """
        SELECT coddoc, dthrmov AS dth_entrada_local_fisico, codlocal, local_fisico
        FROM (
            SELECT mf.coddoc, mf.dthrmov, mf.codlocal, lf.descr AS local_fisico,
                   ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) rn
            FROM movimentofisico mf
            LEFT JOIN localfisico lf ON mf.codlocal = lf.codlocal
        ) t WHERE rn = 1
    """)

    # ── 12: partes ───────────────────────────────────────────────────────────
    log("step 12: parte_stg")
    mat("parte_stg", """
        SELECT r.coddoc,
               array_join(
                   filter(
                       array(
                           COALESCE(array_join(array_sort(collect_set(
                               CASE WHEN CAST(tp.codtippolo AS INT) = 1
                                    THEN trim(cj.nome) END
                           )), '; '), ''),
                           COALESCE(array_join(array_sort(collect_set(
                               CASE WHEN CAST(tp.codtippolo AS INT) = 2
                                    THEN trim(cj.nome) END
                           )), '; '), '')
                       ),
                       x -> x <> ''
                   ),
                   ' x '
               ) AS partes
        FROM turmas_base r
        JOIN parte        p  ON r.coddoc     = p.coddoc
        JOIN pessoa       cj ON cj.codpess   = p.codpess
        JOIN parteprocesso pp ON pp.codparte = p.codparte
        JOIN tipoparte    tp ON tp.codtipparte = pp.codtipparte
        GROUP BY r.coddoc
    """)

    # ── 13a: suspensao events ────────────────────────────────────────────────
    log("step 13a: suspensao_stg")
    mat("suspensao_stg", """
        SELECT m.coddoc, m.dthrmov, m.codfase, m.codcompl1, m.codcompl2, m.codcompl3
        FROM movimento m
        WHERE m.dthrmov <= '{v}'
          AND (
              (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
           OR (m.codfase IN (3, 20)          AND m.codcompl1 = 25)
           OR (m.codfase IN (3, 20, 29)      AND m.codcompl1 = 1013
               AND COALESCE(m.codcompl2, 0) = 1015)
           OR (m.codfase = 861               AND m.codcompl1 IN (690, 50068))
           OR (m.codfase = 861               AND m.codcompl1 IN (359,360,361,362,363,445,50069)
               AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
           OR (m.codfase = 50002             AND m.codcompl1 IN (306, 12065))
          )
    """)

    # ── 13b: reativacao events ───────────────────────────────────────────────
    log("step 13b: reativacao_stg")
    mat("reativacao_stg", """
        SELECT m.coddoc, m.dthrmov
        FROM movimento m
        WHERE m.dthrmov <= '{v}'
          AND (
              (m.codfase = 50002 AND m.codcompl1 = 12066)
           OR  m.codfase IN (893, 24, 193)
          )
    """)

    # ── 13c: repercussao (sobrestamento ativo) ───────────────────────────────
    log("step 13c: repercussao_stg")
    mat("repercussao_stg", """
        SELECT x.coddoc, x.dthrmov, x.codfase, x.codcompl1, x.codcompl2, x.codcompl3,
               x.dthrterminosobre
        FROM (
            SELECT m.coddoc, m.dthrmov, m.codfase, m.codcompl1, m.codcompl2, m.codcompl3,
                   CASE WHEN ms.dthrterminosobre >= '{v}' THEN NULL
                        ELSE ms.dthrterminosobre END AS dthrterminosobre
            FROM movimento m
            JOIN movsobrestamento ms ON ms.coddoc  = m.coddoc
                                    AND ms.dthrmov = m.dthrmov
            WHERE m.dthrmov <= '{v}'
              AND m.codfase  = 50040
        ) x
        WHERE dthrterminosobre IS NULL
    """)

    # ── 13d: susp_rep aggregated ─────────────────────────────────────────────
    log("step 13d: susp_rep_agg_stg")
    mat("susp_rep_agg_stg", """
        SELECT x.coddoc,
               MIN(x.dthrmov)                                  AS datasusp,
               MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END)  AS datarepercussao
        FROM (
            SELECT s.coddoc, s.dthrmov, 'S' AS src
            FROM suspensao_stg s
            LEFT JOIN reativacao_stg r ON r.coddoc  = s.coddoc
                                      AND r.dthrmov > s.dthrmov
            WHERE r.coddoc IS NULL
            UNION ALL
            SELECT r.coddoc, r.dthrmov, 'R' AS src
            FROM repercussao_stg r
        ) x
        GROUP BY x.coddoc
    """)

    # ── 13e: susp_rep_final with descriptions ────────────────────────────────
    log("step 13e: susp_rep_stg")
    mat("susp_rep_stg", """
        SELECT acer.coddoc,
               a.datasusp                                          AS dth_suspensao,
               f_susp.descr                                        AS movimento_suspensao,
               c1_susp.descr                                       AS comp1_ultima_suspensao,
               c2_susp.descr                                       AS comp2_ultima_suspensao,
               c3_susp.descr                                       AS comp3_ultima_suspensao,
               CASE WHEN a.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
               a.datarepercussao                                   AS dth_repercussao,
               a.datarepercussao                                   AS dth_sobrestamento,
               r.dthrterminosobre,
               f_rep.descr                                         AS movimento_sobrestamento,
               c1_rep.descr                                        AS comp1_ultimo_sobrestamento,
               c2_rep.descr                                        AS comp2_ultimo_sobrestamento,
               c3_rep.descr                                        AS comp3_ultimo_sobrestamento,
               CASE WHEN a.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestado
        FROM turmas_base acer
        LEFT JOIN susp_rep_agg_stg  a       ON acer.coddoc  = a.coddoc
        LEFT JOIN suspensao_stg     s       ON s.coddoc     = a.coddoc
                                           AND s.dthrmov    = a.datasusp
        LEFT JOIN fase              f_susp  ON s.codfase    = f_susp.codfase
        LEFT JOIN complemento_stg   c1_susp ON s.codcompl1  = c1_susp.codcompl
        LEFT JOIN complemento_stg   c2_susp ON s.codcompl2  = c2_susp.codcompl
        LEFT JOIN complemento_stg   c3_susp ON s.codcompl3  = c3_susp.codcompl
        LEFT JOIN repercussao_stg   r       ON r.coddoc     = a.coddoc
                                           AND r.dthrmov    = a.datarepercussao
        LEFT JOIN fase              f_rep   ON r.codfase    = f_rep.codfase
        LEFT JOIN complemento_stg   c1_rep  ON r.codcompl1  = c1_rep.codcompl
        LEFT JOIN complemento_stg   c2_rep  ON r.codcompl2  = c2_rep.codcompl
        LEFT JOIN complemento_stg   c3_rep  ON r.codcompl3  = c3_rep.codcompl
    """)

    # ── 14: decisao_final ────────────────────────────────────────────────────
    log("step 14: decisao_stg")
    mat("decisao_stg", """
        SELECT coddoc, dth_ultima_decisao, ultima_decisao,
               comp1_ultima_decisao, comp2_ultima_decisao, comp3_ultima_decisao,
               CASE WHEN dth_ultima_decisao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao
        FROM (
            SELECT m.coddoc,
                   m.dthrmov  AS dth_ultima_decisao,
                   f.descr    AS ultima_decisao,
                   c1.descr   AS comp1_ultima_decisao,
                   c2.descr   AS comp2_ultima_decisao,
                   c3.descr   AS comp3_ultima_decisao,
                   ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
            FROM movimento m
            LEFT JOIN fase f ON m.codfase = f.codfase
            LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
            LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
            LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
            WHERE m.codfase IN (3, 20)
              AND m.dthrmov <= '{v}'
        ) u WHERE rn = 1
    """)

    # ── 14a–14g: exclusion flags (acervo bruto → líquido) ────────────────────
    # Each produces a set of coddocs that match the exclusion condition.
    # Final SELECT converts to S/N flags and computes ind_acervo_liquido.

    log("step 14a: excl_fase_simples_stg")
    mat("excl_fase_simples_stg", """
        SELECT DISTINCT m.coddoc
        FROM movimento m
        JOIN turmas_base tb ON m.coddoc = tb.coddoc
        WHERE m.dthrmov <= '{v}'
          AND m.dthrmov >= tb.dataultdis
          AND m.codfase IN (861, 870, 22, 50011, 50014)
    """)

    log("step 14b: excl_findo_stg")
    mat("excl_findo_stg", """
        SELECT DISTINCT m.coddoc
        FROM movimento m
        JOIN turmas_base tb ON m.coddoc = tb.coddoc
        WHERE m.dthrmov <= '{v}'
          AND m.dthrmov >= tb.dataultdis
          AND m.codfase = 50002
          AND (
              m.codcompl1 IN (50117, 50118, 60435)
           OR COALESCE(m.codcompl2, 0) = 50118
          )
    """)

    log("step 14c: excl_expedicao_stg")
    mat("excl_expedicao_stg", """
        SELECT DISTINCT m.coddoc
        FROM movimento m
        JOIN turmas_base tb ON m.coddoc = tb.coddoc
        WHERE m.dthrmov <= '{v}'
          AND m.dthrmov >= tb.dataultdis
          AND m.codfase = 60
          AND m.codcompl1 = 50007
          AND COALESCE(m.codcompl2, 0) = 50119
    """)

    log("step 14d: excl_remessa_stg")
    mat("excl_remessa_stg", """
        SELECT DISTINCT m.coddoc
        FROM movimento m
        JOIN turmas_base tb ON m.coddoc = tb.coddoc
        WHERE m.dthrmov <= '{v}'
          AND m.dthrmov >= tb.dataultdis
          AND m.codfase IN (50123, 123)
          AND (
              m.codcompl1 IN (50104, 50648, 50105, 50649, 50723)
           OR COALESCE(m.codcompl2, 0) IN (50052, 50079, 50080)
          )
    """)

    log("step 14e: excl_remessa_esp_stg")
    mat("excl_remessa_esp_stg", """
        SELECT DISTINCT m.coddoc
        FROM movimento m
        JOIN turmas_base tb ON m.coddoc = tb.coddoc
        WHERE m.dthrmov <= '{v}'
          AND m.dthrmov >= tb.dataultdis
          AND m.codfase = 123
          AND m.codcompl1 = 66653
          AND COALESCE(m.codcompl2, 0) = 66652
    """)

    log("step 14f: excl_local_saida_stg")
    mat("excl_local_saida_stg", """
        SELECT DISTINCT mf.coddoc
        FROM movimentofisico mf
        JOIN turmas_base tb ON mf.coddoc = tb.coddoc
        WHERE mf.dthrmov <= '{v}'
          AND mf.dthrmov >= tb.dataultdis
          AND mf.codlocal IN (
              503,  515,  519,  520,  521,  549,
              583,  584,
              419,  420,  421,  422,  423,  424,  425,  426,  427,  428,  429,  430,
              1713, 1714, 1715, 1771,
              2085, 2594, 2593,
              3146, 3147, 3148, 3149, 3150, 3151,
              3205, 3206,
              3582, 3583, 3584, 3585,
              3875, 5765
          )
    """)

    log("step 14g: excl_suspensao_stg")
    mat("excl_suspensao_stg", """
        SELECT DISTINCT s.coddoc
        FROM (
            SELECT t.coddoc, t.dthrmov
            FROM (
                SELECT m.coddoc, m.dthrmov,
                       ROW_NUMBER() OVER (PARTITION BY m.coddoc
                                         ORDER BY m.dthrmov DESC) AS rn
                FROM movimento m
                JOIN turmas_base tb ON m.coddoc = tb.coddoc
                WHERE m.dthrmov <= '{v}'
                  AND m.dthrmov >= tb.dataultdis
                  AND m.codfase  IN (3, 11009)
                  AND m.codcompl1 IN (25, 11025)
            ) t WHERE t.rn = 1
        ) s
        LEFT JOIN (
            SELECT m.coddoc, m.dthrmov
            FROM movimento m
            WHERE m.dthrmov <= '{v}'
              AND m.codfase = 849
        ) r ON r.coddoc = s.coddoc AND r.dthrmov > s.dthrmov
        WHERE r.coddoc IS NULL
    """)

    # ── 15: final assembly — all intermediates are already persisted ──────────
    log("step 15: assembling relatorio_final")
    sql_final = """
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
            um1.comp3_ultimo_mov_cnj    AS comp3_ultimo_movimento_d01,
            -- exclusion flags (acervo bruto → líquido)
            CASE WHEN ef1.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_fase_simples,
            CASE WHEN ef2.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_findo,
            CASE WHEN ef3.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_expedicao,
            CASE WHEN ef4.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_remessa,
            CASE WHEN ef5.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_remessa_esp,
            CASE WHEN ef6.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_local_saida,
            CASE WHEN ef7.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_suspensao,
            CASE WHEN ef1.coddoc IS NULL
                  AND ef2.coddoc IS NULL
                  AND ef3.coddoc IS NULL
                  AND ef4.coddoc IS NULL
                  AND ef5.coddoc IS NULL
                  AND ef6.coddoc IS NULL
                  AND ef7.coddoc IS NULL
                 THEN 'S' ELSE 'N' END AS ind_acervo_liquido
        FROM relatorio_base_stg rb
        LEFT JOIN indicadores_stg        ip    ON rb.coddoc = ip.coddoc
        LEFT JOIN ult_mov_stg            um    ON rb.coddoc = um.coddoc
        LEFT JOIN ult_mov_cnj_stg        um1   ON rb.coddoc = um1.coddoc
        LEFT JOIN ult_julg_stg           uj    ON rb.coddoc = uj.coddoc
        LEFT JOIN local_virt_stg         lv    ON rb.coddoc = lv.coddoc
        LEFT JOIN assuntos_stg           asu   ON rb.coddoc = asu.coddoc
        LEFT JOIN vara_origem_stg        vo    ON rb.coddoc = vo.coddoc
        LEFT JOIN susp_rep_stg           usv   ON rb.coddoc = usv.coddoc
        LEFT JOIN prim_relator_stg       pr    ON rb.coddoc = pr.coddoc
        LEFT JOIN local_fisico_stg       lf    ON rb.coddoc = lf.coddoc
        LEFT JOIN parte_stg              parte ON rb.coddoc = parte.coddoc
        LEFT JOIN decisao_stg            ud    ON rb.coddoc = ud.coddoc
        LEFT JOIN excl_fase_simples_stg  ef1   ON rb.coddoc = ef1.coddoc
        LEFT JOIN excl_findo_stg         ef2   ON rb.coddoc = ef2.coddoc
        LEFT JOIN excl_expedicao_stg     ef3   ON rb.coddoc = ef3.coddoc
        LEFT JOIN excl_remessa_stg       ef4   ON rb.coddoc = ef4.coddoc
        LEFT JOIN excl_remessa_esp_stg   ef5   ON rb.coddoc = ef5.coddoc
        LEFT JOIN excl_local_saida_stg   ef6   ON rb.coddoc = ef6.coddoc
        LEFT JOIN excl_suspensao_stg     ef7   ON rb.coddoc = ef7.coddoc
    """
    return spark.sql(sql_final)
