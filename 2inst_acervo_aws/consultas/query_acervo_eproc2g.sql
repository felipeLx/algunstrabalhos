WITH params AS (
    SELECT to_timestamp('{v_dateref_str}', 'yyyy-MM-dd HH:mm:ss') AS dth_ref
),

processo_referencia AS (
    SELECT 
        p.id_processo, 
        lpad(cast(p.num_processo as string), 20, '0') AS num_processo, 
        p.id_orgao_colegiado,
        prm.dth_ref AS dth_referencia
    FROM eproc2g.processo p
    CROSS JOIN params prm
),

ultima_distribuicao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento,
            ej.cod_mni_movimento,
            pe.id_processo_evento,
            ROW_NUMBER() OVER (
                PARTITION BY pr.id_processo
                ORDER BY pe.dth_evento DESC
            ) AS rn
        FROM eproc2g.processo_evento pe
        JOIN processo_referencia pr
            ON pe.id_processo = pr.id_processo
           AND pe.dth_evento <= pr.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE ej.cod_mni_movimento IN (26, 36)
    ) x
    WHERE rn = 1
),

ultimo_cancelamento_distribuicao AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.id_processo_evento,
            pe.dth_evento,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (
                PARTITION BY pr.id_processo
                ORDER BY pe.dth_evento DESC
            ) AS rn
        FROM eproc2g.processo_evento pe
        JOIN processo_referencia pr
            ON pe.id_processo = pr.id_processo
           AND pe.dth_evento <= pr.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE ej.cod_mni_movimento IN (488, 83)
    ) x
    WHERE rn = 1
),

ultima_distribuicao_valida AS (
    SELECT 
        ud.id_processo,
        ud.dth_evento AS dth_ultima_distribuicao,
        ud.cod_mni_movimento,
        ud.id_processo_evento
    FROM ultima_distribuicao ud
    LEFT JOIN ultimo_cancelamento_distribuicao ucd
        ON ud.id_processo = ucd.id_processo
       AND ud.dth_evento < ucd.dth_evento
    WHERE ucd.id_processo IS NULL
),

ultima_baixa AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento AS dth_ultima_baixa,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (
                PARTITION BY pr.id_processo
                ORDER BY pe.dth_evento DESC
            ) AS rn
        FROM eproc2g.processo_evento pe
        JOIN processo_referencia pr
            ON pe.id_processo = pr.id_processo
           AND pe.dth_evento <= pr.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
        WHERE ej.cod_mni_movimento = 22
    ) x
    WHERE rn = 1
),

ultimo_movimento AS (
    SELECT *
    FROM (
        SELECT
            pe.id_processo,
            pe.dth_evento AS dth_ultimo_movimento,
            ej.cod_mni_movimento,
            ROW_NUMBER() OVER (
                PARTITION BY pr.id_processo
                ORDER BY pe.dth_evento DESC
            ) AS rn
        FROM eproc2g.processo_evento pe
        JOIN processo_referencia pr
            ON pe.id_processo = pr.id_processo
           AND pe.dth_evento <= pr.dth_referencia
        JOIN eproc2g.evento_judicial ej
            ON pe.id_evento_judicial = ej.id_evento_judicial
        JOIN eproc2g.mni_movimento mm
            ON mm.cod_mni_movimento = ej.cod_mni_movimento
    ) x
    WHERE rn = 1
),

ultima_distribuicao_orgao AS (
    SELECT *
    FROM (
        SELECT 
            lp.id_processo,
            lp.id_orgao_colegiado,
            ROW_NUMBER() OVER (
                PARTITION BY lp.id_processo
                ORDER BY lp.dth_alteracao DESC
            ) AS rn
        FROM processo_referencia pr
        LEFT JOIN eproc2g.log_processo lp
            ON pr.id_processo = lp.id_processo
           AND lp.dth_alteracao <= pr.dth_referencia
           AND lp.id_orgao_colegiado IS NOT NULL
           AND lp.id_orgao_colegiado NOT IN (3583, 358301)
    ) x
    WHERE rn = 1
),

historico_distribuicao_valido AS (
    SELECT *
    FROM (
        SELECT
            hdj.id_processo,
            hdj.id_processo_evento,
            hdj.cod_magistrado,
            ROW_NUMBER() OVER (
                PARTITION BY hdj.id_processo, hdj.id_processo_evento
                ORDER BY hdj.id_processo_evento DESC
            ) AS rn
        FROM eproc2g.historico_distribuicao_judicial hdj
        JOIN processo_referencia pr
            ON hdj.id_processo = pr.id_processo
    ) x
    WHERE rn = 1
)

SELECT 
    pr.dth_referencia                                                              AS dth_referencia,
    pr.id_processo                                                                 AS id_processo,
    lpad(substring(pr.num_processo, 1, 7), 7, '0') || '-' ||
    lpad(substring(pr.num_processo, 8, 2), 2, '0') || '.' ||
    lpad(substring(pr.num_processo, 10, 4), 4, '0') || '.' ||
    lpad(substring(pr.num_processo, 14, 1), 1, '0') || '.' ||
    lpad(substring(pr.num_processo, 15, 2), 2, '0') || '.' ||
    lpad(substring(pr.num_processo, 17, 4), 4, '0')                                AS num_processo,
    ud.dth_ultima_distribuicao                                                     AS dth_distribuicao_ultima,
    hdj.cod_magistrado                                                             AS cod_relator_ultimo,
    coalesce(pr.id_orgao_colegiado,udo.id_orgao_colegiado, 0)                      AS cod_orgao_julgador_ultimo
FROM processo_referencia pr
JOIN ultima_distribuicao_valida ud
    ON pr.id_processo = ud.id_processo
LEFT JOIN ultima_baixa ub
    ON ud.id_processo = ub.id_processo
   AND ub.dth_ultima_baixa >= ud.dth_ultima_distribuicao
LEFT JOIN ultimo_movimento um
    ON ud.id_processo = um.id_processo
LEFT JOIN eproc2g.mni_movimento mm
    ON um.cod_mni_movimento = mm.cod_mni_movimento
JOIN historico_distribuicao_valido hdj
    ON ud.id_processo = hdj.id_processo
   AND ud.id_processo_evento = hdj.id_processo_evento
LEFT JOIN ultima_distribuicao_orgao udo
    ON pr.id_processo = udo.id_processo
WHERE ub.id_processo IS NULL
and coalesce(pr.id_orgao_colegiado,udo.id_orgao_colegiado, 0) NOT IN (3583, 358301);