        WITH mov_relator AS (
        SELECT coddoc, dthrmov, codmagrel, codorgjulg, codlocal, rn
        FROM 
            (
            SELECT 
                mr.coddoc,
                mr.dthrmov,
                mr.codmagrel,
                mr.codorgjulg,
                oj.codlocal,
                ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn
            FROM ejud.movimentorelator mr
            JOIN ejud.orgaojulgador oj ON mr.codorgjulg = oj.codorgjulg
            JOIN ejud.competencia com ON com.codcompt = oj.codcompt
            WHERE dthrmov <= '{v_dateref_str}' 
            AND com.indconsrec = 'N'  
            ) sub
        WHERE rn = 1
        ), mov_distribuicao AS (
            SELECT coddoc, dthrmov, codfase
            FROM ejud.movdistribuicao
            WHERE DTHRMOV <= '{v_dateref_str}'
            AND codfase IN (26, 36, 60012)
        ), mov_distribuicao_cancelada AS (
            SELECT coddoc, dthrmov, dthrmovdistrcance
            FROM ejud.movcancedistr
            WHERE DTHRMOV <= '{v_dateref_str}'
        ),
        mov_distribuicao_valida AS (
            SELECT coddoc, datadistribuicao AS dthrmov FROM (
                SELECT 
                    md.coddoc,
                    md.dthrmov AS datadistribuicao,
                    ROW_NUMBER() OVER (PARTITION BY md.coddoc ORDER BY md.dthrmov desc) AS rn
                FROM mov_distribuicao md
                LEFT JOIN mov_distribuicao_cancelada mdc ON md.coddoc = mdc.coddoc AND md.dthrmov = mdc.dthrmovdistrcance
                WHERE mdc.dthrmovdistrcance IS NULL
            ) sub
            WHERE rn = 1
        ),
        mov_arquivamento AS (
        SELECT coddoc, codfase, dthrmov
            FROM (
                SELECT 
                    coddoc, 
                    dthrmov, 
                    codfase,
                    ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn                    
                FROM ejud.movimento
                WHERE DTHRMOV <= '{v_dateref_str}'
                AND (
                    (CODFASE = 22) OR
                    (CODFASE = 861 AND CODCOMPL1 = 246) OR 
                    (CODFASE IN (893,849)) 
                )
            ) sub
            WHERE rn = 1 AND codfase NOT IN (893,849)
        ), 	
        mov_findo AS (
        SELECT coddoc, dthrmov
            FROM (
                SELECT 
                    coddoc, 
                    dthrmov, 
                    codfase,
                    ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn 
                FROM ejud.movimento
                WHERE DTHRMOV <= '{v_dateref_str}'
                AND (codfase = 50002 AND (codcompl1 = 50118 or COALESCE(CodCompl2,0) = 50118) or CODFASE IN (893,849))
            ) sub
        WHERE rn = 1  AND CODFASE NOT IN (893,849) 
        ), 	
        mov_ae_mf_ra AS (
        SELECT coddoc, dthrmov
            FROM (
                SELECT 
                    coddoc, 
                    dthrmov, 
                    ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn
                FROM ejud.movimento
                WHERE codfase IN (50011, 50014, 870)
                AND dthrmov <= '{v_dateref_str}'
            ) sub
            WHERE rn = 1
        ),
        mov_local_saida AS ( 
        SELECT coddoc, dthrmov
        FROM (
        SELECT 
            mf.coddoc, 
            mf.dthrmov, 
            ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn 
        FROM ejud.movimentofisico mf
        JOIN mov_distribuicao_valida dv ON mf.coddoc = dv.coddoc AND mf.dthrmov >= dv.dthrmov
        WHERE mf.dthrmov <= '{v_dateref_str}'
        AND mf.codlocal IN (
                88, 503, 519, 520, 521, 549, 2017, 2085, 2275, 2593, 2594, 3144, 
                3146, 3147, 3148, 3149, 3150, 3151, 3205, 3206, 3582, 3583, 
                3584, 3585, 3763, 3875, 4580, 5765, 6919, 8972, 10682, 10683, 
                10684, 10685, 10686, 
                515, 2275)			   		    
            ) sub
        WHERE rn = 1 
        ), mov_remessa_saida AS (
        SELECT coddoc, dthrmov
        FROM (
            SELECT 
                m.coddoc, 
                m.dthrmov, 
                ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn 
            FROM ejud.movimento m
            JOIN mov_distribuicao_valida dv ON m.coddoc = dv.coddoc AND m.dthrmov >= dv.dthrmov
            JOIN ejud.movimentofisico mf ON m.coddoc = mf.coddoc AND m.dthrmov = mf.dthrmov
            WHERE (
                (m.codfase in (123,50123) AND mf.codlocal IN 
                (88, 503, 515, 519, 520, 521, 549, 2017, 2085, 2275, 2593, 2594, 
                3144, 3146, 3147, 3148, 3149, 3150, 3151, 3205, 3206, 3582, 3583, 
                3584, 3585, 3763, 3875, 4580, 5765, 6919, 8972, 10682, 10683, 
                10684, 10685, 10686))
                OR
                --Regra 7: Remessa a 50653-50105
                (m.codfase in (123,50123) AND mf.codlocal in (50653, 50655, 50656, 50104, 50105, 50648, 50649))
                OR
                --Regra 8: Remessa a Arquivo Central
                (m.codfase in (123,50123) AND mf.codlocal IN (515,2275))
                OR
                --Regra 9: Remessa a 50052,50078,50079,50080
                (m.codfase in (123,50123) and coalesce(m.CodCompl2,0) in (50052,50078,50079,50080))
                )
            AND m.dthrmov <= '{v_dateref_str}'
            ) sub
            WHERE rn = 1 
        ), mov_expedicao AS (
        SELECT coddoc, dthrmov
            FROM (
                SELECT 
                    coddoc, 
                    dthrmov, 
                    ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn 
                FROM ejud.movimento
                WHERE (
                    (CodFase = 60 and CodCompl1 = 50007 and coalesce(CodCompl2,0) in (50119,66356))
                OR 
                    (CodFase = 60 and CodCompl1 = 66103 and coalesce(CodCompl2,0) = 296)
                )
                AND dthrmov <= '{v_dateref_str}'
            ) sub
            WHERE rn = 1  
        ), movimentos_unidos AS (
        SELECT coddoc, dthrmov FROM mov_local_saida
        UNION ALL
        SELECT coddoc, dthrmov FROM mov_remessa_saida
        UNION ALL
        SELECT coddoc, dthrmov FROM mov_expedicao
        ),
        mov_saida_fisico AS ( 
        SELECT coddoc, dthrmov 
            FROM (
                SELECT 
                    coddoc, 
                    dthrmov, 
                    ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) AS rn 
                FROM movimentos_unidos
            )  sub
            WHERE rn = 1
        ),
        mov_retorno_fisico AS ( 
        SELECT coddoc, dthrmov, codlocal
        FROM (
            SELECT 
                mr.coddoc, 
                mf.dthrmov, 
                mf.codlocal,
                ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn 
            FROM mov_relator mr
            JOIN ejud.movimentofisico mf 
            ON mr.codlocal = mf.codlocal AND mf.coddoc = mr.coddoc
            WHERE mf.dthrmov <= '{v_dateref_str}'
            ) sub WHERE rn = 1
        ),
        mov_nao_retornado AS ( 
        SELECT CODDOC, dthrsaida, dthrretorno
        FROM (
            SELECT 
                msf.coddoc
                ,msf.dthrmov AS dthrsaida 
                ,mrf.dthrmov AS dthrretorno
            FROM mov_saida_fisico msf
            LEFT JOIN mov_retorno_fisico mrf ON msf.coddoc = mrf.coddoc AND mrf.dthrmov > msf.dthrmov
            where mrf.coddoc IS null
            ) sub
        )
        SELECT 
            '{v_dateref_str}'::timestamp                                    AS dth_referencia
            ,mov_relator.coddoc::numeric(30,0)                              AS cod_documento
            ,mov_distribuicao_valida.dthrmov::timestamp                     AS dth_ultima_distribuicao      
            ,mov_relator.codmagrel::int                                     AS cod_ultimo_relator
            ,mov_relator.codorgjulg::int                                    AS cod_ultimo_orgao_julgador 
        FROM mov_relator
        JOIN mov_distribuicao_valida
            ON mov_relator.coddoc = mov_distribuicao_valida.coddoc
        LEFT JOIN mov_arquivamento 
            ON mov_distribuicao_valida.coddoc = mov_arquivamento.coddoc 
            AND mov_arquivamento.DTHRMOV >= mov_distribuicao_valida.DTHRMOV
        LEFT JOIN mov_findo
            ON mov_distribuicao_valida.coddoc = mov_findo.coddoc 
            AND mov_findo.DTHRMOV >= mov_distribuicao_valida.DTHRMOV
        LEFT JOIN mov_ae_mf_ra
            ON mov_distribuicao_valida.coddoc = mov_ae_mf_ra.coddoc 
            AND mov_ae_mf_ra.DTHRMOV >= mov_distribuicao_valida.DTHRMOV
        LEFT JOIN mov_nao_retornado 
            ON mov_distribuicao_valida.coddoc = mov_nao_retornado.coddoc
            AND mov_nao_retornado.DTHRSAIDA >= mov_distribuicao_valida.DTHRMOV
        WHERE mov_arquivamento.coddoc IS NULL 
        AND mov_ae_mf_ra.CODDOC IS NULL
        AND mov_findo.CODDOC IS NULL
        and mov_nao_retornado.CODDOC IS NULL;