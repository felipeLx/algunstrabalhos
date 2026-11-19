WITH cte_base AS (
    SELECT 
        UPPER(TRIM(usu.cpf_usu)) AS cpf_atribuido,
        pro.ind_sistema AS sistema,
        pro.cod_documento AS id_processo,
        pro.cod_cnj,
        mag.cpf AS cpf_relator,
        CAST(org.codlocalparametrizado AS BIGINT) AS cd_orgao,
        CAST(pro.cod_competencia AS BIGINT) AS cd_competencia,
        CAST(pro.cod_assunto_principal AS BIGINT) AS cd_assunto,
        CAST(pro.cod_classe_sistema AS BIGINT) AS cd_classe,
        CAST(ind.cod_movimento_ultimo AS BIGINT) AS cd_movimento,
        CAST(CASE WHEN ind.ind_suspenso = 'S' THEN 1 ELSE 0 END AS INT) AS in_suspenso,
        CAST(CASE WHEN ind.ind_conclusao = 'S' THEN 1 ELSE 0 END AS INT) AS in_concluso,
        CAST(CASE WHEN ind.ind_julgamento = 'S' THEN 1 ELSE 0 END AS INT) AS in_julgado,
        CAST(sit.dthrentr AS TIMESTAMP_NTZ) AS dt_atribuicao,
        CAST(ind.dth_movimento_ultimo AS TIMESTAMP_NTZ) AS dt_movimento,
        CAST(ind.dth_distribuicao_ultima AS TIMESTAMP_NTZ) AS dt_distribuicao,
        CAST(ind.dth_conclusao_ultima AS TIMESTAMP_NTZ) AS dt_conclusao,
        CAST(pro.dth_autuacao AS TIMESTAMP_NTZ) AS dt_autuacao,
        CAST(ind.dth_suspensao AS TIMESTAMP_NTZ) AS dt_suspensao,
        'EJUD' AS criado_por,
        'EJUD' AS atualizado_por,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao
    FROM dim_2inst_ouro.tb_dim_processo pro
    JOIN dw_apex.sigab_acervo ac 
        ON ac.id_processo = pro.cod_documento 
       AND ac.sistema = 'EJUD'
    JOIN ejud.documentosituacaovirtual sit 
        ON sit.coddoc = pro.cod_documento
    JOIN seg.usuario usu 
        ON usu.cod_usu = sit.idusubloq
    JOIN dw_2inst_ouro.tb_ft_acervo_indicadores ind 
        ON ind.cod_documento = pro.cod_documento
    JOIN dw_2inst_prata.tb_associacao_localfisico org 
        ON org.codlocalsistema = ind.cod_local_ultimo 
       AND org.sistema = ind.ind_sistema
    JOIN (
        SELECT cod_magistrado, cpf, ind_sistema
        FROM dw_2inst_prata.tb_associacao_magistrado
        GROUP BY cod_magistrado, cpf, ind_sistema
    ) mag 
        ON mag.cod_magistrado = ind.cod_magistrado_relator_ultimo 
       AND mag.ind_sistema = ind.ind_sistema
    WHERE pro.ind_sistema = 'EJUD'

    UNION ALL

    SELECT 
        UPPER(TRIM(
            CASE
                WHEN loc.sig_localizador LIKE '%10%' THEN '08457355783'
                WHEN loc.sig_localizador LIKE '%11%' THEN '08331604784'
                WHEN loc.sig_localizador LIKE '%12%' THEN '05509570733'
                WHEN loc.sig_localizador LIKE '%1%'  THEN '07742791765'
                WHEN loc.sig_localizador LIKE '%2%'  THEN '11490803700'
                WHEN loc.sig_localizador LIKE '%3%'  THEN '04145262794'
                WHEN loc.sig_localizador LIKE '%4%'  THEN '05523089788'
                WHEN loc.sig_localizador LIKE '%9%'  THEN '10206089783'
            END
        )) AS cpf_atribuido,
        pro.ind_sistema AS sistema,
        pro.cod_documento AS id_processo,
        pro.cod_cnj,
        mag.cpf AS cpf_relator,
        CAST(org.codlocalparametrizado AS BIGINT) AS cd_orgao,
        CAST(pro.cod_competencia AS BIGINT) AS cd_competencia,
        CAST(pro.cod_assunto_principal AS BIGINT) AS cd_assunto,
        CAST(pro.cod_classe_sistema AS BIGINT) AS cd_classe,
        CAST(ind.cod_movimento_ultimo AS BIGINT) AS cd_movimento,
        CAST(CASE WHEN ind.ind_suspenso = 'S' THEN 1 ELSE 0 END AS INT) AS in_suspenso,
        CAST(CASE WHEN ind.ind_conclusao = 'S' THEN 1 ELSE 0 END AS INT) AS in_concluso,
        CAST(CASE WHEN ind.ind_julgamento = 'S' THEN 1 ELSE 0 END AS INT) AS in_julgado,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atribuicao,
        CAST(ind.dth_movimento_ultimo AS TIMESTAMP_NTZ) AS dt_movimento,
        CAST(ind.dth_distribuicao_ultima AS TIMESTAMP_NTZ) AS dt_distribuicao,
        CAST(ind.dth_conclusao_ultima AS TIMESTAMP_NTZ) AS dt_conclusao,
        CAST(pro.dth_autuacao AS TIMESTAMP_NTZ) AS dt_autuacao,
        CAST(ind.dth_suspensao AS TIMESTAMP_NTZ) AS dt_suspensao,
        'EPROC2G' AS criado_por,
        'EPROC2G' AS atualizado_por,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao
    FROM dim_2inst_ouro.tb_dim_processo pro
    JOIN dw_apex.sigab_acervo sac 
        ON sac.id_processo = pro.cod_documento 
       AND sac.sistema = 'EPROC2G'
    JOIN dw_apex.tb_localizador_processo_2inst loc 
        ON loc.id_processo = sac.id_processo
    JOIN dw_2inst_ouro.tb_ft_acervo_indicadores ind 
        ON ind.cod_documento = pro.cod_documento
    JOIN dw_2inst_prata.tb_associacao_localfisico org 
        ON org.codlocalsistema = ind.cod_local_ultimo 
       AND org.sistema = ind.ind_sistema
    JOIN (
        SELECT cod_magistrado, cpf, ind_sistema
        FROM dw_2inst_prata.tb_associacao_magistrado
        GROUP BY cod_magistrado, cpf, ind_sistema
    ) mag 
        ON mag.cod_magistrado = ind.cod_magistrado_relator_ultimo 
       AND mag.ind_sistema = ind.ind_sistema
    WHERE pro.ind_sistema = 'EPROC2G'
)

SELECT 
    b.*
FROM cte_base b
LEFT JOIN dw_apex.sigab_atribuicao_hist s
    ON s.id_processo = b.id_processo
   AND s.cod_cnj = b.cod_cnj
   AND s.cpf_atribuido = b.cpf_atribuido
   AND s.dt_atribuicao = b.dt_atribuicao
WHERE s.id_processo IS NULL
  AND b.cpf_atribuido IS NOT NULL;