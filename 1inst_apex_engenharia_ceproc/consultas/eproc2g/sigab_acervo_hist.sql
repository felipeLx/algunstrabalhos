WITH cte_base AS (
    SELECT
        pro.ind_sistema AS sistema,
        pro.cod_documento AS id_processo,
        pro.cod_cnj,
        mag.cpf AS cpf_relator,
        CAST(org.codlocalparametrizado AS BIGINT) AS cd_orgao,
        CAST(pro.cod_competencia AS BIGINT) AS cd_competencia,
        CAST(
            CASE
                WHEN pro.ind_sistema = 'EJUD' THEN pro.cod_assunto_principal
                WHEN pro.ind_sistema = 'EPROC2G' THEN ass.id_assunto_principal
            END AS BIGINT
        ) AS cd_assunto,
        CAST(pro.cod_classe_sistema AS BIGINT) AS cd_classe,
        CAST(ind.cod_movimento_ultimo AS BIGINT) AS cd_movimento,
        CAST(ind.cod_movimento_ultimo_cnj AS BIGINT) AS cd_movimento_cnj,
        CAST(CASE WHEN ind.ind_suspenso = 'S' THEN 1 ELSE 0 END AS INT) AS in_suspenso,
        CAST(CASE WHEN ind.ind_conclusao = 'S' THEN 1 ELSE 0 END AS INT) AS in_concluso,
        CAST(CASE WHEN ind.ind_julgamento = 'S' THEN 1 ELSE 0 END AS INT) AS in_julgado,
        CAST(1 AS INT) AS in_acervo,
        CAST(ind.dth_movimento_ultimo AS TIMESTAMP_NTZ) AS dt_movimento,
        CAST(ind.dth_movimento_ultimo_cnj AS TIMESTAMP_NTZ) AS dt_movimento_cnj,
        CAST(ind.dth_distribuicao_ultima AS TIMESTAMP_NTZ) AS dt_distribuicao,
        CAST(ind.dth_conclusao_ultima AS TIMESTAMP_NTZ) AS dt_conclusao,
        CAST(pro.dth_autuacao AS TIMESTAMP_NTZ) AS dt_autuacao,
        CAST(ind.dth_suspensao AS TIMESTAMP_NTZ) AS dt_suspensao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM dim_2inst_ouro.tb_dim_processo pro
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
    LEFT JOIN eproc2g.processo ass 
        ON ass.id_processo = pro.cod_documento 
       AND pro.ind_sistema = 'EPROC2G'
    WHERE org.codlocalparametrizado = 9187
),

cte_antigos AS (
    SELECT *
    FROM dw_apex.sigab_acervo_hist
),

cte_merge AS (
    SELECT
        b.sistema,
        b.id_processo,
        b.cod_cnj,
        b.cpf_relator,
        b.cd_orgao,
        b.cd_competencia,
        b.cd_assunto,
        b.cd_classe,
        b.cd_movimento,
        b.cd_movimento_cnj,
        b.in_suspenso,
        b.in_concluso,
        b.in_julgado,
        b.in_acervo,
        b.dt_movimento,
        b.dt_movimento_cnj,
        b.dt_distribuicao,
        b.dt_conclusao,
        b.dt_autuacao,
        b.dt_suspensao,
        COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
        CASE
            WHEN s.id_processo IS NULL THEN b.dt_criacao
            WHEN NOT (s.cd_orgao <=> b.cd_orgao)
              OR NOT (s.cpf_relator <=> b.cpf_relator)
              OR NOT (s.cd_competencia <=> b.cd_competencia)
              OR NOT (s.cd_assunto <=> b.cd_assunto)
              OR NOT (s.cd_classe <=> b.cd_classe)
              OR NOT (s.cd_movimento <=> b.cd_movimento)
              OR NOT (s.cd_movimento_cnj <=> b.cd_movimento_cnj)
              OR NOT (s.in_suspenso <=> b.in_suspenso)
              OR NOT (s.in_concluso <=> b.in_concluso)
              OR NOT (s.in_julgado <=> b.in_julgado)
              OR NOT (s.dt_movimento <=> b.dt_movimento)
              OR NOT (s.dt_movimento_cnj <=> b.dt_movimento_cnj)
              OR NOT (s.dt_distribuicao <=> b.dt_distribuicao)
              OR NOT (s.dt_conclusao <=> b.dt_conclusao)
              OR NOT (s.dt_autuacao <=> b.dt_autuacao)
              OR NOT (s.dt_suspensao <=> b.dt_suspensao)
                THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
            ELSE s.dt_atualizacao
        END AS dt_atualizacao
    FROM cte_base b
    LEFT JOIN cte_antigos s 
        ON s.id_processo = b.id_processo 
       AND s.cod_cnj = b.cod_cnj
    WHERE s.id_processo IS NULL
       OR NOT (s.cd_orgao <=> b.cd_orgao)
       OR NOT (s.cpf_relator <=> b.cpf_relator)
       OR NOT (s.cd_competencia <=> b.cd_competencia)
       OR NOT (s.cd_assunto <=> b.cd_assunto)
       OR NOT (s.cd_classe <=> b.cd_classe)
       OR NOT (s.cd_movimento <=> b.cd_movimento)
       OR NOT (s.cd_movimento_cnj <=> b.cd_movimento_cnj)
       OR NOT (s.in_suspenso <=> b.in_suspenso)
       OR NOT (s.in_concluso <=> b.in_concluso)
       OR NOT (s.in_julgado <=> b.in_julgado)
       OR NOT (s.dt_movimento <=> b.dt_movimento)
       OR NOT (s.dt_movimento_cnj <=> b.dt_movimento_cnj)
       OR NOT (s.dt_distribuicao <=> b.dt_distribuicao)
       OR NOT (s.dt_conclusao <=> b.dt_conclusao)
       OR NOT (s.dt_autuacao <=> b.dt_autuacao)
       OR NOT (s.dt_suspensao <=> b.dt_suspensao)

    UNION ALL

    SELECT
        s.sistema,
        s.id_processo,
        s.cod_cnj,
        s.cpf_relator,
        s.cd_orgao,
        s.cd_competencia,
        s.cd_assunto,
        s.cd_classe,
        s.cd_movimento,
        s.cd_movimento_cnj,
        s.in_suspenso,
        s.in_concluso,
        s.in_julgado,
        CAST(0 AS INT) AS in_acervo,
        s.dt_movimento,
        s.dt_movimento_cnj,
        s.dt_distribuicao,
        s.dt_conclusao,
        s.dt_autuacao,
        s.dt_suspensao,
        s.dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM cte_antigos s
    LEFT JOIN cte_base b 
        ON b.id_processo = s.id_processo 
       AND b.cod_cnj = s.cod_cnj
    WHERE b.id_processo IS NULL 
      AND s.in_acervo = 1
)

SELECT *
FROM cte_merge;