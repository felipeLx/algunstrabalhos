WITH cte_base AS (
    SELECT DISTINCT
        CAST(loc.codlocal AS BIGINT) AS cd_orgao,
        UPPER(TRIM(org.nm_orgao)) AS ds_orgao,
        TRIM(usu.cpf_usu) AS cpf,
        TRIM(usu.matr_usu) AS matricula,
        UPPER(TRIM(usu.nome_usu)) AS nome,
        UPPER(TRIM(usu.cod_usu)) AS login,
        UPPER(TRIM(usu.cod_usu || '@TJRJ.JUS.BR')) AS email,
        CAST(1 AS INT) AS in_ativo,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM seg.usuario usu
    JOIN ejud.usuariolocalfisico loc 
        ON usu.cod_usu = loc.idusu
    JOIN dim_2inst_ouro.tb_dim_orgao org 
        ON org.cod_orgao = loc.codlocal
    LEFT JOIN ejud.documentosituacaovirtual sit_doc 
        ON sit_doc.idusubloq = usu.cod_usu
    WHERE loc.indlocalprinc = 'S'
      AND loc.codlocal IN (9183, 9093, 9141, 9194, 9187, 8480)
      AND (
            UPPER(TRIM(usu.cargo)) = 'MAGISTRADO'
            OR sit_doc.idusubloq IS NOT NULL
      )
),

cte_antigos AS (
    SELECT *
    FROM dw_apex.sigab_usuario_gabinete_hist
),

cte_merge AS (
    SELECT
        b.cd_orgao,
        b.ds_orgao,
        b.cpf,
        b.matricula,
        b.nome,
        b.login,
        b.email,
        CAST(1 AS INT) AS in_ativo,
        COALESCE(s.dt_criacao, b.dt_criacao) AS dt_criacao,
        CASE
            WHEN s.cpf IS NULL THEN b.dt_criacao
            WHEN NOT (s.matricula <=> b.matricula)
              OR NOT (s.nome <=> b.nome)
              OR NOT (s.login <=> b.login)
              OR NOT (s.email <=> b.email)
                THEN CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ)
            ELSE s.dt_atualizacao
        END AS dt_atualizacao
    FROM cte_base b
    LEFT JOIN cte_antigos s 
        ON s.cpf = b.cpf 
       AND s.cd_orgao = b.cd_orgao
    WHERE s.cpf IS NULL
       OR NOT (s.matricula <=> b.matricula)
       OR NOT (s.nome <=> b.nome)
       OR NOT (s.login <=> b.login)
       OR NOT (s.email <=> b.email)

    UNION ALL

    SELECT
        s.cd_orgao,
        s.ds_orgao,
        s.cpf,
        s.matricula,
        s.nome,
        s.login,
        s.email,
        CAST(0 AS INT) AS in_ativo,
        s.dt_criacao,
        CAST(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS TIMESTAMP_NTZ) AS dt_atualizacao
    FROM cte_antigos s
    LEFT JOIN cte_base b 
        ON b.cpf = s.cpf 
       AND b.cd_orgao = s.cd_orgao
    WHERE b.cpf IS NULL 
      AND s.in_ativo = 1
)

SELECT *
FROM cte_merge;