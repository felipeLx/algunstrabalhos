WITH processo_tratado as (
    SELECT DISTINCT
        CAST(COD_DOCUMENTO AS DECIMAL(30,0))        AS cod_documento,
        CAST(COD_CNJ_SIGILO AS VARCHAR(255))        AS cod_cnj,
        CAST(IND_SISTEMA AS VARCHAR(255))           AS ind_sistema,
        CAST(NM_CLASSE AS VARCHAR(255))             AS nm_classe,
        CAST(IND_ORIGINARIO AS VARCHAR(255))        AS ind_originario,
        CAST(IND_PRIORIDADE_IDOSO AS VARCHAR(255))  AS ind_prioridade_idoso,
        CAST(IND_PRIORIDADE_IDOSO_80 AS VARCHAR(255)) AS ind_prioridade_idoso_80,
        CAST(IND_DOENCA_GRAVE AS VARCHAR(255))      AS ind_doenca_grave,
        CAST(IND_PRIORIDADE_DEFICIENTE AS VARCHAR(255)) AS ind_prioridade_deficiente,
        CAST(COD_TIPO_PROCESSO AS INT)              AS cod_classe_sistema,
        CAST(IND_CRIME_ADM_PUBLICO AS VARCHAR(255)) AS ind_crime_adm_publico
    FROM dim_2inst_ouro.tb_dim_processo_ejud
    UNION ALL
    SELECT DISTINCT
        CAST(ID_PROCESSO AS DECIMAL(30,0))          AS cod_documento,
        CAST(COD_CNJ_SIGILO AS VARCHAR(255))        AS cod_cnj,
        CAST(IND_SISTEMA AS VARCHAR(255))           AS ind_sistema,
        CAST(DES_CLASSE AS VARCHAR(255))            AS nm_classe,
        CAST(IND_ORIGINARIO AS VARCHAR(255))        AS ind_originario,
        CAST(IDOSOS AS VARCHAR(255))                AS ind_prioridade_idoso,
        CAST(IDOSOS_80_MAIS AS VARCHAR(255))        AS ind_prioridade_idoso_80,
        CAST(DOENCA_GRAVE AS VARCHAR(255))          AS ind_doenca_grave,
        CAST(PRIORIDADE_DEFICIENTE AS VARCHAR(255)) AS ind_prioridade_deficiente,
        CAST(ID_CLASSE_JUDICIAL AS INT)             AS cod_classe_sistema,
        CAST(IND_CRIME_ADM_PUBLICO AS VARCHAR(255)) AS ind_crime_adm_publico
    FROM dim_2inst_ouro.tb_dim_processo_eproc2g
), processo_ultima_ocorrencia AS (
    SELECT *
    FROM (
        SELECT 
            pdc.id_processo,
            dc.nome AS nome_campo,
            dcv.dado_comp_valor,
            pdc.id_dado_comp_valor,
            dcv.sin_exibe_rotulo,
            ROW_NUMBER() OVER (PARTITION BY pdc.id_processo, dc.nome ORDER BY pdc.dth_inclusao DESC) AS rn
        FROM eproc2g.processo_dado_complementar pdc
        JOIN eproc2g.dado_complementar_valor dcv
            ON pdc.id_dado_complementar = dcv.id_dado_complementar
            AND pdc.id_dado_comp_valor = dcv.id_dado_comp_valor
        JOIN eproc2g.dado_complementar dc
            ON dcv.id_dado_complementar = dc.id_dado_complementar
        WHERE dc.nome IN ('ANTECIPACAO_DE_TUTELA') and pdc.sin_ativo = 'S'
    ) x
    WHERE rn = 1
), indicadores_processo_eproc2g as (
    SELECT
        u.id_processo,
        MAX(CASE WHEN u.nome_campo = 'ANTECIPACAO_DE_TUTELA' AND u.rn = 1 and u.id_dado_comp_valor = 1 THEN 'S' ELSE 'N' END) AS IND_TUTELA_ANTECIPADA
    FROM processo_ultima_ocorrencia u
    WHERE u.rn = 1
    GROUP BY u.id_processo
), indicadores_acervo as (
	select 
		DTH_REFERENCIA
		,'DIARIO' as IND_REFERENCIA
		,COD_DOCUMENTO
		,DTH_DISTRIBUICAO_ULTIMA
		,COD_MAGISTRADO_RELATOR_ULTIMO
		,COD_ORGAO_JULGADOR_ULTIMO
		,DTH_MOVIMENTO_ULTIMO as DTH_ULTIMO_MOVIMENTO
    	,DTH_MOVIMENTO_ULTIMO_CNJ
		,COD_MOVIMENTO_ULTIMO
		,IND_SUSPENSO
		,DTH_JULGAMENTO
		,IND_JULGAMENTO
		,DTH_LOCAL_ULTIMO
		,COD_LOCAL_ULTIMO
        ,DTH_LOCAL_VIRTUAL_ULTIMO
        ,COD_LOCAL_VIRTUAL_ULTIMO
        ,dth_localizador_ultimo 
        ,cod_localizador_ultimo 
		,DTH_CONCLUSAO_ULTIMA
		,IND_CONCLUSAO
		,IND_SISTEMA
		,DTH_ETL
        ,ind_diligencia
        ,ind_decisao
        ,cast(dth_ultima_conclusao_relator as timestamp) as dth_ultima_conclusao_relator
        ,cast(ind_conclusao_relator as varchar(1)) as ind_conclusao_relator
        ,cast(cod_movimento_ultimo_cnj as int) as cod_movimento_ultimo_cnj
	from dw_2inst_ouro.tb_ft_acervo_indicadores
	union all
	select 	
		dth_referencia as DTH_REFERENCIA
		,'ANO_PASSADO' as IND_REFERENCIA
		,CAST(id_processo AS DECIMAL(30,0)) as COD_DOCUMENTO
		,dth_distribuicao_ultima as DTH_DISTRIBUICAO_ULTIMA
		,CAST(cod_relator_ultimo AS INT) as COD_MAGISTRADO_RELATOR_ULTIMO
		,CAST(cod_orgao_julgador_ultimo AS INT) as COD_ORGAO_JULGADOR_ULTIMO
		,dth_ultimo_movimento as DTH_ULTIMO_MOVIMENTO
        ,DTH_MOVIMENTO_ULTIMO_CNJ
		,CAST(id_evento_judicial_ultimo AS INT) as COD_MOVIMENTO_ULTIMO
		,CAST(ind_suspenso AS VARCHAR(1)) as IND_SUSPENSO
		,dth_julgamento as DTH_JULGAMENTO
		,CAST(ind_julgamento AS VARCHAR(1)) as IND_JULGAMENTO
		,dth_ultimo_local as DTH_LOCAL_ULTIMO
		,CAST(cod_ultimo_local AS INT) as COD_LOCAL_ULTIMO
        ,cast(null as timestamp) as DTH_LOCAL_VIRTUAL_ULTIMO
        ,cast(null as int) as COD_LOCAL_VIRTUAL_ULTIMO
        ,dth_entrada_localizador as dth_localizador_ultimo
        ,cod_ultimo_localizador as cod_localizador_ultimo
		,dth_ultima_conclusao as DTH_CONCLUSAO_ULTIMA
		,CAST(ind_conclusao AS VARCHAR(1)) as IND_CONCLUSAO
		,CAST(ind_sistema AS VARCHAR(10)) as IND_SISTEMA
		,CAST(dth_etl AS TIMESTAMP) as DTH_ETL
        ,ind_diligencia
        ,ind_decisao
        ,cast(null as timestamp) as dth_ultima_conclusao_relator
        ,cast(null as varchar(1)) as ind_conclusao_relator 
        ,cast(id_evento_judicial_ultimo_cnj as int) as cod_movimento_ultimo_cnj
	FROM dw_2inst_prata.tb_acervo_hist_indicadores_eproc2g 
	where CAST(dth_referencia AS TIMESTAMP) = 
	      (date_trunc('day', current_timestamp - interval '1' year) - interval '1' day)
	      + interval '23' hour + interval '59' minute + interval '59' second
	union all 
	SELECT
		dth_referencia as DTH_REFERENCIA
		,'ANO_PASSADO' as IND_REFERENCIA
		,CAST(CODDOC AS DECIMAL(30,0)) as COD_DOCUMENTO	
		,dth_distribuicao_ultima as DTH_DISTRIBUICAO_ULTIMA
		,CAST(cod_magistrado_relator_ultimo AS INT) as COD_MAGISTRADO_RELATOR_ULTIMO
		,CAST(cod_orgao_julgador_ultimo AS INT) as COD_ORGAO_JULGADOR_ULTIMO
		,dth_movimento_ultimo as DTH_ULTIMO_MOVIMENTO
        ,DTH_MOVIMENTO_ULTIMO_CNJ
		,CAST(cod_fase_ultimo AS INT) as COD_MOVIMENTO_ULTIMO
		,CAST(ind_suspenso AS VARCHAR(1)) as IND_SUSPENSO
		,dth_julgamento as DTH_JULGAMENTO
		,CAST(ind_julgamento AS VARCHAR(1)) as IND_JULGAMENTO
		,dth_ultimo_local as DTH_LOCAL_ULTIMO
		,CAST(cod_ultimo_local AS INT) as COD_LOCAL_ULTIMO
        ,cast(DTH_ULTIMO_LOCAL_VIRTUAL as timestamp) as DTH_LOCAL_VIRTUAL_ULTIMO
        ,cast(COD_ULTIMO_LOCAL_VIRTUAL as int) as COD_LOCAL_VIRTUAL_ULTIMO
        ,cast(null as timestamp) as dth_localizador_ultimo 
        ,cast(null as int) as cod_localizador_ultimo
		,dth_ultima_conclusao as DTH_CONCLUSAO_ULTIMA
		,CAST(ind_conclusao AS VARCHAR(1)) as IND_CONCLUSAO
		,CAST(ind_sistema AS VARCHAR(10)) as IND_SISTEMA
		,CAST(dth_etl AS TIMESTAMP) as DTH_ETL
        ,cast(null as int) as ind_diligencia
        ,ind_decisao
        ,cast(dth_ultima_conclusao_relator as timestamp) as dth_ultima_conclusao_relator
        ,cast(ind_conclusao_relator as varchar(1)) as ind_conclusao_relator 
        ,cast(cod_fase_ultimo_cnj as int) as cod_movimento_ultimo_cnj
	FROM dw_2inst_prata.tb_acervo_hist_indicadores_ejud
	where CAST(dth_referencia AS TIMESTAMP) = 
	      (date_trunc('day', current_timestamp - interval '1' year) - interval '1' day)
	      + interval '23' hour + interval '59' minute + interval '59' second
), base_dados AS (
    SELECT
        A.DTH_REFERENCIA,
        A.IND_REFERENCIA,
        A.COD_DOCUMENTO,
        p.COD_CNJ,
        p.NM_CLASSE,
        A.DTH_DISTRIBUICAO_ULTIMA,
        A.DTH_ULTIMO_MOVIMENTO,
        dm.NM_MOVIMENTO AS ULTIMO_MOVIMENTO,
        A.COD_LOCAL_ULTIMO,
        alo.localfisico,
        alo.ind_secretaria,
        alo.ind_localexterno,
        A.COD_MAGISTRADO_RELATOR_ULTIMO as cod_relator,
        dmag.CPF,
        dmag.nome,
        ao.codorg_novo as cod_orgao_julgador_ultimo,
        do.nm_orgao as orgao_julgador_ultimo,
        A.IND_SUSPENSO,
        A.IND_JULGAMENTO,
        A.IND_CONCLUSAO,
        A.DTH_CONCLUSAO_ULTIMA,
        datediff(A.DTH_REFERENCIA, A.DTH_CONCLUSAO_ULTIMA) AS DIAS_CONCLUSO,
        A.ind_sistema,
        A.DTH_MOVIMENTO_ULTIMO_CNJ,
        dmcnj.nm_movimento as ULTIMO_MOVIMENTO_CNJ,
        p.ind_originario,
        p.ind_prioridade_idoso,
        p.ind_prioridade_idoso_80,
        p.ind_doenca_grave,
        p.ind_prioridade_deficiente,
        p.cod_classe_sistema,
        p.ind_crime_adm_publico,
        A.ind_diligencia,
        A.ind_decisao,
        A.dth_ultima_conclusao_relator,
        A.ind_conclusao_relator,
        pe.IND_PEDIDO_LIMINAR,
        pe.IND_REU_PRESO,
        ip.IND_TUTELA_ANTECIPADA,
        alo.ind_secretaria
    FROM indicadores_acervo A
    LEFT JOIN dw_2inst_prata.tb_associacao_magistrado ram ON A.COD_MAGISTRADO_RELATOR_ULTIMO = ram.cod_magistrado
    LEFT JOIN dim_2inst_ouro.tb_dim_mag_cpf dmag ON ram.cpf = dmag.cpf
    JOIN processo_tratado p ON A.cod_documento = p.cod_documento AND A.ind_sistema = p.ind_sistema
    LEFT JOIN dw_2inst_prata.tb_associacao_local_orgao alo ON A.cod_local_ultimo = alo.codlocal AND A.ind_sistema = alo.ind_sistema
    LEFT JOIN dw_2inst_prata.tb_associacao_orgao ao ON A.cod_orgao_julgador_ultimo = ao.codorg_antigo
    LEFT JOIN dim_2inst_ouro.tb_dim_orgao do ON ao.codorg_novo = do.cod_orgao
    LEFT JOIN dim_2inst_ouro.tb_dim_movimento dm ON A.COD_MOVIMENTO_ULTIMO = dm.COD_MOVIMENTO AND A.IND_SISTEMA = dm.IND_SISTEMA
    LEFT JOIN dim_2inst_ouro.tb_dim_movimento dmcnj ON A.cod_movimento_ultimo_cnj = dmcnj.COD_MOVIMENTO AND A.IND_SISTEMA = dmcnj.IND_SISTEMA
    LEFT JOIN dim_2inst_ouro.tb_dim_processo_ejud pe ON CAST(pe.cod_documento AS VARCHAR(255)) = CAST(A.cod_documento AS VARCHAR(255)) AND A.IND_SISTEMA = pe.IND_SISTEMA
    LEFT JOIN indicadores_processo_eproc2g ip ON CAST(A.cod_documento AS VARCHAR(255)) = CAST(ip.id_processo AS VARCHAR(255))
), item_10_1 AS (
    --Total de processos em tramitação na unidade, incluindo suspensos, sobrestados e arquivados provisoriamente (acervo total)
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        1 as SUBITEM,
        5 as ITEM_V2,
        1 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
), item_10_2 AS (
    --Total de processos suspensos, sobrestados ou arquivados provisoriamente
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        2 as SUBITEM,
        5 as ITEM_V2,
        2 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS SUSPENSOS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_SUSPENSO = 'S'
), item_10_3 AS (
    --Total de processos em tramitação, excluídos suspensos, sobrestados ou arquivados provisoriamente (acervo ajustado)
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        3 as SUBITEM,
        5 as ITEM_V2,
        3 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS EXCLUINDO SUSPENSOS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_SUSPENSO = 'N'
), item_10_4 AS (
    --Total de processos originários em tramitação sem decisão de mérito
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        4 as SUBITEM,
        cast(null as int) as ITEM_V2,
        cast(null as int) as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS ORIGINARIOS SEM DECISAO DE MERITO' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_SUSPENSO = 'N' AND IND_JULGAMENTO = 'N' AND ind_originario = 'S'
), item_10_5 AS (
    --Total de recursos em tramitação sem decisão de mérito
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        5 as SUBITEM,
        cast(null as int) as ITEM_V2,
        cast(null as int) as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS SEM DECISAO DE MERITO' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_SUSPENSO = 'N' AND IND_JULGAMENTO = 'N' AND ind_originario = 'N'
), item_10_6 AS (
    --Total de processos conclusos para o magistrado
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        6 as SUBITEM,
        5 as ITEM_V2,
        4 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS CONCLUSOS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_CONCLUSAO = 'S' 
), item_10_7 AS (
    --	Total de processos conclusos para o magistrado há mais de 120 dias
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        7 as SUBITEM,
        5 as ITEM_V2,
        5 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS CONCLUSOS HÁ MAIS DE 120 DIAS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_CONCLUSAO = 'S' AND DTH_CONCLUSAO_ULTIMA < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' DAY
), item_10_8 AS (
    --	Total de processos em secretaria
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        8 as SUBITEM,
        5 as ITEM_V2,
        6 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS EM SECRETARIA' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE ind_secretaria = 'S'
), item_10_9 AS (
    --	Total de processos paralisados em secretaria há mais de 102 dias
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        9 as SUBITEM,
        5 as ITEM_V2,
        7 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS EM SECRETARIA HÁ MAIS DE 120 DIAS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE DTH_MOVIMENTO_ULTIMO_CNJ < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' day 
    AND ind_secretaria = 'S'
    AND IND_SUSPENSO = 'N'
), item_10_10 AS (
    --Total de processos em diligência na origem
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        10 as SUBITEM,
        5 as ITEM_V2,
        10 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS EM DILIGENCIA NA ORIGEM' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE (ind_sistema = 'EJUD' AND ind_localexterno = 'S') OR (ind_sistema = 'EPROC2G' AND ind_diligencia = 'S')
), item_10_12 AS (
    --Total de processos com prioridade legal conclusos há mais de 100 dias
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo, 
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        10 as ITEM,
        12 as SUBITEM,
        5 as ITEM_V2,
        8 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS COM PRIORIDADE LEGAL CONCLUSOS HÁ MAIS DE 120 DIAS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE IND_CONCLUSAO = 'S'
    AND (ind_prioridade_idoso = 'S' OR ind_prioridade_idoso_80 = 'S' OR ind_doenca_grave = 'S' OR ind_prioridade_deficiente = 'S')
    AND DTH_CONCLUSAO_ULTIMA < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' day 
), item_13_1 as (
    --Total de processos  PAD em tramitação na unidade, incluindo suspensos, sobrestados e  arquivados provisoriamente (acervo total)
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        13 as ITEM,
        1 as SUBITEM,
        7 as ITEM_V2,
        1 as SUBITEM_V2,
        'ACERVO_PAD' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE cod_classe_sistema IN (101, 118, 221) AND ind_sistema = 'EJUD'
), item_13_6 as (
    --total de Processos Administrativos Disciplinares em tramitação no gabinete conclusos
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        13 as ITEM,
        6 as SUBITEM,
        7 as ITEM_V2,
        5 as SUBITEM_V2,
        'CONCLUSOS_PAD' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE cod_classe_sistema IN (101, 118, 221) AND ind_sistema = 'EJUD' AND IND_CONCLUSAO = 'S'
), item_13_7 as (
    --Total de processos Processos Administrativos Disciplinares paralisados há mais de 120 dias
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        13 as ITEM,
        7 as SUBITEM,
        7 as ITEM_V2,
        6 as SUBITEM_V2,
        'PARALISADOS_PAD' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM base_dados
    WHERE cod_classe_sistema IN (101, 118, 221) AND ind_sistema = 'EJUD' AND DTH_MOVIMENTO_ULTIMO_CNJ < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' day
), item_14 as (
    ----Preencha o quadro a seguir com os 10 processos de relatoria do desembargor paralisados há mais tempo, independentemente de sua localização atual:
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO,
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        LOCALFISICO,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        14 as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        1 as SUBITEM_V2,
        'AMOSTRA_PROCESSUAL_PARALISADOS_ULTIMO_MOVIMENTO' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cpf, ind_referencia ORDER BY (datediff(DTH_REFERENCIA, DTH_MOVIMENTO_ULTIMO_CNJ)) DESC) as rn
        FROM base_dados
        WHERE ind_suspenso = 'N'
    ) t
    WHERE rn <= 10    
), item_15 as (
     --Preencha o quadro a seguir com os 10 processos de relatoria do desembargor paralisados há mais tempo, independentemente de sua localização atual:
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO,
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        LOCALFISICO,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        15 as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        2 as SUBITEM_V2,
        'AMOSTRA_PROCESSUAL_PARALISADOS_ULTIMA_DISTRIBUICAO' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cpf, ind_referencia ORDER BY (datediff(DTH_REFERENCIA, DTH_DISTRIBUICAO_ULTIMA)) DESC) as rn
        FROM base_dados
    ) t
    WHERE rn <= 10    
), item_16 as (
     -- 10 processos mais antigos considerando a data de conclusão
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO,
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        LOCALFISICO,
        cod_relator,
        CPF,
        NOME,
        cod_orgao_julgador_ultimo,
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        CASE 
            WHEN ind_sistema = 'EJUD' THEN ind_conclusao_relator
            ELSE IND_CONCLUSAO
        END AS IND_CONCLUSAO,
        CASE 
            WHEN ind_sistema = 'EJUD' THEN dth_ultima_conclusao_relator
            ELSE DTH_CONCLUSAO_ULTIMA
        END AS DTH_CONCLUSAO_ULTIMA,
        datediff(
            DTH_REFERENCIA,
            CASE 
                WHEN ind_sistema = 'EJUD' THEN dth_ultima_conclusao_relator
                ELSE DTH_CONCLUSAO_ULTIMA
            END
        ) AS DIAS_CONCLUSO,
        16 as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        3 as SUBITEM_V2,
        'AMOSTRA_PROCESSUAL_PARALISADOS_ULTIMA_CONCLUSAO' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY cpf, ind_referencia 
                ORDER BY 
                    datediff(
                        DTH_REFERENCIA,
                        CASE 
                            WHEN ind_sistema = 'EJUD' THEN dth_ultima_conclusao_relator
                            ELSE dth_conclusao_ultima
                        END
                    ) DESC
            ) AS rn
        FROM base_dados
        WHERE ((ind_sistema = 'EJUD' AND ind_conclusao_relator = 'S') OR (ind_sistema = 'EPROC2G' AND IND_CONCLUSAO = 'S'))
    ) t
    WHERE rn <= 10
), item_17 as (
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO,
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        LOCALFISICO,
        cod_relator,
        CPF, 
        NOME,
        cod_orgao_julgador_ultimo,
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        17 as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        4 as SUBITEM_V2,
        'AMOSTRA PROCESSUAL: PROCESSOS DE CRIME CONTRA ADM PUBLICA PARALISADOS' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cpf, ind_referencia ORDER BY (datediff(DTH_REFERENCIA, DTH_CONCLUSAO_ULTIMA)) DESC) as rn
        FROM base_dados
        WHERE IND_CONCLUSAO = 'S' AND ind_crime_adm_publico = 'S' 
    ) t
    WHERE rn <= 10
), item_5_8_v2 as (
    --Total de processos com prioridade legal conclusos ao magistrado há mais de 120 dias
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        CAST(null as INT) as ITEM,
        CAST(null as INT) as SUBITEM,
        5 as ITEM_V2,
        8 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS DE PRIORIDADE LEGAL CONCLUSOS HÁ MAIS DE 120 DIAS' as TP_PERGUNTA,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_sistema,
        ind_secretaria
    FROM base_dados
    WHERE IND_CONCLUSAO = 'S' 
    AND (ind_prioridade_idoso = 'S' OR ind_prioridade_idoso_80 = 'S' OR ind_doenca_grave = 'S' OR ind_prioridade_deficiente = 'S')
    AND DTH_CONCLUSAO_ULTIMA < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' DAY
), item_5_9_v2 as (
    --Total de processos com prioridade legal sem movimentação há mais de 120 dias em Secretaria (excluídos suspensos, sobrestados e arquivados provisoriamente)
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        CAST(null as INT) as ITEM,
        CAST(null as INT) as SUBITEM,
        5 as ITEM_V2,
        9 as SUBITEM_V2,
        'DADOS PROCESSUAIS: TOTAL DE PROCESSOS EM SECRETARIA DE PRIORIDADE LEGAL HÁ MAIS DE 120 DIAS' as TP_PERGUNTA,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_sistema,
    ind_secretaria
    FROM base_dados
    WHERE IND_CONCLUSAO = 'S' 
    AND (ind_prioridade_idoso = 'S' OR ind_prioridade_idoso_80 = 'S' OR ind_doenca_grave = 'S' OR ind_prioridade_deficiente = 'S')
    AND DTH_MOVIMENTO_ULTIMO_CNJ < to_timestamp(DTH_REFERENCIA, 'yyyy-MM-dd HH:mm:ss') - INTERVAL '120' DAY
    AND ind_secretaria = 'S'
    AND IND_SUSPENSO = 'N'
), item_8_5_v2 as (
    --Discrimine os processos em que haja pendência de exame a respeito de medida liminar
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        CAST(null as int) as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        5 as SUBITEM_V2,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        'DADOS PROCESSUAIS: PROCESSOS COM PENDENCIA DE EXAME DE LIMINAR' as TP_PERGUNTA,
        ind_sistema,
        ind_secretaria
    FROM base_dados
    WHERE ((ind_sistema = 'EJUD' AND IND_PEDIDO_LIMINAR = 'S' AND ind_decisao = 'N' AND IND_SUSPENSO = 'N' AND IND_JULGAMENTO = 'N')
           OR (ind_sistema = 'EPROC2G' AND IND_TUTELA_ANTECIPADA = 'S' AND ind_decisao = 'N' AND IND_SUSPENSO = 'N' AND IND_JULGAMENTO = 'N'))
), item_8_6_v2 as (
    --PROCESSOS EM QUE HAJA RÉU PRESO E AGUARDA JULGAMENTO HÁ MAIS DE 60 DIAS
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO, 
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        localfisico,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,  
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        cast(null as int) as IND_CONCLUSAO,
        cast(uc.dt_conclusao as timestamp) as DTH_CONCLUSAO_ULTIMA,
        cast(null as int) as DIAS_CONCLUSO,
        CAST(null as int) as ITEM,
        cast(null as int) as SUBITEM,
        8 as ITEM_V2,
        6 as SUBITEM_V2,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        'DADOS PROCESSUAIS: PROCESSOS EM QUE HAJA RÉU PRESO E AGUARDA JULGAMENTO HÁ MAIS DE 60 DIAS' as TP_PERGUNTA,
        ind_sistema,
        ind_secretaria
    FROM base_dados A
    JOIN (
      SELECT 
        m.coddoc, MAX(CAST(m.dthrmov AS DATE)) AS dt_conclusao
      FROM ejud.movimento m
      WHERE m.codfase = 51 AND m.codcompl1 = 50006
      GROUP BY m.coddoc
    ) uc ON cast(uc.coddoc as varchar(255)) = cast(A.cod_documento as varchar(255)) AND A.ind_sistema = 'EJUD'
    WHERE IND_REU_PRESO = 'S' AND ind_julgamento = 'N' AND ind_suspenso = 'N' AND datediff(DTH_REFERENCIA, uc.dt_conclusao) > 60 
), item_sec_4_1 as (
    SELECT 
        DTH_REFERENCIA,
        IND_REFERENCIA,
        COD_DOCUMENTO,
        COD_CNJ,
        NM_CLASSE,
        DTH_DISTRIBUICAO_ULTIMA,
        DTH_ULTIMO_MOVIMENTO,
        ULTIMO_MOVIMENTO,
        COD_LOCAL_ULTIMO,
        LOCALFISICO,
        cod_relator,
        CPF,
        nome,
        cod_orgao_julgador_ultimo,
        orgao_julgador_ultimo,
        IND_SUSPENSO,
        IND_JULGAMENTO,
        IND_CONCLUSAO,
        DTH_CONCLUSAO_ULTIMA,
        DIAS_CONCLUSO,
        99 as ITEM,
        cast(null as int) as SUBITEM,
        cast(null as int) as ITEM_V2,
        cast(null as int) as SUBITEM_V2,
        'AMOSTRA_PROCESSUAL_PARALISADOS_ULTIMA_DISTRIBUICAO_SECRETARIA' as TP_PERGUNTA,
        ind_sistema,
        DTH_MOVIMENTO_ULTIMO_CNJ,
        ULTIMO_MOVIMENTO_CNJ,
        ind_secretaria
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cod_orgao_julgador_ultimo, ind_referencia ORDER BY (datediff(DTH_REFERENCIA, DTH_DISTRIBUICAO_ULTIMA)) DESC) as rn
        FROM base_dados
        WHERE ind_suspenso = 'N'
    ) t
    WHERE rn <= 10    
), tramitacao as (
    	select 
	DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria
    from item_10_1
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria
    from item_10_2
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_3
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_4
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_5
	union all
	select
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_6
	union all
	select
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_7
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_8
	union all
	select
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_9
	union all
	select 
	DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_10
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_10_12
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_13_1
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_13_6
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA, ind_secretaria 
    from item_13_7
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria
    from item_14
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria
    from item_15
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_16
	union all
	select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_17
    union all
    select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria
    from item_5_9_v2
    union all
    select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_8_5_v2       
    union all
    select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_8_6_v2   
    union all
    select 
    DTH_REFERENCIA,IND_REFERENCIA	,COD_DOCUMENTO,COD_CNJ,NM_CLASSE,DTH_DISTRIBUICAO_ULTIMA,DTH_ULTIMO_MOVIMENTO,ULTIMO_MOVIMENTO,COD_LOCAL_ULTIMO	,LOCALFISICO,COD_RELATOR,CPF ,NOME,COD_ORGAO_JULGADOR_ULTIMO,ORGAO_JULGADOR_ULTIMO,
    IND_SUSPENSO,IND_JULGAMENTO,IND_CONCLUSAO,DTH_CONCLUSAO_ULTIMA,DIAS_CONCLUSO,ITEM,SUBITEM,ITEM_V2,SUBITEM_V2,TP_PERGUNTA,DTH_MOVIMENTO_ULTIMO_CNJ,ULTIMO_MOVIMENTO_CNJ,IND_SISTEMA,ind_secretaria 
    from item_sec_4_1
)
SELECT  
    DTH_REFERENCIA
    ,IND_REFERENCIA
    ,COD_DOCUMENTO
    ,COD_CNJ
    ,NM_CLASSE
    ,DTH_DISTRIBUICAO_ULTIMA
    ,DTH_ULTIMO_MOVIMENTO
    ,ULTIMO_MOVIMENTO
    ,COD_LOCAL_ULTIMO
    ,LOCALFISICO
    ,COD_RELATOR
    ,CPF 
    ,NOME
    ,COD_ORGAO_JULGADOR_ULTIMO
    ,ORGAO_JULGADOR_ULTIMO
    ,IND_SUSPENSO
    ,IND_JULGAMENTO
    ,IND_CONCLUSAO
    ,DTH_CONCLUSAO_ULTIMA
    ,DIAS_CONCLUSO
    ,ITEM
    ,SUBITEM
    ,ITEM_V2
    ,SUBITEM_V2
    ,TP_PERGUNTA
    ,DTH_MOVIMENTO_ULTIMO_CNJ
    ,ULTIMO_MOVIMENTO_CNJ
    ,IND_SISTEMA
    ,ind_secretaria
    ,cast(current_timestamp as timestamp) as DTH_ETL
from tramitacao;