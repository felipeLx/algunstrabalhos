CREATE VIEW dw_2inst_ouro.vw_ft_acervo_inspecao_secretaria AS
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    5 as item_sec,
    1 as subitem_sec,
    3 as item_sec_v2,
    1 as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 10 AND A.subitem = 1
union all
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    5 as item_sec,
    2 as subitem_sec,
    3 as item_sec_v2,
    2 as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 10 AND A.subitem = 2
union all 
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    5 as item_sec,
    4 as subitem_sec,
    3 as item_sec_v2,
    3 as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 10 AND A.subitem = 3
union all
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    A.tp_pergunta,
    5 as item_sec,
    5 as subitem_sec,
    3 as item_sec_v2,
    4 as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 10 AND A.subitem = 9
union all
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    5 as item_sec,
    7 as subitem_sec,
    null as item_sec_v2,
    null as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 10 AND A.subitem = 12
union all
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    A.tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    7 as item_sec,
    null as subitem_sec,
    4 as item_sec_v2,
    1 as subitem_sec_v2,
    A.ind_sistema,
    cast(null as timestamp) as dth_entrada_mesa_virtual,
    cast(null as decimal(30,0)) as cod_mesa_virtual,
    cast(null as varchar(255)) as mesa_virtual,
    a.dth_etl
FROM dw_2inst_ouro.tb_ft_acervo_inspecao a
WHERE A.item = 99
union all
SELECT 
    A.dth_referencia,
    A.ind_referencia,
    A.cod_documento,
    A.cod_cnj,
    A.nm_classe,
    A.dth_distribuicao_ultima,
    A.dth_ultimo_movimento,
    A.ultimo_movimento,
    A.cod_local_ultimo,
    A.localfisico,
    A.cod_relator,
    A.cpf,
    A.nome,
    A.cod_orgao_julgador_ultimo,
    A.orgao_julgador_ultimo,
    A.ind_suspenso,
    A.ind_julgamento,
    A.ind_conclusao,
    A.dth_conclusao_ultima,
    A.dias_concluso,
    CAST(NULL AS varchar(255)) AS tp_pergunta,
    A.DTH_MOVIMENTO_ULTIMO_CNJ,
    A.ULTIMO_MOVIMENTO_CNJ,
    3 AS item_sec,
    6 AS subitem_sec,
    3 AS item_sec_v2,
    6 AS subitem_sec_v2,
    A.ind_sistema,
    CASE
       WHEN A.ind_sistema = 'EJUD'    THEN CAST(ejud_lvd.dthrentr AS timestamp)
       WHEN A.ind_sistema = 'EPROC2G' THEN CAST(eproc_lvd.dth_inclusao AS timestamp)
    END AS dth_entrada_mesa_virtual,
    CASE
        WHEN A.ind_sistema = 'EJUD'    THEN CAST(ejud_lvd.codlocalvirt AS decimal(30,0))
        WHEN A.ind_sistema = 'EPROC2G' THEN CAST(eproc_lvd.id_localizador AS decimal(30,0))
    END AS cod_mesa_virtual,
    CASE
        WHEN A.ind_sistema = 'EJUD'    THEN ejud_lvd.descr
        WHEN A.ind_sistema = 'EPROC2G' THEN eproc_lvd.des_localizador
    END AS mesa_virtual,
    A.dth_etl
	FROM dw_2inst_ouro.tb_ft_acervo_inspecao A
	LEFT JOIN (
	            SELECT *
	              FROM (
	                     SELECT 
	                         A2.cod_documento,
	                         A2.dth_referencia,
	                         A2.ind_referencia,
	                         lvd.codlocalvirt,
	                         lvd.dthrentr,
	                         lv.descr,
	                         ROW_NUMBER() OVER (PARTITION BY A2.cod_documento, A2.dth_referencia ORDER BY lvd.dthrentr DESC
	                         ) AS rn
	                     FROM (
	                            SELECT 
	                                dth_referencia,
	                                cod_documento, 
	                                ind_sistema,
	                                ind_referencia
	                            FROM dw_2inst_ouro.tb_ft_acervo_inspecao 
	                            WHERE item = 10 
	                              AND subitem = 1 
	                              AND ind_sistema = 'EJUD' 
	                    ) A2
	                  LEFT JOIN ejud.localvirtualdocumento lvd  
	                  	ON A2.cod_documento = lvd.coddoc AND CAST(lvd.dthrentr AS timestamp) <= CAST(A2.dth_referencia AS timestamp)
	                  LEFT JOIN ejud.localvirtual lv ON lvd.codlocalvirt = lv.codlocalvirt and lvd.codlocal = lv.codlocal
	              )
	              WHERE rn = 1
	          ) ejud_lvd 
	    ON A.cod_documento = ejud_lvd.cod_documento and a.ind_referencia = ejud_lvd.ind_referencia 
	LEFT JOIN (
	   SELECT
	       A3.cod_documento,
	       A3.dth_referencia,
	       A3.ind_referencia,
	       pl.dth_inclusao,
	       pl.dth_desativacao,
	       l.id_localizador,
	       l.des_localizador
	   FROM (
	          SELECT 
	              dth_referencia, 
	              cod_documento, 
	              ind_sistema,
	              ind_referencia
	          FROM dw_2inst_ouro.tb_ft_acervo_inspecao 
	          WHERE item = 10 
	            AND subitem = 1 
	            AND ind_sistema = 'EPROC2G' 
	            ) A3
		JOIN eproc2g.processo_localizador pl 
			ON CAST(A3.cod_documento AS decimal(30,0)) = CAST(pl.id_processo AS decimal(30,0))     
	        AND CAST(pl.dth_inclusao AS timestamp) <= CAST(A3.dth_referencia AS timestamp)
            AND (CAST(pl.dth_desativacao AS timestamp) >= CAST(A3.dth_referencia AS timestamp) OR pl.dth_desativacao IS NULL)
		JOIN eproc2g.localizador_orgao lo ON pl.id_localizador_orgao = lo.id_localizador_orgao
		JOIN eproc2g.localizador l ON lo.id_localizador = l.id_localizador
	) eproc_lvd  ON A.cod_documento = eproc_lvd.cod_documento and a.ind_referencia = eproc_lvd.ind_referencia 
	WHERE A.item = 10 AND A.subitem = 1;
