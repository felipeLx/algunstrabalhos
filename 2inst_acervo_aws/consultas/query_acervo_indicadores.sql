select 	
	dth_referencia as DTH_REFERENCIA
	
	,CAST(id_processo AS DECIMAL(30,0)) as COD_DOCUMENTO
	
	,dth_distribuicao_ultima as DTH_DISTRIBUICAO_ULTIMA
	,CAST(cod_relator_ultimo AS INT) as COD_MAGISTRADO_RELATOR_ULTIMO
	,CAST(cod_relator_primeiro AS INT) as COD_MAGISTRADO_RELATOR_PRIMEIRO
	,CAST(cod_orgao_julgador_ultimo AS INT) as COD_ORGAO_JULGADOR_ULTIMO
	,CAST(cod_orgao_julgador_primeiro AS INT) as COD_ORGAO_JULGADOR_PRIMEIRO

	,dth_ultimo_movimento as dth_movimento_ultimo
	,CAST(id_evento_judicial_ultimo AS INT) as COD_MOVIMENTO_ULTIMO
	,CAST(NULL AS INT) as cod_complemento1_ultimo
	,CAST(NULL AS INT) as cod_complemento2_ultimo
	,CAST(NULL AS INT) as cod_complemento3_ultimo

	,dth_suspensao as DTH_SUSPENSAO
	,CAST(id_evento_judicial_suspensao AS INT) as COD_MOVIMENTO_SUSPENSAO
	,CAST(NULL AS INT) as cod_complemento1_suspensao
	,CAST(NULL AS INT) as cod_complemento2_suspensao
	,CAST(NULL AS INT) as cod_complemento3_suspensao
	,CAST(ind_suspenso AS VARCHAR(1)) as IND_SUSPENSO

	,dth_sobrestamento as DTH_SOBRESTAMENTO
	,CAST(id_evento_judicial_sobrestamento AS INT) as COD_MOVIMENTO_SOBRESTAMENTO
	,CAST(NULL AS INT) as cod_complemento1_sobrestamento
	,CAST(NULL AS INT) as cod_complemento2_sobrestamento
	,CAST(NULL AS INT) as cod_complemento3_sobrestamento
	,CAST(ind_sobrestamento AS VARCHAR(1)) as IND_SOBRESTAMENTO

	,dth_julgamento as DTH_JULGAMENTO
	,CAST(id_evento_judicial_julgamento AS INT) as COD_MOVIMENTO_JULGAMENTO
	,CAST(NULL AS INT) as cod_complemento1_julgamento
	,CAST(NULL AS INT) as cod_complemento2_julgamento
	,CAST(NULL AS INT) as cod_complemento3_julgamento
	,CAST(ind_julgamento AS VARCHAR(1)) as IND_JULGAMENTO

	,CAST(dth_decisao AS TIMESTAMP) as DTH_DECISAO
	,CAST(id_evento_judicial_decisao AS INT) as COD_MOVIMENTO_DECISAO
	,CAST(NULL AS INT) as cod_complemento1_decisao
	,CAST(NULL AS INT) as cod_complemento2_decisao
	,CAST(NULL AS INT) as cod_complemento3_decisao
	,CAST(ind_decisao AS VARCHAR(1)) as IND_DECISAO

	,dth_ultimo_local as DTH_LOCAL_ULTIMO
	,CAST(cod_ultimo_local AS INT) as COD_LOCAL_ULTIMO
		
	,NULL as DTH_LOCAL_VIRTUAL_ULTIMO
	,CAST(NULL AS INT) as COD_LOCAL_VIRTUAL_ULTIMO

	,dth_entrada_localizador as DTH_LOCALIZADOR_ULTIMO
	,CAST(cod_ultimo_localizador AS INT) as COD_LOCALIZADOR_ULTIMO

	,dth_ultima_conclusao as DTH_CONCLUSAO_ULTIMA
	,CAST(id_evento_judicial_conclusao AS INT) as COD_MOVIMENTO_CONCLUSAO
	,CAST(NULL AS INT) as cod_complemento1_conclusao
	,CAST(NULL AS INT) as cod_complemento2_conclusao
	,CAST(NULL AS INT) as cod_complemento3_conclusao
	,CAST(ind_conclusao AS VARCHAR(1)) as IND_CONCLUSAO

	,dth_ultimo_julg_mono as DTH_JULG_MONO_ULTIMO
	,CAST(id_evento_judicial_julg_mono AS INT) as COD_MOVIMENTO_JULG_MONO_ULTIMO
	,CAST(NULL AS INT) as cod_complemento1_julg_mono
	,CAST(NULL AS INT) as cod_complemento2_julg_mono
	,CAST(NULL AS INT) as cod_complemento3_julg_mono
	
	,dth_ultimo_julg_coleg as DTH_JULG_COLEG_ULTIMO
	,CAST(id_evento_judicial_julg_coleg AS INT) as COD_MOVIMENTO_JULG_COLEG_ULTIMO
	,CAST(NULL AS INT) as cod_complemento1_julg_coleg
	,CAST(NULL AS INT) as cod_complemento2_julg_coleg
	,CAST(NULL AS INT) as cod_complemento3_julg_coleg


	,CAST(ind_sistema AS VARCHAR(10)) as IND_SISTEMA
	,CAST(dth_etl AS TIMESTAMP) as DTH_ETL

	,dth_movimento_ultimo_cnj as dth_movimento_ultimo_cnj
	,CAST(id_evento_judicial_ultimo_cnj AS INT) as COD_MOVIMENTO_ULTIMO_CNJ
	,CAST(NULL AS INT) as cod_complemento1_ultimo_cnj
	,CAST(NULL AS INT) as cod_complemento2_ultimo_cnj
	,CAST(NULL AS INT) as cod_complemento3_ultimo_cnj

	,cast(dth_evento_entrada_diligencia as timestamp) as dth_evento_entrada_diligencia
	,cast(dth_evento_saida_diligencia as timestamp) as dth_evento_saida_diligencia
	,cast(ind_diligencia as varchar(1)) as ind_diligencia

FROM dw_2inst_prata.tb_acervo_indicadores_eproc2g

union all

SELECT
	dth_referencia as DTH_REFERENCIA

	,CAST(CODDOC AS DECIMAL(30,0)) as COD_DOCUMENTO

	,dth_distribuicao_ultima as DTH_DISTRIBUICAO_ULTIMA
	,CAST(cod_magistrado_relator_ultimo AS INT) as COD_MAGISTRADO_RELATOR_ULTIMO
	,CAST(cod_magistrado_relator_primeiro AS INT) as COD_MAGISTRADO_RELATOR_PRIMEIRO
	,CAST(cod_orgao_julgador_ultimo AS INT) as COD_ORGAO_JULGADOR_ULTIMO
	,CAST(cod_orgao_julgador_primeiro AS INT) as COD_ORGAO_JULGADOR_PRIMEIRO
			
	,dth_movimento_ultimo as DTH_ULTIMO_MOVIMENTO
	,CAST(cod_fase_ultimo AS INT) as COD_MOVIMENTO_ULTIMO
	,CAST(COD_COMPLEMENTO1_ULTIMO AS INT) as COD_COMPLEMENTO1_ULTIMO
	,CAST(COD_COMPLEMENTO2_ULTIMO AS INT) as COD_COMPLEMENTO2_ULTIMO
	,CAST(COD_COMPLEMENTO3_ULTIMO AS INT) as COD_COMPLEMENTO3_ULTIMO

	-- SUSPENSAO
	,dth_suspensao as DTH_SUSPENSAO
	,CAST(cod_fase_suspensao AS INT) as COD_MOVIMENTO_SUSPENSAO
	,CAST(COD_COMPLEMENTO1_SUSPENSAO AS INT) as COD_COMPLEMENTO1_SUSPENSAO
	,CAST(COD_COMPLEMENTO2_SUSPENSAO AS INT) as COD_COMPLEMENTO2_SUSPENSAO
	,CAST(COD_COMPLEMENTO3_SUSPENSAO AS INT) as COD_COMPLEMENTO3_SUSPENSAO
	,CAST(ind_suspenso AS VARCHAR(1)) as IND_SUSPENSO

	-- SOBRESTAMENTO / REPERCUSSAO
	,dth_repercussao as DTH_SOBRESTAMENTO
	,CAST(cod_fase_repercussao AS INT) as COD_MOVIMENTO_SOBRESTAMENTO
	,CAST(cod_complemento1_repercussao AS INT) as COD_COMPLEMENTO1_SOBRESTAMENTO
	,CAST(cod_complemento2_repercussao AS INT) as COD_COMPLEMENTO2_SOBRESTAMENTO
	,CAST(cod_complemento3_repercussao AS INT) as COD_COMPLEMENTO3_SOBRESTAMENTO
	,CAST(ind_sobrestamento AS VARCHAR(1)) as IND_SOBRESTAMENTO

	-- JULGAMENTO
	,dth_julgamento as DTH_JULGAMENTO
	,CAST(cod_fase_julgamento AS INT) as COD_MOVIMENTO_JULGAMENTO
	,CAST(cod_complemento1_julgamento AS INT) as COD_COMPLEMENTO1_JULGAMENTO
	,CAST(cod_complemento2_julgamento AS INT) as COD_COMPLEMENTO2_JULGAMENTO
	,CAST(cod_complemento3_julgamento AS INT) as COD_COMPLEMENTO3_JULGAMENTO
	,CAST(ind_julgamento AS VARCHAR(1)) as IND_JULGAMENTO

	-- DECISAO 
	,dth_decisao as DTH_DECISAO
	,CAST(cod_fase_decisao AS INT) as COD_MOVIMENTO_DECISAO
	,CAST(cod_complemento1_decisao AS INT) as COD_COMPLEMENTO1_DECISAO
	,CAST(cod_complemento2_decisao AS INT) as COD_COMPLEMENTO2_DECISAO
	,CAST(cod_complemento3_decisao AS INT) as COD_COMPLEMENTO3_DECISAO
	,CAST(ind_decisao AS VARCHAR(1)) as IND_DECISAO

	-- LOCAL
	,dth_ultimo_local as DTH_LOCAL_ULTIMO
	,CAST(cod_ultimo_local AS INT) as COD_LOCAL_ULTIMO

	-- LOCAL VIRTUAL
	,dth_ultimo_local_virtual as DTH_LOCAL_VIRTUAL_ULTIMO
	,CAST(COD_ULTIMO_LOCAL_VIRTUAL AS INT) as COD_LOCAL_VIRTUAL_ULTIMO
	
	--LOCALIZADOR
	,NULL as DTH_LOCALIZADOR_ULTIMO
	,CAST(NULL AS INT) as COD_LOCALIZADOR_ULTIMO

	-- CONCLUSAO 
	,dth_ultima_conclusao as DTH_CONCLUSAO_ULTIMA
	,CAST(cod_fase_conclusao AS INT) as COD_MOVIMENTO_CONCLUSAO
	,CAST(cod_complemento1_ultima_conclusao AS INT) as COD_COMPLEMENTO1_CONCLUSAO
	,CAST(cod_complemento2_ultima_conclusao AS INT) as COD_COMPLEMENTO2_CONCLUSAO
	,CAST(cod_complemento3_ultima_conclusao AS INT) as COD_COMPLEMENTO3_CONCLUSAO
	,CAST(ind_conclusao AS VARCHAR(1)) as IND_CONCLUSAO

	-- JULGAMENTO MONOCRATICO 
	,dth_ultima_julgamento_monocratico as DTH_JULG_MONO_ULTIMO
	,CAST(cod_fase_julgamento_monocratico AS INT) as COD_MOVIMENTO_JULG_MONO_ULTIMO
	,CAST(cod_complemento1_julgamento_monocratico AS INT) as COD_COMPLEMENTO1_JULG_MONO
	,CAST(cod_complemento2_julgamento_monocratico AS INT) as COD_COMPLEMENTO2_JULG_MONO
	,CAST(cod_complemento3_julgamento_monocratico AS INT) as COD_COMPLEMENTO3_JULG_MONO

	-- JULGAMENTO COLEGIADO 
	,dth_ultima_julgamento_colegiado as DTH_JULG_COLEG_ULTIMO
	,CAST(cod_fase_julgamento_colegiado AS INT) as COD_MOVIMENTO_JULG_COLEG_ULTIMO
	,CAST(cod_complemento1_julgamento_colegiado AS INT) as COD_COMPLEMENTO1_JULG_COLEG
	,CAST(cod_complemento2_julgamento_colegiado AS INT) as COD_COMPLEMENTO2_JULG_COLEG
	,CAST(cod_complemento3_julgamento_colegiado AS INT) as COD_COMPLEMENTO3_JULG_COLEG

	,CAST(ind_sistema AS VARCHAR(10)) as IND_SISTEMA
	,CAST(dth_etl AS TIMESTAMP) as DTH_ETL

	,dth_movimento_ultimo_cnj as dth_movimento_ultimo_cnj
	,CAST(cod_fase_ultimo_cnj AS INT) as COD_MOVIMENTO_ULTIMO_CNJ
	,CAST(cod_complemento1_ultimo_cnj AS INT) as cod_complemento1_ultimo_cnj
	,CAST(cod_complemento2_ultimo_cnj AS INT) as cod_complemento2_ultimo_cnj
	,CAST(cod_complemento3_ultimo_cnj AS INT) as cod_complemento3_ultimo_cnj

	,cast(null as timestamp) as dth_evento_entrada_diligencia
	,cast(null as timestamp) as dth_evento_saida_diligencia
	,cast(null as varchar(1)) as ind_diligencia
	
FROM dw_2inst_prata.tb_acervo_indicadores_ejud;