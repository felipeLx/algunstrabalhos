import os
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def meta_export_path(meta_name: str, export_base_path: str, system: str = "ejud") -> str:
	"""
	Monta o path para exportação de resultados de uma meta.
	"""
	if system.lower() == "ejud":
		return os.path.join(export_base_path, meta_name, "current")
	elif system.lower() == "eproc":
		return os.path.join(export_base_path.replace("ejud", "eproc2g"), meta_name, "current")
	else:
		raise ValueError(f"Unknown system: {system}")

def save_parquet_to_postgres_table(
	schema: str = "dw2inst",
	table_name: str = "metas_2025",
	mode: str = "overwrite"
):
	"""
	Lê parquet(s) definidos no config/meta_config.json, empilha e salva no PostgreSQL.
	"""
	spark = SparkSession.builder.appName("LoadParquetMetas").getOrCreate()
	try:
		config_path = Path(__file__).resolve().parents[1] / "config" / "meta_config.json"
		with open(config_path, "r", encoding="utf-8") as f:
			config = json.load(f)

		# Robust key checks
		metas_cfg = config.get("metas", {})
		variaveis = metas_cfg.get("tabela")
		metas = metas_cfg.get("metas")
		paths_cfg = metas_cfg.get("paths", {})
		base_path = paths_cfg.get("export_base_path")

		missing = []
		if variaveis is None:
			missing.append("metas.tabela")
		if metas is None:
			missing.append("metas.metas")
		if base_path is None:
			missing.append("metas.paths.export_base_path")
		if missing:
			logger.error(f"Config missing required key(s): {missing}. Aborting save. Config structure: {json.dumps(config, indent=2)}")
			return

		all_parquet_paths = [
			os.path.join(meta_export_path(meta, base_path), f"{var}.parquet")
			for meta in metas
			for var in variaveis
		]

		existing_paths = [p for p in all_parquet_paths if os.path.exists(p)]
		logger.info(f"Total de arquivos parquet esperados: {len(all_parquet_paths)}")
		logger.info(f"Total de arquivos parquet existentes: {len(existing_paths)}")

		if not existing_paths:
			logger.warning("Nenhum arquivo parquet encontrado. Encerrando execução.")
			return

		df = None
		for pq_file in existing_paths:
			tmp = spark.read.parquet(pq_file)
			if df is None:
				df = tmp
			else:
				df = df.unionByName(tmp)

		logger.info(f"DataFrame final montado com {df.count()} registros e {len(df.columns)} colunas.")

		pg_url = os.getenv("PG_URL", "jdbc:postgresql://172.20.30.141:5432/postgres")
		pg_properties = {
			"user": os.getenv("POSTGRES_USER", "meu_usuario"),
			"password": os.getenv("POSTGRES_PASS", "minha_senha"),
			"driver": "org.postgresql.Driver"
		}

		#logger.info(f"Salvando DataFrame no PostgreSQL: {schema}.{table_name} (mode={mode})")
		df.write \
			.mode(mode) \
			.jdbc(pg_url, f"{schema}.{table_name}", properties=pg_properties)

		#logger.info(f"✅ Dados salvos com sucesso em {schema}.{table_name}")

	except Exception as e:
		logger.exception(f"❌ Erro ao salvar no PostgreSQL: {e}")
		raise
