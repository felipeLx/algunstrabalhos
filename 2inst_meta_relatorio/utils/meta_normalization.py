import logging
from pyspark.sql.functions import coalesce, lit, current_timestamp, to_timestamp, col, when, explode, struct, array, filter, from_utc_timestamp, lower, regexp_replace
from pathlib import Path

def standardize_combined(combined, logger, cfg, meta_number, config_prefixes=None):
    """Normalize an aggregated combined DataFrame into canonical columns.

    Returns a Spark DataFrame with standardized columns including:
    id_processo, id_classe_judicial, cod_orgao_julgador, nivel_sigilo,
    sistema, variavel, dth_inicio, dth_distribuidos,
    distribuicao, dth_julgados, julgados, suspensos, desobrestados, denominador, resultado
    """
    cols = combined.columns

    def find_cols_by_suffix(suffixes):
        return [c for c in cols if any(c.endswith(suf) for suf in suffixes)]

    def find_cols_by_prefix(prefix):
        return [c for c in cols if c.startswith(prefix)]

    id_processo_col = col("coddoc").alias("id_processo")

    classe_candidates = find_cols_by_suffix(["_cod_classe", "_codclasse", "_classe"])
    id_classe = coalesce(*[col(c) for c in classe_candidates]).alias("id_classe_judicial") if classe_candidates else lit(None).alias("id_classe_judicial")

    nivel_candidates = find_cols_by_suffix(["_nivel_sigilo", "_id_sigilo", "_sigilo"])
    nivel_sig = coalesce(*[col(c) for c in nivel_candidates]).alias("nivel_sigilo") if nivel_candidates else lit(None).alias("nivel_sigilo")

    sistema_col = lit("ejud").alias("sistema")

    # id_assunto_principal: collect any per-prefix subject id columns (e.g. susm01_id_assunto_principal)
    assunto_candidates = [c for c in cols if c.lower().endswith('id_assunto_principal')]
    logger.debug(f"Found assunto_candidates: {assunto_candidates}")
    if not assunto_candidates:
        logger.warning(f"No id_assunto_principal columns found in any variables! Available columns: {[c for c in cols if 'assunto' in c.lower()]}")
    id_assunto_col = coalesce(*[col(c) for c in assunto_candidates]).alias('id_assunto_principal') if assunto_candidates else lit(None).alias('id_assunto_principal')

    # variavel: prefer config variable filenames (case1) and set based on presence sums
    if config_prefixes is None:
        try:
            cfg_vars = cfg.get_meta_config(f"meta_{meta_number}").get("variables", {})
            cfg_case1 = cfg_vars.get("case1", []) if isinstance(cfg_vars, dict) else []
            config_prefixes = [Path(x).stem for x in cfg_case1]
        except Exception:
            config_prefixes = []

    # For long format: build array of active prefixes with their dth, then explode
    prefix_structs = []
    for p in config_prefixes:
        present_col = f"{p}_present"
        # Only process this prefix if the present column actually exists in the data
        if present_col in cols:
            p_dth_candidates = [c for c in cols if c.startswith(p) and any(s in c for s in ["dthrmov", "dth_fim", "dth_inicio"])]
            p_dth = to_timestamp(coalesce(*[col(c) for c in p_dth_candidates])) if p_dth_candidates else lit(None)
            prefix_structs.append(when(col(present_col) > 0, struct(lit(p).alias('prefix'), p_dth.alias('dth'))).otherwise(None))
        else:
            logger.warning(f"Skipping prefix '{p}' - column '{present_col}' not found in data. Available columns: {[c for c in cols if '_present' in c]}")

    # Only proceed if we found any valid prefixes
    if prefix_structs:
        active_array = array(*prefix_structs)
        filtered_array = filter(active_array, lambda x: x.isNotNull())
        combined = combined.withColumn('exploded', explode(filtered_array))
    else:
        logger.error(f"No valid prefix columns found! Config prefixes: {config_prefixes}, Available present columns: {[c for c in cols if '_present' in c]}")
        # Fallback: add a dummy exploded column so the rest of the function works
        combined = combined.withColumn('exploded', struct(lit("unknown").alias('prefix'), lit(None).alias('dth')))

    # Now, set variavel and dth_inicio from exploded
    variavel_col = col('exploded.prefix').alias('variavel')
    dth_col = col('exploded.dth').alias('dth_evento')
    
    flag_col = lit(1).alias("flag")
    dth_atualizacao_col = from_utc_timestamp(current_timestamp(), "America/Sao_Paulo").alias("dth_atualizacao")

    # Map to variable dimension id (cd_ind) using a simple presence-flag mapping first (KISS),
    # then fall back to exploded.prefix matching if presence flags are missing.
    cd_ind_col = None
    var_map = {}
    try:
        meta_cfg = cfg.get_meta_config(f"meta_{meta_number}")
        var_map = meta_cfg.get("variable_dimension_map", {}) if isinstance(meta_cfg, dict) else {}
        logger.debug(f"config_prefixes: {config_prefixes}")
        logger.debug(f"variable_dimension_map keys: {list(var_map.keys())}")

        # Prefer exploded.prefix mapping per exploded row (deterministic for long format rows)
        normalized_prefix = regexp_replace(lower(col('exploded.prefix').cast('string')), r"\.parquet$", "")
        if var_map:
            cd_expr = None
            for p, mapped in var_map.items():
                p_norm = str(p).strip().lower()
                if cd_expr is None:
                    cd_expr = when(normalized_prefix == p_norm, lit(mapped))
                else:
                    cd_expr = cd_expr.when(normalized_prefix == p_norm, lit(mapped))
            if cd_expr is not None:
                cd_ind_col = cd_expr.otherwise(lit(None)).cast('int').alias('cd_ind')
                logger.debug('cd_ind mapped using exploded.prefix matching')
                # Sample suppressed to avoid driver-side blocking operations

        # If exploded mapping yielded no value (e.g., exploded is null), fallback to presence flags
        if cd_ind_col is None:
            pres_expr = None
            pres_cols_sample = []
            priority_prefixes = config_prefixes if config_prefixes else list(var_map.keys())
            for p in priority_prefixes:
                mapped = var_map.get(p)
                if mapped is None:
                    continue
                pres = f"{p}_present"
                if pres in cols:
                    pres_cols_sample.append(pres)
                    if pres_expr is None:
                        pres_expr = when(col(pres) > 0, lit(mapped))
                    else:
                        pres_expr = pres_expr.when(col(pres) > 0, lit(mapped))
            if pres_expr is not None:
                cd_ind_col = pres_expr.otherwise(lit(None)).cast('int').alias('cd_ind')
                logger.debug('cd_ind mapped using presence-flag fallback')
                # Presence-flag sample suppressed to avoid driver-side blocking operations
    except Exception as e:
        logger.warning(f"Failed to build cd_ind mapping: {e}")
        cd_ind_col = None

    # If there is an explicit process-event id column in combined, expose it as id_processo_evento
    id_proc_evt_candidates = [c for c in cols if c.lower().endswith('id_processo_evento') or c.lower().endswith('cod_tip_mov_numero') or c.lower().endswith('id_evento')]
    id_processo_evento_col = col(id_proc_evt_candidates[0]).alias('id_processo_evento') if id_proc_evt_candidates else lit(None).alias('id_processo_evento')

    # Insert any generic per-prefix dth columns before denominador/result
    standardized_cols = [
        id_processo_col, id_classe, cod_oj, nivel_sig,
        sistema_col, variavel_col, dth_col, flag_col,
        id_processo_evento_col, dth_atualizacao_col
    ]

    # include the cod_indicador column if we were able to map it
    if cd_ind_col is not None:
        standardized_cols.append(cd_ind_col)
    # include assunto principal if present
    if id_assunto_col is not None:
        standardized_cols.append(id_assunto_col)

    final_df = combined.select(*standardized_cols)

    # Deterministic explicit mapping join: map variavel -> cd_ind using variable_dimension_map
    try:
        if var_map:
            # build mapping as (variavel_key, cd_ind_map) using lower-case keys
            mapping = [(str(k).strip().lower(), int(v)) for k, v in var_map.items()]
            map_df = combined.sparkSession.createDataFrame(mapping, schema=["variavel_key", "cd_ind_map"])  # small local DF
            from pyspark.sql.functions import lower as _lower, col as _col, coalesce as _coalesce
            final_df = final_df.withColumn('variavel_l', _lower(_col('variavel').cast('string')))
            final_df = final_df.join(map_df, final_df['variavel_l'] == map_df['variavel_key'], how='left')
            # prefer mapping value from config over any ambiguous cd_ind (mapping overrides when present)
            final_df = final_df.withColumn('cd_ind', _coalesce(_col('cd_ind_map'), _col('cd_ind')))
            # debug sample (light)
            # Mapping post-join sample suppressed to avoid driver-side blocking operations
            final_df = final_df.drop('variavel_l', 'variavel_key', 'cd_ind_map')
    except Exception as e:
        logger.debug(f'Explicit mapping join inside standardize_combined failed: {e}')

    # Ensure downstream consumers expecting the canonical column name `dth_evento_indicador`
    # will find it. Some paths build the column as `dth_evento`; normalize to the
    # expected name if needed.
    try:
        cols = final_df.columns
        if 'dth_evento' in cols and 'dth_evento_indicador' not in cols:
            final_df = final_df.withColumnRenamed('dth_evento', 'dth_evento_indicador')
    except Exception:
        # Defensive: if renaming fails, just return the original DataFrame
        pass

    return final_df
