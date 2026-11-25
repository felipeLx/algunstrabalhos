# eproc2g Meta 01 – prata → Postgres (`meta_01_eproc.py` / `eproc_meta01/`)

This job takes the eproc2g Meta 01 **variable files** (already computed upstream by
`2inst_metacnj/eproc2g`) and loads two Postgres tables used by the reports:

| Target | Grain | Content |
|---|---|---|
| `dw2inst.meta_analitico` | one row per **process × indicator** | which processes count for each indicator, with the date of the event |
| `dw2inst.fato_auxiliar` | one row per **process** | process details: first/last distribution, órgão julgador, magistrado, last event, flags |

It runs from `run_all_metas.py` (meta `01_eproc`):

```python
from meta_01_eproc import EprocMetaCalculator
EprocMetaCalculator(spark=spark, logger=logger, config_mgr=config_mgr, meta_number=meta, mode=mode).normalize_and_save()  # -> bool
```

---

## 1. Code map

```
meta_01_eproc.py            thin adapter (keeps run_all_metas.py unchanged) -> pipeline.run()
eproc_meta01/
├── constants.py            META_ANO, COD_SISTEMA, COD_INDICADOR, PRATA_DIR, Postgres table names
├── sources.py              ALL parquet reads (prata variables, bronze tables)
├── variables.py            tag cod_indicador, derive PendJulg01TG, union
├── enrichment.py           add_* functions: processo, natureza, órgão julgador, magistrado, assunto, last event, flags
├── meta_analitico.py       final select/casts for dw2inst.meta_analitico
├── fato_auxiliar.py        final select/casts + one row per process for dw2inst.fato_auxiliar
├── writer.py               Postgres writes (lazy psycopg2 import)
└── pipeline.py             order of steps, fallbacks, writes
tests/test_eproc_meta01.py  unit tests with in-memory DataFrames (no files, no DB)
```

Rules for where code goes:

- **Reads only in `sources.py`.** Every other function receives DataFrames, so it can be tested with tiny fake data.
- **Business rules in pure functions** (`variables`, `enrichment`, `meta_analitico`, `fato_auxiliar`): DataFrame in → DataFrame out, no I/O, no logging of counts.
- **Fallbacks and writes only in `pipeline.py` / `writer.py`.** If you wonder "what happens when table X is missing", look there only.
- **Magic numbers only in `constants.py`.** Change the year in one place.

---

## 2. Data flow

```
config/variables_config.json  (meta_01.variables.case1 = cnm01, julgadom01, desm01, susm01 .parquet)
        │
        ▼
sources.read_variables ── prata data/prata/eproc2g/meta_cnj2026/meta_01/current/<var>.parquet
        │
        ▼
variables.tag_variables     cod_indicador per file + PendJulg01TG = cnm01 LEFT ANTI julgadom01
variables.union_variables   unionByName(allowMissingColumns=True)
        │                   -> one row per (id_processo, indicator)
        ▼
enrichment (bronze eproc2g)
  add_processo         INNER  processo              num_processo, id_classe_judicial, cod_competencia, id_sigilo, id_assunto_principal
  add_natureza         LEFT   competencia_judicial  cod_natureza
  add_orgao_julgador   LEFT   log_processo          cod_prim_oj, cod_ult_oj
  add_magistrado       LEFT   historico_distribuicao_judicial  cod_prim/ult_magistrado, dth_prim/ult_distribuicao
  add_assunto          INNER  assunto_judicial      cod_mni_assunto_judicial
        │
        ├──► meta_analitico.build ──► writer.save_meta_analitico ──► dw2inst.meta_analitico
        │
        └──► add_ultimo_evento   LEFT processo_evento + evento_judicial  dth_ult_evento, cod_ult_evento_cnj
             add_flag            prata baixm01  -> ind_baixado
             add_flag            prata julgadom01 -> ind_julgado
             fato_auxiliar.build ──► writer.save_fato_auxiliar ──► dw2inst.fato_auxiliar
```

---

## 3. Indicators (`constants.COD_INDICADOR`)

| cod_indicador | Variable | Source | `dth_evento_indicador` |
|---|---|---|---|
| 1 | `cnm01` – casos novos | prata file | `dth_inicio` of the file |
| 2 | `julgadom01` – julgados | prata file | `dth_inicio` |
| 3 | `susm01` – suspensos | prata file | `dth_inicio` |
| 4 | `desm01` – dessobrestados | prata file | `dth_inicio` |
| 5 | `PendJulg01TG` – pendentes | **derived here**: cnm01 rows whose id_processo is not in julgadom01 | `dth_inicio` of the cnm01 row |

A variable file whose name is not in the map gets `cod_indicador = NULL` and a warning.

---

## 4. Column rules

### meta_analitico

| Column | From |
|---|---|
| `id_processo` | variable file |
| `num_processo` | bronze `processo`, left-padded with zeros to 20 chars |
| `cod_sistema` | constant `2` (eproc2g) |
| `cod_classe_cnj` | `processo.id_classe_judicial` |
| `id_assunto_principal` | **CNJ code** `assunto_judicial.cod_mni_assunto_judicial` (not the internal id) |
| `cod_competencia`, `id_sigilo` | `processo` |
| `cod_natureza` | `competencia_judicial`: `sin_civel='S'` → 1, else `sin_criminal='S'` → 2, else NULL |
| `dth_evento_indicador` | `dth_inicio` of the variable row |
| `cod_indicador` | section 3 |
| `dth_atualizacao` | `current_timestamp()` |
| `cod_ult_oj` | `id_orgao_colegiado` of the latest `log_processo` row (by `dth_alteracao`) |
| `cod_ult_magistrado` | `cod_magistrado` of the latest `historico_distribuicao_judicial` row (by `dth_distribuicao`) |
| `meta_ano` | `constants.META_ANO` |

### fato_auxiliar

| Column | From |
|---|---|
| `dth_prim_distribuicao`, `cod_prim_magistrado` | earliest `historico_distribuicao_judicial` row |
| `dth_ult_distribuicao`, `cod_ult_magistrado` | latest `historico_distribuicao_judicial` row |
| `cod_prim_oj` | earliest `log_processo` row, **only if its `id_orgao_colegiado` is not NULL** |
| `cod_ult_oj` | latest `log_processo` row |
| `dth_ult_evento`, `cod_ult_evento_cnj` | latest `processo_evento` row (by `dth_evento`) → `evento_judicial.cod_mni_movimento` |
| `ind_baixado` | 1 if in prata `baixm01.parquet`, else 0 |
| `ind_julgado` | 1 if in prata `julgadom01.parquet`, else 0 |
| `id_sigilo` | `processo` |
| `complemento` | NULL |
| `meta_ano` | `constants.META_ANO` |

---

## 5. Failures and fallbacks (`pipeline.py`)

| Missing / failing | Effect |
|---|---|
| a prata variable file | skipped with a warning; other variables still load |
| all prata variable files | job returns `False` |
| `processo`, `competencia_judicial`, `log_processo`, `historico_distribuicao_judicial` | job returns `False` |
| `assunto_judicial` | `id_assunto_principal` = NULL (rows kept) |
| `processo_evento` / `evento_judicial` | `dth_ult_evento`, `cod_ult_evento_cnj` = NULL |
| `baixm01.parquet` / `julgadom01.parquet` (flags) | `ind_baixado` / `ind_julgado` = 0 |

### Write strategy (`writer.py`)

1. **meta_analitico**: `DELETE FROM dw2inst.meta_analitico WHERE meta_ano = 2026 AND cod_sistema = 2 AND cod_indicador IN (<codes present>)`, then INSERT (`save_df_to_postgres_no_jar`). Re-running is safe; ejud rows (`cod_sistema = 1`) are untouched.
2. If step 1 fails: write `/tmp/diagnostic_meta_analitico_<epoch>.parquet`, then retry with a plain JDBC append (`save_df_to_postgres`). **This fallback does not delete**, so a retry after a partial failure can duplicate rows.
3. **fato_auxiliar**: plain append (`save_parquet_to_postgres`). **No delete**: running twice duplicates rows unless the util / table handles it.

The job returns `True` only if both tables were written.

---

## 6. Known issues (kept as-is on purpose – behaviour unchanged by the refactor)

These were in the old script. The refactor did not change them so results stay identical; each needs a business decision.

1. **No de-duplication inside a variable file.** The old code computed `df.dropDuplicates(["id_processo"])` and never used it. If a prata file has two rows for the same process, `meta_analitico` gets two rows for that indicator.
2. **INNER joins drop rows silently.** A process missing from bronze `processo`, or with NULL / unmapped `id_assunto_principal`, disappears from both tables. (Old comments said "left join".)
3. **`cod_prim_oj` is "first log row if not NULL"**, not "first non-NULL órgão". If the earliest log row has NULL `id_orgao_colegiado`, `cod_prim_oj` is NULL even when later rows have a value.
4. **Ties in ordering are non-deterministic.** Two log / distribution / event rows with the same timestamp → Spark picks either.
5. **`fato_auxiliar` is append-only** (see write strategy).
6. **Year 2026 is hardcoded** in `constants.py` and also in `utils/parquet_utils.meta_current` / `meta_01_current`. Change both for 2027.

Removed as dead code (no effect on output): unused `assuntos_groups.json` load (the file name was wrong, real file is `assunto_groups.json`, and the list was never used), `source_variable` / `variavel` columns, the `PendJulg01TG_` column prefix, the `PendJulg01TG_present` column, and unused imports.

Performance change (same results): `log_processo`, `historico_distribuicao_judicial` and `processo_evento` are filtered to the processes in the job (left semi join) **before** the row_number windows, instead of ranking the whole table.

---

## 7. Tests

```powershell
$env:PYSPARK_PYTHON="python"; $env:PYSPARK_DRIVER_PYTHON="python"; python -m pytest tests/test_eproc_meta01.py -q
```

Tests build small DataFrames in memory and call the pure functions; no parquet files, no Postgres, no psycopg2 needed.

## 8. How to change things

| Want to… | Edit |
|---|---|
| change the year | `constants.META_ANO` (+ `utils/parquet_utils` meta paths) |
| add a new indicator file | add it to `config/variables_config.json` (`meta_01.variables.case1`) and to `constants.COD_INDICADOR` |
| change how a column is computed | the matching `add_*` in `enrichment.py`, then the column table above |
| change a fallback | `pipeline.py` |
| change how Postgres is written | `writer.py` |
