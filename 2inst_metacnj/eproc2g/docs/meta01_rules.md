# eproc2g — Meta 01 rules

Job: `scripts/eproc_metacnj_m01_emr.py` → `dw_2inst_ouro.tb_ft_meta_analitico`, `cod_sistema = 2`.

## Code map

| Want to know… | Open |
|---|---|
| CLI flags, how the job starts | `scripts/eproc_metacnj_m01_emr.py` |
| Order of steps, what is cached, what is written | `src/eproc_metacnj/meta01/pipeline.py` |
| Which processes count | `src/eproc_metacnj/meta01/eligible.py` |
| How each indicator is computed | `src/eproc_metacnj/meta01/indicators.py` |
| Output columns | `src/eproc_metacnj/meta01/schema.py` |
| Situacao codes, tables, target | `json/meta_config.json`, `src/eproc_metacnj/config.py` |
| Why DELETE + APPEND | `src/eproc_metacnj/iceberg.py` |

## Sources

| Table | Use |
|---|---|
| `dw_2inst_ouro.tb_tf_situacao_eproc` | Events. `id_processo` STRING, `dth_inicio`, `situacao`, `id_classe_judicial` (already CNJ code), `id_assunto_principal` (internal eproc2g id), `id_orgao_colegiado`, `id_sigilo`, `num_processo` |
| `eproc2g.processo` | Class + competência per process (`id_processo` INT → cast to STRING) |
| `eproc2g.relacao_classe_judicial_mni` | Internal class → CNJ class code |
| `eproc2g.assunto_judicial` | Internal assunto → CNJ assunto code |
| `eproc2g.competencia_judicial` | `cod_natureza`: `sin_civel='S'` → 1, `sin_criminal='S'` → 2 (today all eproc2g = 1) |

## Eligible processes

1. Class is "conhecimento": `cod_mni_classe_judicial IN conhecimento_mni_codes`.
   Uses `relacao_classe_judicial_mni` directly. `mni_classe_judicial.natureza` is incomplete in Iceberg.
2. `cod_competencia NOT IN competencia_exclude`.
3. Has a distribution event (`trigger_codes` = 24, 9, 65, 81) up to the end of `period_end`.
   eproc2g has sit-24 only. sit-61 does not exist.

## Indicators

All periods are inclusive: `period_start 00:00:00` to `period_end 23:59:59`.

| variavel | cod_indicador | Rule | dth_evento_indicador |
|---|---|---|---|
| cnm01 | 1 | First-ever sit-88 (Pendente) of an eligible process falls in the period, and a trigger event exists at or before it | first sit-88 |
| julgadom01 | 2 | First judgment event in the period for **any** eligible process (backlog included). Codes: `prim_sent` ∪ `baixa`. decm (140) and mpum (17, 34) do not occur in eproc data | first judgment |
| PendJulg01TG | 5 | cnm01 not in julgadom01 | first sit-88 |
| susm01 | 3 | cnm01 whose last suspension event (ADD or REMOVE codes, from `dist_start` to `period_end`) is an ADD code: still suspended | first sit-88 |
| desm01 | 4 | cnm01 whose last suspension event is a REMOVE code dated inside the period | suspension lifted |

Compliance formula: `julgadom1 / (cnm1 + desm1 − susm1)`.

## Write strategy

ejud m01 writes `cod_sistema = 1` with `overwritePartitions()`. eproc2g must not overwrite, or it deletes
ejud rows. eproc2g runs, per indicator:

```sql
DELETE FROM tb_ft_meta_analitico
WHERE cod_sistema = 2 AND meta_ano = :year AND cod_indicador = :cod;
-- then APPEND
```

**ejud m01 must run before eproc2g m01.**

## Not available

`cod_ult_magistrado` is NULL. It needs a `historico_distribuicao` / `log_processo` join.

## Validate in Athena

```sql
SELECT cod_indicador, COUNT(*) AS n
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = 2026 AND cod_sistema = 2
GROUP BY cod_indicador
ORDER BY cod_indicador;
```

More checks: `consultas/08_validate_m01_eproc.sql`.

## Deploy

`--py-files` needs the package as a zip. CI runs `python3 eproc2g/build_zip.py`, then syncs
`scripts/*.py` and `scripts/*.zip` to S3. `run_all_metas_emr.py` passes
`--py-files .../eproc2g/scripts/eproc_metacnj.zip`.

## Tests

```bash
cd eproc2g
python -m pytest tests
```

Local SparkSession, small in-memory DataFrames. No AWS access needed.
