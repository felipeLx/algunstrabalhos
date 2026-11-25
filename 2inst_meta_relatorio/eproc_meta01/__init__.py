"""eproc2g Meta 01: prata variable parquets -> Postgres dw2inst.meta_analitico + dw2inst.fato_auxiliar.

Read in this order:
    constants.py       year, system code, indicator codes, paths, target tables
    sources.py         every parquet read (prata variables + bronze tables)
    variables.py       tag each variable with cod_indicador, derive PendJulg01TG, union
    enrichment.py      add processo / natureza / órgão julgador / magistrado / assunto
    meta_analitico.py  shape rows for dw2inst.meta_analitico
    fato_auxiliar.py   shape one row per process for dw2inst.fato_auxiliar
    writer.py          Postgres writes (delete + append, JDBC fallback)
    pipeline.py        wiring: read -> build -> write

Full explanation: docs/meta_01_eproc.md.
"""
