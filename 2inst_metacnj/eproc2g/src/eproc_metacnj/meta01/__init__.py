"""Meta 01 — cnm01, julgadom01, PendJulg01TG, susm01, desm01 for eproc2g.

Read in this order:
    eligible.py     which processes count at all
    indicators.py   one function per indicator (DataFrame in -> DataFrame out)
    schema.py       map any indicator to the tb_ft_meta_analitico columns
    pipeline.py     wiring: read, build, cache, write
    diagnostics.py  extra logging when a step returns zero rows
"""
