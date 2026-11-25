"""eproc2g Meta CNJ pipeline package.

Layout:
    config.py      Settings (where to read/write) + rules loaded from meta_config.json
    spark.py       SparkSession builder with Iceberg/Glue catalog
    dates.py       Inclusive day-range timestamp helpers
    sources.py     Situacao filtering + bronze enrichment
    iceberg.py     Target DDL + DELETE/APPEND write
    meta01/        Meta 01 indicators (eligible, indicators, schema, diagnostics, pipeline)

Business rules: see docs/meta01_rules.md.
"""
