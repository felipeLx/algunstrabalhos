"""eproc2g Meta 01 entrypoint used by run_all_metas.py.

All logic lives in the eproc_meta01/ package. Architecture: docs/meta_01_eproc.md.
"""

from eproc_meta01 import pipeline


class EprocMetaCalculator:
    """Thin adapter kept so run_all_metas.py does not change.

    config_mgr, meta_number and mode are accepted for compatibility but not used:
    the write strategy (delete + append) is fixed in eproc_meta01/writer.py.
    """

    def __init__(self, spark, logger, config_mgr, config_path=None, meta_number="01_eproc", mode="append"):
        self.spark = spark
        self.logger = logger
        self.config_path = config_path

    def normalize_and_save(self) -> bool:
        self.logger.info("Starting eproc2g Meta 01 (eproc_meta01.pipeline.run)")
        return pipeline.run(self.spark, self.config_path)
