import os


def is_debug() -> bool:
    """Return True when pipeline debug mode is enabled via env var PIPELINE_DEBUG.

    This lets us keep heavy driver-side operations (count/show/collect) off by default
    while preserving the ability to re-enable them for troubleshooting.
    """
    val = os.environ.get("PIPELINE_DEBUG", "0")
    return val.lower() in ("1", "true", "yes")


def debug_log_count(logger, df, msg: str):
    """Log DataFrame count only when debug mode is active (best-effort)."""
    if not is_debug():
        return
    try:
        c = df.count()
        logger.info(f"{msg}: {c}")
    except Exception as e:
        logger.warning(f"Could not compute debug count for {msg}: {e}")
