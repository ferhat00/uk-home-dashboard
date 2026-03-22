"""
ETL Pipeline Orchestrator.

Runs all ETL modules in sequence with logging and error handling.
"""
import logging
import time

from etl import land_registry, epc, ons_hpi, crime, schools, amenities, transport, noise

logger = logging.getLogger(__name__)

PIPELINE_MODULES = [
    ("land_registry", land_registry),
    ("epc", epc),
    ("ons_hpi", ons_hpi),
    ("crime", crime),
    ("schools", schools),
    ("amenities", amenities),
    ("transport", transport),
    ("noise", noise),
]


def run_all() -> dict[str, str]:
    """Run all ETL pipelines and return status for each."""
    results = {}
    for name, module in PIPELINE_MODULES:
        logger.info("──── Starting %s ETL ────", name)
        start = time.time()
        try:
            module.run()
            elapsed = time.time() - start
            results[name] = f"success ({elapsed:.1f}s)"
            logger.info("──── %s ETL completed in %.1fs ────", name, elapsed)
        except Exception as e:
            elapsed = time.time() - start
            results[name] = f"failed: {e} ({elapsed:.1f}s)"
            logger.exception("──── %s ETL FAILED ────", name)

    return results


def run_single(source: str) -> str:
    """Run a single ETL pipeline by name."""
    module_map = dict(PIPELINE_MODULES)
    if source not in module_map:
        available = ", ".join(module_map.keys())
        raise ValueError(f"Unknown source '{source}'. Available: {available}")

    module = module_map[source]
    start = time.time()
    try:
        module.run()
        elapsed = time.time() - start
        return f"success ({elapsed:.1f}s)"
    except Exception as e:
        elapsed = time.time() - start
        return f"failed: {e} ({elapsed:.1f}s)"
