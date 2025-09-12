# -*- coding: utf-8 -*-
"""
pipeline_runner.py
Orchestration du pipeline :
  1) staging_loader  (ingestion + préparation + upsert staging)
  2) dims_loader     (peuplement des dimensions)
  3) facts_loader    (upsert des faits)

Caractéristiques :
- YEAR propagé (ENV + argument)
- Logs homogènes et durées par étape
- Tolérant aux signatures main() / main(year)
- --stop-on-fail pour interrompre en cas d'erreur
"""

import os
import sys
import time
import argparse
import logging
from types import ModuleType
from typing import Optional
from datetime import datetime, timezone
from ops_logger import log_run


# ========= LOGGING =========
def setup_logging(level: str = "INFO") -> logging.Logger:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("pipeline")


# ========= HELPERS =========
def _call_module_main(mod: ModuleType, year: int, logger: logging.Logger) -> None:
    """
    Appelle mod.main(year) si possible, sinon mod.main()
    (pour compatibilité avec d'anciens modules).
    """
    if not hasattr(mod, "main"):
        raise AttributeError(
            f"Le module {mod.__name__} n'expose pas de fonction main().")

    main_fn = getattr(mod, "main")
    try:
        # Essai avec 'year'
        main_fn(year)
    except TypeError:
        # Fallback sans paramètre (le module lira YEAR via l'ENV)
        logger.debug(
            "Signature de %s.main ne prend pas 'year' → fallback main()", mod.__name__)
        main_fn()


def _run_step(title: str, mod: ModuleType, year: int, logger: logging.Logger, stop_on_fail: bool = False) -> float:
    logger.info("=== Début %s ===", title)
    started = datetime.now(timezone.utc)
    t0 = time.time()
    status = "OK"
    msg = None
    try:
        _call_module_main(mod, year, logger)
    except Exception as e:
        status = "KO"
        msg = str(e)
        logger.exception("Échec étape %s : %s", title, e)
        if stop_on_fail:
            finished = datetime.now(timezone.utc)
            raise
    finally:
        finished = datetime.now(timezone.utc)
        dt = time.time() - t0
        # rows_written: inconnu ici -> None (on suit la durée/état)
        log_run("pro_sante", title, started, finished,
                status, rows_written=None, message=msg)
        logger.info("=== Fin %s (%.1fs) ===", title, dt)
    return time.time() - t0


# ========= ENTRYPOINT =========
def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Runner du pipeline Ameli → Postgres")
    parser.add_argument("--year", type=int, required=True,
                        help="Année à traiter (ex: 2021)")
    parser.add_argument("--log-level", default="INFO",
                        help="Niveau de log (DEBUG, INFO, WARNING...)")
    parser.add_argument("--stop-on-fail", action="store_true",
                        help="Stopper le pipeline au premier échec")
    parser.add_argument("--skip-staging", action="store_true",
                        help="Sauter l'étape staging")
    parser.add_argument("--skip-dims", action="store_true",
                        help="Sauter l'étape dimensions")
    parser.add_argument("--skip-facts", action="store_true",
                        help="Sauter l'étape faits")

    args = parser.parse_args(argv)

    logger = setup_logging(args.log_level)

    # Pose l’ENV YEAR AVANT d’importer les modules (pour compat globale)
    os.environ["YEAR"] = str(args.year)

    # Imports après paramétrage de l'ENV
    try:
        import staging_loader
    except Exception as e:
        logger.exception("Impossible d'importer staging_loader : %s", e)
        sys.exit(1)

    try:
        import dims_loader
    except Exception as e:
        logger.exception("Impossible d'importer dims_loader : %s", e)
        sys.exit(1)

    try:
        import facts_loader
    except Exception as e:
        logger.exception("Impossible d'importer facts_loader : %s", e)
        sys.exit(1)

    logger.info("=== Démarrage pipeline ===")
    total_t0 = time.time()

    # STAGING
    if not args.skip_staging:
        _run_step("staging", staging_loader,
                  args.year, logger, args.stop_on_fail)
    else:
        logger.info("=== Étape staging SKIPPED ===")

    # DIMENSIONS
    if not args.skip_dims:
        _run_step("dimensions", dims_loader,
                  args.year, logger, args.stop_on_fail)
    else:
        logger.info("=== Étape dimensions SKIPPED ===")

    # FAITS
    if not args.skip_facts:
        _run_step("faits", facts_loader, args.year, logger, args.stop_on_fail)
    else:
        logger.info("=== Étape faits SKIPPED ===")

    total_dt = time.time() - total_t0
    logger.info("Pipeline terminé sans erreur. (%.1fs)", total_dt)


if __name__ == "__main__":
    main()
