from __future__ import annotations

import logging
import os
from pathlib import Path

try:
    from .ci_provider import MidpointCIProvider
    from .csv_io import load_ci, load_jobs, load_sites
    from .metrics import print_summary
    from .simulator import ReplaySimulator
except ImportError:  # direct script-style execution fallback
    from ci_provider import MidpointCIProvider
    from csv_io import load_ci, load_jobs, load_sites
    from metrics import print_summary
    from simulator import ReplaySimulator

logger = logging.getLogger(__name__)


def run(base: Path, tick_minutes: int = 1, guard_steps: int = 20000) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    logger.info("run start base=%s tick=%d guard=%d", base, tick_minutes, guard_steps)
    sites = load_sites(base / "sites.csv")
    jobs = load_jobs(base / "trace.csv")
    ci = load_ci(base / "site_ci.csv")
    logger.info("input loaded sites=%d jobs=%d ci_sites=%d", len(sites), len(jobs), len(ci))

    use_live_ci = os.getenv("SIMULATOR_USE_LIVE_CI", "1").lower() in {"1", "true", "yes"}
    ci_provider = None
    if use_live_ci:
        conf_path = Path(os.getenv("SIMULATOR_CI_CONF", str(base / "cim.conf")))
        token = None #os.getenv("SIMULATOR_CI_TOKEN")
        email = "ezzeddine@cppm.in2p3.fr" #os.getenv("CIM_EMAIL")
        password = "5379a25ab32a0322f50ff973" #os.getenv("CIM_PASSWORD")
        if conf_path.exists():
            ci_provider = MidpointCIProvider.from_config(
                conf_path=conf_path,
                email=email,
                password=password,
                token=token,
            )
            logger.info("live ci enabled conf=%s", conf_path)
        else:
            logger.info("live ci requested but conf missing: %s", conf_path)
    else:
        logger.info("live ci disabled")

    sim = ReplaySimulator(
        sites=sites,
        jobs=jobs,
        ci_series=ci,
        tick_minutes=tick_minutes,
        ci_provider=ci_provider,
    )

    steps = 0
    while not sim.done() and steps < guard_steps:
        sim.step()
        steps += 1

    logger.info("run done steps=%d done_jobs=%d", steps, len(sim.done_jobs))
    print_summary(sim.done_jobs)
