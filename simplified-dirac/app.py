from __future__ import annotations

import logging
import os
from pathlib import Path

try:
    from .ci_provider import MidpointCIProvider
    from .csv_io import load_jobs, load_sites
    from .metrics import print_summary
    from .policy import ReplayCarbonPolicy
    from .simulator import ReplaySimulator
except ImportError:  # direct script-style execution fallback
    from ci_provider import MidpointCIProvider
    from csv_io import load_jobs, load_sites
    from metrics import print_summary
    from policy import ReplayCarbonPolicy
    from simulator import ReplaySimulator

logger = logging.getLogger(__name__)


def run(base: Path, tick_minutes: int = 1, guard_steps: int = 20000) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    logger.info("run start base=%s tick=%d guard=%d", base, tick_minutes, guard_steps)
    sites = load_sites(base / "sites.csv")
    jobs = load_jobs(base / "trace.csv")
    logger.info("input loaded sites=%d jobs=%d", len(sites), len(jobs))

    conf_path = base / "cim.conf"
    token = None
    ci_provider = MidpointCIProvider.from_config(
        conf_path=conf_path,
        token=token,
    )
    logger.info("ci provider configured conf=%s", conf_path)
    green = int(os.getenv("SIMULATOR_GREEN", "0"))
    policy = ReplayCarbonPolicy(green=green)
    logger.info("policy configured green=%d", green)

    sim = ReplaySimulator(
        sites=sites,
        jobs=jobs,
        ci_series={},
        tick_minutes=tick_minutes,
        policy=policy,
        ci_provider=ci_provider,
    )

    steps = 0
    while not sim.done() and steps < guard_steps:
        sim.step()
        steps += 1

    logger.info("run done steps=%d done_jobs=%d", steps, len(sim.done_jobs))
    print_summary(sim.done_jobs)
