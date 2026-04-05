from __future__ import annotations

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


def run(base: Path, tick_minutes: int = 1, guard_steps: int = 20000) -> None:
    sites = load_sites(base / "sites.csv")
    jobs = load_jobs(base / "jobs.csv")
    ci = load_ci(base / "site_ci.csv")

    use_live_ci = os.getenv("SIMPLE4_USE_LIVE_CI", "0").lower() in {"1", "true", "yes"}
    ci_provider = None
    if use_live_ci:
        token = os.getenv("SIMPLE4_CI_TOKEN", "")
        api_base = os.getenv("SIMPLE4_CI_API_BASE", "")
        if token and api_base:
            ci_provider = MidpointCIProvider(token=token, kpi_api_base=api_base)

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

    print_summary(sim.done_jobs)
