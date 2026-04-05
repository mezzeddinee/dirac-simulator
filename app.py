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

    use_live_ci = os.getenv("SIMULATOR_USE_LIVE_CI", "0").lower() in {"1", "true", "yes"}
    ci_provider = None
    if use_live_ci:
        conf_path = Path(os.getenv("SIMULATOR_CI_CONF", str(base / "cim.conf")))
        token = os.getenv("SIMULATOR_CI_TOKEN")
        email = os.getenv("CIM_EMAIL")
        password = os.getenv("CIM_PASSWORD")
        if conf_path.exists():
            ci_provider = MidpointCIProvider.from_config(
                conf_path=conf_path,
                email=email,
                password=password,
                token=token,
            )

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
