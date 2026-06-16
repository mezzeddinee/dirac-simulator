from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List

try:
    from .ci_provider import MidpointCIProvider
    from .csv_io import load_jobs, load_sites
    from .metrics import print_summary
    from .policy import ReplayCarbonPolicy
    from .report_plots import save_report_plots
    from .simulator import ReplaySimulator
except ImportError:  # direct script-style execution fallback
    from ci_provider import MidpointCIProvider
    from csv_io import load_jobs, load_sites
    from metrics import print_summary
    from policy import ReplayCarbonPolicy
    from report_plots import save_report_plots
    from simulator import ReplaySimulator

logger = logging.getLogger(__name__)


def _parse_green_fraction(raw: str) -> float:
    value = float(raw.strip())
    if value > 1.0:
        value = value / 100.0
    if value < 0.0 or value > 1.0:
        raise ValueError("green fraction must be between 0.0 and 1.0, or 0 and 100")
    return value


def _parse_green_fractions(raw: str) -> List[float]:
    fractions = [_parse_green_fraction(part) for part in raw.split(",") if part.strip()]
    if not fractions:
        raise ValueError("SIMULATOR_GREEN_FRACTIONS must contain at least one value")
    return fractions


def _run_once(base: Path, tick_minutes: int, green_fraction: float, save_plots: bool) -> Dict[str, float]:
    logger.info("run start base=%s tick=%d green_fraction=%.2f", base, tick_minutes, green_fraction)
    random_seed_raw = os.getenv("SIMULATOR_RANDOM_SEED")
    random_seed = int(random_seed_raw) if random_seed_raw else None

    sites = load_sites(base / "sites.csv")
    jobs = load_jobs(base / "trace6")
    logger.info("input loaded sites=%d jobs=%d", len(sites), len(jobs))

    conf_path = base / "cim.conf"
    token = None
    ci_provider = MidpointCIProvider.from_config(
        conf_path=conf_path,
        token=token,
    )
    logger.info("ci provider configured conf=%s", conf_path)
    policy = ReplayCarbonPolicy(green_fraction=green_fraction, random_seed=random_seed)
    logger.info("policy configured green_fraction=%.2f", green_fraction)

    sim = ReplaySimulator(
        sites=sites,
        jobs=jobs,
        tick_minutes=tick_minutes,
        policy=policy,
        ci_provider=ci_provider,
    )

    steps = 0
    heartbeat_every = 1000
    while not sim.done():
        sim.step()
        steps += 1
        if steps % heartbeat_every == 0:
            waiting = len(sim.waiting_jobs())
            active = sim.active_jobs()
            done = len(sim.done_jobs)
            logger.info(
                "heartbeat steps=%d t=%s waiting=%d active=%d done=%d",
                steps,
                sim.current_time.isoformat(),
                waiting,
                active,
                done,
            )

    logger.info("run done steps=%d done_jobs=%d", steps, len(sim.done_jobs))
    summary = print_summary(sim.done_jobs)
    if save_plots:
        save_report_plots(sim, out_dir=base / "plots")
    return summary


def _print_fraction_table(results: List[Dict[str, float]]) -> None:
    print("")
    print("Green fraction | Throughput (jobs/hour) | Total CO2 (kg)")
    print("-------------- | ---------------------- | --------------")
    for row in results:
        print(
            f"{row['green_fraction'] * 100:>13.0f}% | "
            f"{row['throughput']:>22.8f} | "
            f"{row['total_carbon']:>14.8f}"
        )


def run(base: Path, tick_minutes: int = 1) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    sweep_raw = os.getenv("SIMULATOR_GREEN_FRACTIONS")
    if sweep_raw:
        results: List[Dict[str, float]] = []
        for fraction in _parse_green_fractions(sweep_raw):
            summary = _run_once(base, tick_minutes, fraction, save_plots=False)
            summary["green_fraction"] = fraction
            results.append(summary)
        _print_fraction_table(results)
        return

    fraction_raw = os.getenv("SIMULATOR_GREEN_FRACTION", 8)
    if fraction_raw is None:
        fraction_raw = os.getenv("SIMULATOR_GREEN", "1")
    green_fraction = _parse_green_fraction(fraction_raw)
    _run_once(base, tick_minutes, green_fraction, save_plots=True)
