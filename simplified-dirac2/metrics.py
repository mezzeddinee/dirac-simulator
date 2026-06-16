from __future__ import annotations

import logging
from collections import Counter
from typing import Dict, List

try:
    from .models import Job
except ImportError:  # direct script-style execution fallback
    from models import Job

logger = logging.getLogger(__name__)


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    idx = int((len(values) - 1) * p)
    return values[idx]


def summarize(done_jobs: List[Job]) -> Dict[str, float]:
    waits: List[float] = []
    turns: List[float] = []
    carbons: List[float] = []
    total_norm_cpu_seconds = 0.0

    for j in done_jobs:
        if j.start_time is not None:
            waits.append((j.start_time - j.submit_time).total_seconds() / 60.0)
        if j.finish_time is not None:
            turns.append((j.finish_time - j.submit_time).total_seconds() / 60.0)
        carbons.append(j.carbon_kg)
        total_norm_cpu_seconds += j.norm_cpu_seconds

    waits.sort()
    turns.sort()
    total_carbon = sum(carbons)
    avg_carbon = sum(carbons) / len(carbons) if carbons else 0.0
    first_submit = min((j.submit_time for j in done_jobs), default=None)
    last_finish = max((j.finish_time for j in done_jobs if j.finish_time is not None), default=None)
    elapsed_hours = 0.0
    if first_submit is not None and last_finish is not None:
        elapsed_hours = (last_finish - first_submit).total_seconds() / 3600.0
    throughput = len(done_jobs) / elapsed_hours if elapsed_hours > 0.0 else 0.0

    return {
        "jobs": float(len(done_jobs)),
        "wait_p50": percentile(waits, 0.50),
        "wait_p90": percentile(waits, 0.90),
        "wait_p95": percentile(waits, 0.95),
        "wait_p99": percentile(waits, 0.99),
        "turnaround_p50": percentile(turns, 0.50),
        "turnaround_p90": percentile(turns, 0.90),
        "turnaround_p95": percentile(turns, 0.95),
        "turnaround_p99": percentile(turns, 0.99),
        "total_carbon": total_carbon,
        "total_norm_cpu_seconds": total_norm_cpu_seconds,
        "avg_carbon": avg_carbon,
        "throughput": throughput,
    }


def print_summary(done_jobs: List[Job]) -> Dict[str, float]:
    summary = summarize(done_jobs)
    site_counts: Counter[str] = Counter(j.site for j in done_jobs if j.site)

    logger.info(
        "summary jobs=%d avg_carbon=%.8f total_carbon=%.8f total_norm_cpu_seconds=%.2f",
        int(summary["jobs"]),
        summary["avg_carbon"],
        summary["total_carbon"],
        summary["total_norm_cpu_seconds"],
    )

    print(f"Completed jobs: {int(summary['jobs'])}")
    print(
        "Wait min p50/p90/p95/p99:",
        f"{summary['wait_p50']:.6f}/{summary['wait_p90']:.6f}/{summary['wait_p95']:.6f}/{summary['wait_p99']:.6f}",
    )
    print(
        "Turnaround min p50/p90/p95/p99:",
        (
            f"{summary['turnaround_p50']:.6f}/{summary['turnaround_p90']:.6f}/"
            f"{summary['turnaround_p95']:.6f}/{summary['turnaround_p99']:.6f}"
        ),
    )
    print(f"Throughput (jobs/hour): {summary['throughput']:.8f}")
    print(f"Total CFP (kgCO2): {summary['total_carbon']:.8f}")
    print(f"Total normalized CPU (s): {summary['total_norm_cpu_seconds']:.2f}")
    print(f"Average carbon/job (kgCO2): {summary['avg_carbon']:.8f}")
    print("Notes:")
    print("- Wait = start_time - submit_time (queue delay), in minutes.")
    print("- Turnaround = finish_time - submit_time (wait + execution), in minutes.")
    print("- p50/p90/p95/p99 are percentile cutoffs over completed jobs.")
    print("- Throughput = completed jobs divided by elapsed simulated hours.")
    print("- Average carbon/job is the mean simulated job emissions in kgCO2.")
    print("Jobs executed per site:")
    for site, count in sorted(site_counts.items()):
        print(f"- {site}: {count}")
    return summary
