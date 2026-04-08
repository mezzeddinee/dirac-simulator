from __future__ import annotations

from typing import List

try:
    from .models import Job
except ImportError:  # direct script-style execution fallback
    from models import Job


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    idx = int((len(values) - 1) * p)
    return values[idx]


def print_summary(done_jobs: List[Job]) -> None:
    waits: List[float] = []
    turns: List[float] = []
    carbons: List[float] = []

    for j in done_jobs:
        if j.start_time is not None:
            waits.append((j.start_time - j.submit_time).total_seconds() / 60.0)
        if j.finish_time is not None:
            turns.append((j.finish_time - j.submit_time).total_seconds() / 60.0)
        carbons.append(j.carbon_kg)

    waits.sort()
    turns.sort()
    avg_carbon = sum(carbons) / len(carbons) if carbons else 0.0

    print(f"Completed jobs: {len(done_jobs)}")
    print(
        "Wait min p50/p90/p95/p99:",
        f"{percentile(waits,0.50):.1f}/{percentile(waits,0.90):.1f}/{percentile(waits,0.95):.1f}/{percentile(waits,0.99):.1f}",
    )
    print(
        "Turnaround min p50/p90/p95/p99:",
        f"{percentile(turns,0.50):.1f}/{percentile(turns,0.90):.1f}/{percentile(turns,0.95):.1f}/{percentile(turns,0.99):.1f}",
    )
    print(f"Average carbon/job (kgCO2): {avg_carbon:.4f}")
