from __future__ import annotations

import csv
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

logger = logging.getLogger(__name__)


def _times_and_values(series: List[Tuple[datetime, int]]) -> tuple[List[datetime], List[int]]:
    if not series:
        return [], []
    times = [t for t, _ in series]
    values = [v for _, v in series]
    return times, values


def _rows_from_site_series(
    series: List[Tuple[datetime, Dict[str, int]]], site_names: List[str]
) -> List[Dict[str, int | str]]:
    rows: List[Dict[str, int | str]] = []
    for t, by_site in series:
        row: Dict[str, int | str] = {"timestamp": t.isoformat()}
        for site in site_names:
            row[site] = int(by_site.get(site, 0))
        rows.append(row)
    return rows


def _write_site_csv(path: Path, rows: List[Dict[str, int | str]], site_names: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", *site_names])
        writer.writeheader()
        writer.writerows(rows)


def _write_waiting_csv(path: Path, series: List[Tuple[datetime, int]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "waiting_jobs"])
        for t, value in series:
            writer.writerow([t.isoformat(), value])


def _compute_submit_time_metrics(jobs: Iterable) -> tuple[List[datetime], List[int], List[float]]:
    arrivals: Dict[datetime, int] = defaultdict(int)
    runtime_sum: Dict[datetime, float] = defaultdict(float)

    for job in jobs:
        minute = job.submit_time.replace(second=0, microsecond=0)
        arrivals[minute] += 1
        runtime_sum[minute] += float(job.wallclock) / 60.0

    minutes = sorted(arrivals.keys())
    return (
        minutes,
        [arrivals[m] for m in minutes],
        [runtime_sum[m] for m in minutes],
    )


def save_report_plots(sim, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    site_names = sorted(sim.sites.keys())

    waiting_series = sim.waiting_history
    submissions_series = sim.submissions_history
    running_series = sim.running_history

    waiting_t, waiting_v = _times_and_values(waiting_series)
    running_rows = _rows_from_site_series(running_series, site_names)
    submission_rows = _rows_from_site_series(submissions_series, site_names)

    # Save companion CSV files.
    _write_waiting_csv(out_dir / "waiting_over_time.csv", waiting_series)
    _write_site_csv(out_dir / "jobs_submitted_per_site_over_time.csv", submission_rows, site_names)
    _write_site_csv(out_dir / "jobs_running_per_site_over_time.csv", running_rows, site_names)

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed; CSV telemetry exported, plot export skipped")
        return

    # 1) Waiting jobs over time.
    fig = plt.figure(figsize=(13, 4))
    ax = fig.add_subplot(1, 1, 1)
    ax.step(waiting_t, waiting_v, where="post", color="tab:green")
    ax.set_title("Waiting jobs over time")
    ax.set_ylabel("Waiting jobs")
    ax.set_xlabel("Time")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "waiting_jobs_over_time.png", dpi=150)
    plt.close(fig)

    # 2) Jobs submitted to each site over time (flow, stacked).
    sub_times = [datetime.fromisoformat(r["timestamp"]) for r in submission_rows]
    sub_series = [[int(r[s]) for r in submission_rows] for s in site_names]
    fig = plt.figure(figsize=(13, 5))
    ax = fig.add_subplot(1, 1, 1)
    ax.stackplot(sub_times, sub_series, labels=site_names, alpha=0.85)
    ax.set_title("Jobs submitted to each site over time")
    ax.set_ylabel("Jobs submitted")
    ax.set_xlabel("Time")
    ax.legend(loc="upper left", ncol=3, fontsize=8)
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "jobs_submitted_per_site_over_time.png", dpi=150)
    plt.close(fig)

    # 3) Jobs running in each site over time (tick).
    run_times = [datetime.fromisoformat(r["timestamp"]) for r in running_rows]
    fig = plt.figure(figsize=(13, 6))
    ax = fig.add_subplot(1, 1, 1)
    for site in site_names:
        ax.plot(run_times, [int(r[site]) for r in running_rows], linewidth=1.1, label=site)
    max_site_cap = max(int(s.max_running_jobs) for s in sim.sites.values()) if sim.sites else 0
    if max_site_cap > 0:
        ax.axhline(max_site_cap, color="black", linestyle="--", linewidth=1, label=f"Site max ({max_site_cap})")
    ax.set_title("Jobs running in each site over time (tick)")
    ax.set_ylabel("Running jobs")
    ax.set_xlabel("Time")
    ax.legend(loc="upper right", ncol=3, fontsize=8)
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "jobs_running_per_site_over_time.png", dpi=150)
    plt.close(fig)

    # 4) Additional figure: waiting vs total running jobs.
    total_running = [sum(int(r[s]) for s in site_names) for r in running_rows]
    fig = plt.figure(figsize=(13, 4))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(run_times, total_running, color="tab:blue", label="Total running jobs")
    if waiting_t and waiting_v:
        ax.plot(waiting_t, waiting_v, color="tab:red", label="Waiting jobs")
    ax.set_title("Total running jobs vs waiting jobs")
    ax.set_ylabel("Jobs")
    ax.set_xlabel("Time")
    ax.legend(loc="upper right")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "total_running_vs_waiting.png", dpi=150)
    plt.close(fig)

    # 5) Additional figure: arrivals and runtime sum per minute.
    minute_t, arrivals, runtime_sum = _compute_submit_time_metrics(sim.pending_jobs)
    fig = plt.figure(figsize=(13, 4))
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.step(minute_t, arrivals, where="post", color="tab:purple", label="Arrivals/min")
    ax1.set_ylabel("Arrivals/min", color="tab:purple")
    ax1.tick_params(axis="y", labelcolor="tab:purple")

    ax2 = ax1.twinx()
    ax2.step(minute_t, runtime_sum, where="post", color="tab:orange", label="Runtime sum (min/min)")
    ax2.set_ylabel("Runtime sum (min/min)", color="tab:orange")
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    ax1.set_title("Arrivals and runtime load per minute")
    ax1.set_xlabel("Time")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "arrivals_and_runtime_per_minute.png", dpi=150)
    plt.close(fig)

    logger.info("plots exported dir=%s", out_dir)
