from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from .models import Job, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Site

logger = logging.getLogger(__name__)


def _first_nonempty(row: Dict[str, str], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return ""


def load_sites(path: Path) -> Dict[str, Site]:
    sites: Dict[str, Site] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row["site"].strip()
            sites[name] = Site(
                name=name,
                max_running_jobs=int(row.get("max_running_jobs", row.get("max_pilots", 0))),
                e_fixed=float(row.get("e_fixed", 0.0)),
                latitude=float(row["latitude"]) if row.get("latitude") else None,
                longitude=float(row["longitude"]) if row.get("longitude") else None,
                avg_tdp_w=float(row.get("avg_tdp_w", 150.0)),
                avg_total_cores=float(row.get("avg_total_cores", 12.0)),
                perf_hs06=float(row.get("perf_hs06", 1.0)),
                avg_wallclock_cpu_ratio=float(row.get("avg_wallclock_cpu_ratio", 1.0)),
            )
    logger.info("sites loaded path=%s count=%d", path, len(sites))
    return sites


def load_jobs(path: Path) -> List[Job]:
    jobs: List[Job] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            wallclock_raw = _first_nonempty(
                row,
                ("wallclock", "wallclocktime", "WallClockTime", "WallClockTime(s)"),
            )
            if not wallclock_raw:
                runtime_min_raw = _first_nonempty(row, ("runtime_min",))
                wallclock_raw = str(float(runtime_min_raw) * 60.0) if runtime_min_raw else ""
            cpu_norm_factor_raw = _first_nonempty(
                row,
                ("CPUNormFactor", "CPUNormalizationFactor", "cpunormlazationfactor"),
            )
            jobs.append(
                Job(
                    job_id=row["job_id"].strip(),
                    submit_time=datetime.fromisoformat(row["submit_time"].strip()),
                    norm_cpu_seconds=float(
                        _first_nonempty(row, ("norm_cpu_seconds", "cpu_seconds", "NormCPUTime(s)")) or 0.0
                    ),
                    cores_used=int(row.get("cores_used", 1)),
                    wallclock=float(wallclock_raw) if wallclock_raw else 0.0,
                    cpu_norm_factor=float(cpu_norm_factor_raw) if cpu_norm_factor_raw else 1.0,
                )
            )
    logger.info("jobs loaded path=%s count=%d", path, len(jobs))
    return jobs


def load_ci(path: Path) -> Dict[str, List[Tuple[datetime, float]]]:
    out: Dict[str, List[Tuple[datetime, float]]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = datetime.fromisoformat(row["timestamp"].strip())
            site = row["site"].strip()
            ci = float(row["ci_gco2_per_kwh"])
            out.setdefault(site, []).append((t, ci))
    for site in out:
        out[site].sort(key=lambda x: x[0])
    logger.info("ci series loaded path=%s sites=%d", path, len(out))
    return out
