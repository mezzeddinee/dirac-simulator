from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

try:
    from .models import Job, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Site


def parse_tags(raw: str) -> Set[str]:
    if not raw:
        return set()
    return {x.strip() for x in raw.split("|") if x.strip()}


def load_sites(path: Path) -> Dict[str, Site]:
    sites: Dict[str, Site] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row["site"].strip()
            tags = parse_tags(row.get("tags", ""))
            tags.add(f"site:{name}")
            sites[name] = Site(
                name=name,
                max_pilots=int(row["max_pilots"]),
                startup_delay_min=int(row.get("startup_delay_min", 2)),
                jobs_per_pilot=int(row.get("jobs_per_pilot", 1)),
                e_fixed=float(row.get("e_fixed", 0.0)),
                latitude=float(row["latitude"]) if row.get("latitude") else None,
                longitude=float(row["longitude"]) if row.get("longitude") else None,
                avg_tdp_w=float(row.get("avg_tdp_w", 150.0)),
                avg_total_cores=int(row.get("avg_total_cores", 12)),
                tags=tags,
            )
    return sites


def load_jobs(path: Path) -> List[Job]:
    jobs: List[Job] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            jobs.append(
                Job(
                    job_id=row["job_id"].strip(),
                    tq=row["tq"].strip(),
                    submit_time=datetime.fromisoformat(row["submit_time"].strip()),
                    runtime_min=int(row["runtime_min"]),
                    required_all_tags=parse_tags(row.get("required_all_tags", "")),
                    required_any_tags=parse_tags(row.get("required_any_tags", "")),
                    cpu_seconds=float(row.get("cpu_seconds", 0.0)),
                    wallclock_seconds=float(row.get("wallclock_seconds", 0.0)),
                    cores_used=int(row.get("cores_used", 1)),
                )
            )
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
    return out
