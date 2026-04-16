from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Job:
    job_id: str
    submit_time: datetime
    norm_cpu_seconds: float = 0.0
    cores_used: int = 1
    wallclock: float = 0.0
    cpu_norm_factor: float = 1.0

    status: str = "pending"  # pending/waiting/running/done
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    site: Optional[str] = None
    remaining_min: int = 0
    carbon_kg: float = 0.0
    total_energy_kwh: float = 0.0
    assigned_ci_gco2_per_kwh: float = 0.0
    assigned_cpu_seconds: float = 0.0
    assigned_wallclock_seconds: float = 0.0
    assigned_runtime_min: int = 0

    def activate(self) -> None:
        self.status = "waiting"
        self.remaining_min = 0


@dataclass
class Site:
    name: str
    max_running_jobs: int
    e_fixed: float
    latitude: Optional[float]
    longitude: Optional[float]
    avg_tdp_w: float
    avg_total_cores: int
    perf_hs06: float
    avg_wallclock_cpu_ratio: float
    running_jobs: list[Job] = field(default_factory=list)

    def available_slots(self) -> int:
        return self.max_running_jobs - len(self.running_jobs)

    def running(self) -> list[Job]:
        return list(self.running_jobs)
