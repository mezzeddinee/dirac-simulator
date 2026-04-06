from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Set


@dataclass
class Job:
    job_id: str
    tq: str
    submit_time: datetime
    runtime_min: int
    required_all_tags: Set[str] = field(default_factory=set)
    required_any_tags: Set[str] = field(default_factory=set)
    norm_cpu_seconds: float = 0.0
    cores_used: int = 1

    status: str = "pending"  # pending/waiting/running/done
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    site: Optional[str] = None
    remaining_min: int = 0
    carbon_kg: float = 0.0
    energy_per_min_kwh: float = 0.0
    assigned_ci_gco2_per_kwh: float = 0.0
    assigned_cpu_seconds: float = 0.0
    assigned_wallclock_seconds: float = 0.0
    assigned_runtime_min: int = 0

    def activate(self) -> None:
        self.status = "waiting"
        self.remaining_min = self.runtime_min


@dataclass
class Pilot:
    pilot_id: str
    site: str
    tags: Set[str]
    startup_left_min: int
    jobs_left: int
    status: str = "starting"  # starting/idle/running
    job: Optional[Job] = None


@dataclass
class Site:
    name: str
    max_pilots: int
    startup_delay_min: int
    jobs_per_pilot: int
    e_fixed: float
    latitude: Optional[float]
    longitude: Optional[float]
    avg_tdp_w: float
    avg_total_cores: int
    perf_hs06: float
    avg_wallclock_cpu_ratio: float
    tags: Set[str]
    pilots: list[Pilot] = field(default_factory=list)

    def available(self) -> int:
        return self.max_pilots - len(self.pilots)

    def running(self) -> list[Pilot]:
        return [p for p in self.pilots if p.status == "running"]

    def idle(self) -> list[Pilot]:
        return [p for p in self.pilots if p.status == "idle"]

    def starting(self) -> list[Pilot]:
        return [p for p in self.pilots if p.status == "starting"]
