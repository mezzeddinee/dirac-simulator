from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

try:
    from .models import Job, Pilot, Site
    from .policy import ReplayCarbonPolicy
except ImportError:  # direct script-style execution fallback
    from models import Job, Pilot, Site
    from policy import ReplayCarbonPolicy


class ReplaySimulator:
    def __init__(
        self,
        sites: Dict[str, Site],
        jobs: List[Job],
        ci_series: Dict[str, List[Tuple[datetime, float]]],
        tick_minutes: int = 1,
        policy: Optional[ReplayCarbonPolicy] = None,
        ci_provider: Optional[Any] = None,
    ):
        self.sites = sites
        self.pending_jobs = sorted(jobs, key=lambda j: j.submit_time)
        self.ci_series = ci_series
        self.policy = policy or ReplayCarbonPolicy()
        self.ci_provider = ci_provider
        self.tick = timedelta(minutes=tick_minutes)
        self.current_time = min(j.submit_time for j in jobs)
        self.done_jobs: List[Job] = []
        self.idle_consumption_factor = 0.4

    def waiting_jobs(self) -> List[Job]:
        return [j for j in self.pending_jobs if j.status == "waiting"]

    def active_pilots(self) -> int:
        return sum(len(s.pilots) for s in self.sites.values())

    def ci_at(self, site: str, ts: datetime) -> float:
        series = self.ci_series.get(site, [])
        if not series:
            return 300.0
        value = series[0][1]
        for t, ci in series:
            if t > ts:
                break
            value = ci
        return value

    def release_jobs(self) -> None:
        for j in self.pending_jobs:
            if j.status == "pending" and j.submit_time <= self.current_time:
                j.activate()

    def step_start(self) -> None:
        for s in self.sites.values():
            for p in s.pilots:
                if p.status == "starting":
                    p.startup_left_min -= 1
                    if p.startup_left_min <= 0:
                        p.status = "idle"

    def compatible(self, pilot: Pilot, job: Job) -> bool:
        return self.policy.compatible(pilot.tags, job)

    def ci_for_job(self, site: Site, job: Job) -> float:
        runtime_for_midpoint = job.assigned_runtime_min if job.assigned_runtime_min > 0 else job.runtime_min
        midpoint = job.submit_time + timedelta(minutes=(runtime_for_midpoint / 2.0))
        if self.ci_provider is not None:
            return self.ci_provider.get_ci(
                site_name=site.name,
                midpoint_ts=midpoint,
                latitude=site.latitude,
                longitude=site.longitude,
            )
        return self.ci_at(site.name, midpoint)

    def derive_job_runtime_for_site(self, job: Job, site: Site) -> Tuple[float, float, int]:
        perf = float(site.perf_hs06)
        if perf <= 0.0:
            return 0.0, 0.0, 0
        cpu_seconds_sim = float(job.norm_cpu_seconds) / perf
        wc_ratio = float(site.avg_wallclock_cpu_ratio)
        if wc_ratio <= 0.0:
            wc_ratio = 1.0
        wallclock_seconds_sim = max(cpu_seconds_sim * wc_ratio, 60.0)
        runtime_min_sim = max(1, int(math.ceil(wallclock_seconds_sim / 60.0)))
        return cpu_seconds_sim, wallclock_seconds_sim, runtime_min_sim

    def compute_energy_kwh(self, job: Job, site: Site) -> float:
        cpu_seconds, wallclock_seconds, _ = self.derive_job_runtime_for_site(job, site)
        total_cores = int(site.avg_total_cores)
        cores_used = int(job.cores_used)
        tdp = float(site.avg_tdp_w)
        if wallclock_seconds <= 0.0 or total_cores <= 0:
            return 0.0

        f = max(0.0, min(1.0, float(self.idle_consumption_factor)))
        effective_time_s = (1.0 - f) * cpu_seconds + f * wallclock_seconds
        core_fraction = float(cores_used) / float(total_cores)
        energy_joule = effective_time_s * core_fraction * tdp
        return energy_joule / 3_600_000.0

    def step_match(self) -> None:
        idle = [p for s in self.sites.values() for p in s.idle()]
        waiting = sorted(self.waiting_jobs(), key=lambda j: (j.submit_time, j.job_id))

        for p in idle:
            picked: Optional[Job] = None
            for j in waiting:
                if self.compatible(p, j):
                    picked = j
                    break
            if picked is None:
                continue

            p.status = "running"
            p.job = picked
            picked.status = "running"
            picked.start_time = self.current_time
            picked.site = p.site
            site = self.sites[p.site]
            cpu_s, wall_s, runtime_s = self.derive_job_runtime_for_site(picked, site)
            picked.assigned_cpu_seconds = cpu_s
            picked.assigned_wallclock_seconds = wall_s
            picked.assigned_runtime_min = runtime_s
            if runtime_s > 0:
                picked.remaining_min = runtime_s
            total_kwh = self.compute_energy_kwh(picked, site)
            if picked.assigned_runtime_min > 0:
                picked.energy_per_min_kwh = total_kwh / float(picked.assigned_runtime_min)
            else:
                picked.energy_per_min_kwh = 0.0
            picked.assigned_ci_gco2_per_kwh = self.ci_for_job(site, picked)
            waiting.remove(picked)

    def step_execute(self) -> None:
        for s in self.sites.values():
            for p in list(s.running()):
                j = p.job
                if j is None:
                    continue

                j.carbon_kg += (j.energy_per_min_kwh * j.assigned_ci_gco2_per_kwh) / 1000.0
                j.remaining_min -= 1

                if j.remaining_min <= 0:
                    j.status = "done"
                    j.finish_time = self.current_time + self.tick
                    self.done_jobs.append(j)
                    p.jobs_left -= 1
                    p.job = None
                    if p.jobs_left > 0:
                        p.status = "idle"
                    else:
                        s.pilots.remove(p)

    def step_schedule(self) -> None:
        submissions = self.policy.schedule(self.waiting_jobs(), self.sites)
        for site_name, k in submissions:
            site = self.sites[site_name]
            for _ in range(k):
                site.pilots.append(self.policy.new_pilot(site))

    def step(self) -> None:
        # Move jobs whose submit_time has arrived from pending -> waiting.
        self.release_jobs()
        # Decrease startup delay and promote ready pilots from starting -> idle.
        self.step_start()
        # Global late-binding: idle pilots pick the first compatible waiting job.
        self.step_match()
        # Execute one tick of running jobs, accumulate carbon, finalize completed jobs.
        self.step_execute()
        # Provision new pilots for compatibility-aware unmet demand.
        self.step_schedule()
        # Advance simulation clock by one tick (1 minute by default).
        self.current_time += self.tick

    def done(self) -> bool:
        all_jobs_done = all(j.status == "done" for j in self.pending_jobs)
        no_active = self.active_pilots() == 0
        return all_jobs_done and no_active
