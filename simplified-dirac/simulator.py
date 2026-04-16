from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from .models import Job, Site
    from .policy import ReplayCarbonPolicy
except ImportError:  # direct script-style execution fallback
    from models import Job, Site
    from policy import ReplayCarbonPolicy

logger = logging.getLogger(__name__)


class ReplaySimulator:
    def __init__(
        self,
        sites: Dict[str, Site],
        jobs: List[Job],
        tick_minutes: int = 1,
        policy: Optional[ReplayCarbonPolicy] = None,
        ci_provider: Any = None,
    ):
        self.sites = sites
        self.pending_jobs = sorted(jobs, key=lambda j: j.submit_time)
        self.policy = policy or ReplayCarbonPolicy()
        self.ci_provider = ci_provider
        if self.ci_provider is None:
            raise ValueError("ci_provider is required")
        self.tick = timedelta(minutes=tick_minutes)
        self.current_time = min(j.submit_time for j in jobs)
        self.done_jobs: List[Job] = []
        self.idle_consumption_factor = 0.4
        logger.info(
            "sim init sites=%d jobs=%d tick_min=%d",
            len(self.sites),
            len(self.pending_jobs),
            tick_minutes,
        )

    def waiting_jobs(self) -> List[Job]:
        return [j for j in self.pending_jobs if j.status == "waiting"]

    def active_jobs(self) -> int:
        return sum(len(s.running_jobs) for s in self.sites.values())

    def release_jobs(self) -> None:
        # Check all jobs that are not released yet.
        # If their submit time is now, put them in the waiting queue.
        released = 0
        for j in self.pending_jobs:
            if j.status == "pending" and j.submit_time <= self.current_time:
                j.activate()
                released += 1
        if released:
            logger.info("release t=%s jobs=%d", self.current_time.isoformat(), released)

    def ci_for_job(self, site: Site, job: Job) -> float:
        if job.assigned_runtime_min > 0:
            runtime_for_midpoint = job.assigned_runtime_min
        else:
            _, _, runtime_for_midpoint = self.derive_job_runtime_for_site(job, site)
        midpoint = job.submit_time + timedelta(minutes=(runtime_for_midpoint / 2.0))
        return self.ci_provider.get_ci(
            site_name=site.name,
            midpoint_ts=midpoint,
            latitude=site.latitude,
            longitude=site.longitude,
        )

    def derive_job_runtime_for_site(self, job: Job, site: Site) -> Tuple[float, float, int]:
        # perf_hs06 is a site speed factor:
        # higher perf => less CPU seconds needed for the same normalized workload.
        perf = float(site.perf_hs06)
        if perf <= 0.0:
            return 0.0, 0.0, 0
        cpu_seconds_sim = float(job.norm_cpu_seconds) / perf

        # Derive wallclock from job metadata and site performance.
        wallclock_seconds_sim = (float(job.wallclock) * float(job.cpu_norm_factor)) / perf
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
        # Take waiting jobs in a stable order (oldest first, then by id).
        waiting = sorted(self.waiting_jobs(), key=lambda j: (j.submit_time, j.job_id))
        # Ask the policy which site should take how many jobs now.
        submissions = self.policy.schedule(waiting, self.sites)
        if submissions:
            logger.info("match t=%s waiting=%d submissions=%s", self.current_time.isoformat(), len(waiting), submissions)

        for site_name, k in submissions:
            site = self.sites[site_name]
            quota = k

            # Start up to "quota" jobs on this site.
            for picked in list(waiting[:quota]):
                # Mark job as running and stamp start metadata.
                picked.status = "running"
                picked.start_time = self.current_time
                picked.site = site.name
                # Compute runtime and energy numbers for this site/job pair.
                cpu_seconds_sim, wallclock_seconds_sim, runtime_min_sim = self.derive_job_runtime_for_site(picked, site)
                picked.assigned_cpu_seconds = cpu_seconds_sim
                picked.assigned_wallclock_seconds = wallclock_seconds_sim
                picked.assigned_runtime_min = runtime_min_sim
                picked.remaining_min = runtime_min_sim
                total_kwh = self.compute_energy_kwh(picked, site)
                picked.total_energy_kwh = total_kwh
                picked.assigned_ci_gco2_per_kwh = self.ci_for_job(site, picked)
                picked.carbon_kg = (picked.total_energy_kwh * picked.assigned_ci_gco2_per_kwh) / 1000.0
                # Put job into site's running list and remove it from waiting list.
                site.running_jobs.append(picked)
                waiting.remove(picked)
                logger.debug("start job=%s site=%s rt=%d", picked.job_id, site.name, picked.assigned_runtime_min)

    def step_execute(self) -> None:
        # Run one simulation minute for every running job.
        finished = 0
        for site in self.sites.values():
            for job in list(site.running_jobs):
                # Job worked for one more minute.
                job.remaining_min -= 1
                if job.remaining_min > 0:
                    continue
                # Job is finished now: mark done and move it to completed list.
                job.status = "done"
                job.finish_time = self.current_time + self.tick
                self.done_jobs.append(job)
                site.running_jobs.remove(job)
                finished += 1
        if finished:
            logger.info("finish t=%s jobs=%d", self.current_time.isoformat(), finished)

    def step(self) -> None:
        logger.debug("step t=%s active=%d", self.current_time.isoformat(), self.active_jobs())
        # STEP 1: Release new jobs (pending -> waiting).
        self.release_jobs()
        # STEP 2: Match waiting jobs to sites and start them.
        self.step_match()
        # STEP 3: Execute one tick and finish jobs that reached 0 time left.
        self.step_execute()
        # STEP 4: Move clock forward by one tick (usually 1 minute).
        self.current_time += self.tick

    def done(self) -> bool:
        all_jobs_done = all(j.status == "done" for j in self.pending_jobs)
        no_active = self.active_jobs() == 0
        return all_jobs_done and no_active
