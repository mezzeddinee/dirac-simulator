from __future__ import annotations

from typing import Dict, List, Set, Tuple

try:
    from .models import Job, Pilot, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Pilot, Site


class ReplayCarbonPolicy:
    def __init__(self, beta: float = 0.5, gamma: float = 0.5):
        self.beta = beta
        self.gamma = gamma
        self.pid = 0

    def normalize(self, d: Dict[str, float]) -> Dict[str, float]:
        vals = list(d.values())
        if max(vals) == min(vals):
            return {k: 0.0 for k in d}
        lo = min(vals)
        hi = max(vals)
        return {k: (v - lo) / (hi - lo) for k, v in d.items()}

    def estimate_e(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # E: carbon-efficiency proxy per site (static in this version).
        # Lower E means "greener / more carbon-efficient" and is preferred.
        # It comes from site.e_fixed and is normalized before ranking.
        return {name: s.e_fixed for name, s in sites.items()}

    def estimate_d(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # D: delay/congestion signal per site.
        # Intuition: if many pilots are already waiting/starting relative to running,
        # new jobs are more likely to wait or see slower pickup.
        # Higher D means "more congested / higher expected delay".
        d = {}
        for name, s in sites.items():
            running = len(s.running())
            waiting = len(s.starting()) + len(s.idle())
            d[name] = waiting / (running + 1)
        return d

    def compatible(self, pilot_tags: Set[str], job: Job) -> bool:
        if not job.required_all_tags.issubset(pilot_tags):
            return False
        if job.required_any_tags and not (job.required_any_tags & pilot_tags):
            return False
        return True

    def unmet_jobs(self, waiting_jobs: List[Job], sites: Dict[str, Site]) -> List[Job]:
        idle_tags = [set(p.tags) for s in sites.values() for p in s.idle()]
        unmet: List[Job] = []

        for job in waiting_jobs:
            assigned = False
            for i, tags in enumerate(idle_tags):
                if self.compatible(tags, job):
                    idle_tags.pop(i)
                    assigned = True
                    break
            if not assigned:
                unmet.append(job)
        return unmet

    def site_can_run_job(self, site: Site, job: Job) -> bool:
        return self.compatible(site.tags, job)

    def schedule(self, waiting_jobs: List[Job], sites: Dict[str, Site]) -> List[Tuple[str, int]]:
        unmet = self.unmet_jobs(waiting_jobs, sites)
        demand = len(unmet)
        if demand <= 0:
            return []

        e_norm = self.normalize(self.estimate_e(sites))
        # First-trial simplified ranking: sort only by E (lower is better).
        scored = sorted(sites.values(), key=lambda s: e_norm[s.name])

        # Previous combined E+D scoring (kept for later use):
        # score = -beta * E + gamma * D
        # -beta * E pushes scheduling toward carbon-efficient sites
        # +gamma * D penalizes sites where jobs are expected to wait more
        # d_norm = self.normalize(self.estimate_d(sites))
        # scored = sorted(
        #     sites.values(),
        #     key=lambda s: -self.beta * e_norm[s.name] + self.gamma * d_norm[s.name],
        # )

        submissions: List[Tuple[str, int]] = []
        for site in scored:
            avail = site.available()
            if avail <= 0:
                continue

            coverable = [j for j in unmet if self.site_can_run_job(site, j)]
            if not coverable:
                continue

            x = min(demand, avail, len(coverable))
            if x <= 0:
                continue

            submissions.append((site.name, x))
            for j in coverable[:x]:
                unmet.remove(j)
            demand -= x
            if demand == 0:
                break

        return submissions

    def new_pilot(self, site: Site) -> Pilot:
        self.pid += 1
        return Pilot(
            pilot_id=f"P{self.pid}",
            site=site.name,
            tags=set(site.tags),
            startup_left_min=site.startup_delay_min,
            jobs_left=site.jobs_per_pilot,
        )
