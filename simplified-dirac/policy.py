from __future__ import annotations

from typing import Dict, List, Tuple

try:
    from .models import Job, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Site


class ReplayCarbonPolicy:
    def __init__(self, beta: float = 0.5, gamma: float = 0.5):
        self.beta = beta
        self.gamma = gamma

    def normalize(self, d: Dict[str, float]) -> Dict[str, float]:
        vals = list(d.values())
        if max(vals) == min(vals):
            return {k: 0.0 for k in d}
        lo = min(vals)
        hi = max(vals)
        return {k: (v - lo) / (hi - lo) for k, v in d.items()}

    def estimate_e(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # E: carbon-efficiency proxy per site (lower is better).
        return {name: s.e_fixed for name, s in sites.items()}

    def estimate_d(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # D: optional delay/congestion signal based on occupied capacity.
        d: Dict[str, float] = {}
        for name, s in sites.items():
            cap = max(1, s.max_running_jobs)
            d[name] = float(len(s.running_jobs)) / float(cap)
        return d

    def unmet_jobs(self, waiting_jobs: List[Job], sites: Dict[str, Site]) -> List[Job]:
        slots = {name: s.available_slots() for name, s in sites.items()}
        unmet: List[Job] = []

        for job in waiting_jobs:
            assigned = False
            for site_name, site in sites.items():
                if slots[site_name] <= 0:
                    continue
                slots[site_name] -= 1
                assigned = True
                break
            if not assigned:
                unmet.append(job)
        return unmet

    def schedule(self, waiting_jobs: List[Job], sites: Dict[str, Site]) -> List[Tuple[str, int]]:
        remaining = sorted(waiting_jobs, key=lambda j: (j.submit_time, j.job_id))
        demand = len(remaining)
        if demand <= 0:
            return []

        e_norm = self.normalize(self.estimate_e(sites))
        # First-trial simplified ranking: sort only by E (lower is better).
        scored = sorted(sites.values(), key=lambda s: e_norm[s.name])

        # Previous combined E+D scoring (kept for later use):
        # score = -beta * E + gamma * D
        # d_norm = self.normalize(self.estimate_d(sites))
        # scored = sorted(
        #     sites.values(),
        #     key=lambda s: -self.beta * e_norm[s.name] + self.gamma * d_norm[s.name],
        # )

        submissions: List[Tuple[str, int]] = []
        for site in scored:
            avail = site.available_slots()
            if avail <= 0:
                continue

            x = min(demand, avail)

            submissions.append((site.name, x))
            for j in remaining[:x]:
                remaining.remove(j)
            demand -= x
            if demand == 0:
                break

        return submissions
