from __future__ import annotations

import logging
import random
from typing import Dict, List, Tuple

try:
    from .models import Job, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Site

logger = logging.getLogger(__name__)


class ReplayCarbonPolicy:
    def __init__(self, green: int = 1):
        self.green = int(green)

    def estimate_e(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # E: greenscore proxy per site (higher is better).
        return {name: s.e_fixed for name, s in sites.items()}

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
        # Preserve the caller-defined waiting order (sorted in simulator step_match).
        remaining = list(waiting_jobs)
        demand = len(remaining)
        if demand <= 0:
            logger.debug("schedule no demand")
            return []

        if self.green == 1:
            e_score = self.estimate_e(sites)
            # Green mode: rank by greenscore (higher is better).
            scored = sorted(sites.values(), key=lambda s: e_score[s.name], reverse=True)
        else:
            # Non-green mode: random site order.
            scored = list(sites.values())
            random.shuffle(scored)

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

        logger.info("schedule green=%d demand=%d submissions=%s", self.green, len(waiting_jobs), submissions)
        return submissions
