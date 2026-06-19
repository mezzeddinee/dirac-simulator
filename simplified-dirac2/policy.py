from __future__ import annotations

import logging
import random
from typing import Dict, List, Optional, Tuple

try:
    from .models import Job, Site
except ImportError:  # direct script-style execution fallback
    from models import Job, Site

logger = logging.getLogger(__name__)


class ReplayCarbonPolicy:
    def __init__(
        self,
        green: Optional[int] = None,
        green_fraction: Optional[float] = None,
        random_seed: Optional[int] = None,
    ):
        if green_fraction is None:
            green_fraction = 1.0 if green is None or int(green) == 1 else 0.0
        if green_fraction > 1.0:
            green_fraction = green_fraction / 100.0
        self.green_fraction = max(0.0, min(1.0, float(green_fraction)))
        self.green = 1 if self.green_fraction >= 1.0 else 0
        self.rng = random.Random(random_seed)

    def estimate_green(self, sites: Dict[str, Site]) -> Dict[str, float]:
        # Green score per site (higher is better).
        return {name: s.green for name, s in sites.items()}

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

        slots = {name: site.available_slots() for name, site in sites.items()}
        total_slots = sum(max(0, slot_count) for slot_count in slots.values())
        schedulable = min(demand, total_slots)
        green_target = min(schedulable, int((schedulable * self.green_fraction) + 0.5))

        green_score = self.estimate_green(sites)
        green_order = sorted(sites.values(), key=lambda s: green_score[s.name], reverse=True)
        random_order = list(sites.values())
        self.rng.shuffle(random_order)

        submissions: List[Tuple[str, int]] = []
        submitted_by_site: Dict[str, int] = {}

        def add_submission(site_name: str, count: int) -> None:
            if count <= 0:
                return
            submitted_by_site[site_name] = submitted_by_site.get(site_name, 0) + count

        green_remaining = green_target
        for site in green_order:
            avail = slots[site.name]
            if avail <= 0:
                continue
            x = min(green_remaining, avail)
            if x <= 0:
                continue
            add_submission(site.name, x)
            slots[site.name] -= x
            for j in remaining[:x]:
                remaining.remove(j)
            green_remaining -= x
            if green_remaining == 0:
                break

        demand_remaining = schedulable - sum(submitted_by_site.values())
        for site in random_order:
            avail = slots[site.name]
            if avail <= 0:
                continue
            x = min(demand_remaining, avail)
            if x <= 0:
                continue
            add_submission(site.name, x)
            slots[site.name] -= x
            for j in remaining[:x]:
                remaining.remove(j)
            demand_remaining -= x
            if demand_remaining == 0:
                break

        for site in list(green_order) + list(random_order):
            count = submitted_by_site.pop(site.name, 0)
            if count:
                submissions.append((site.name, count))

        logger.info(
            "schedule green_fraction=%.2f demand=%d submissions=%s",
            self.green_fraction,
            len(waiting_jobs),
            submissions,
        )
        return submissions
