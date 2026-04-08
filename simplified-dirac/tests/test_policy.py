import unittest
from datetime import datetime
from pathlib import Path
import sys

# Allow running tests from any cwd (IDE or CLI).
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import Job, Site
from policy import ReplayCarbonPolicy


def make_site(name: str, max_running_jobs: int = 2, e_fixed: float = 0.5) -> Site:
    return Site(
        name=name,
        max_running_jobs=max_running_jobs,
        e_fixed=e_fixed,
        latitude=0.0,
        longitude=0.0,
        avg_tdp_w=150.0,
        avg_total_cores=12,
        perf_hs06=1.0,
        avg_wallclock_cpu_ratio=1.0,
    )


def make_job(job_id: str) -> Job:
    return Job(
        job_id=job_id,
        tq="TQ",
        submit_time=datetime(2026, 1, 1, 0, 0, 0),
        runtime_min=2,
        norm_cpu_seconds=60,
        cores_used=1,
    )


class PolicyTests(unittest.TestCase):
    def test_unmet_jobs_shared_site_capacity_counted_once(self):
        policy = ReplayCarbonPolicy()
        sara = make_site("SARA", max_running_jobs=1)
        sites = {"SARA": sara}
        jobs = [
            make_job("J1"),
            make_job("J2"),
        ]

        unmet = policy.unmet_jobs(jobs, sites)
        self.assertEqual(1, len(unmet))
        self.assertEqual("J2", unmet[0].job_id)

    def test_schedule_places_on_highest_greenscore_site(self):
        policy = ReplayCarbonPolicy()
        sites = {
            "SARA": make_site("SARA", max_running_jobs=1, e_fixed=0.1),
            "NIKHEF": make_site("NIKHEF", max_running_jobs=1, e_fixed=0.9),
        }
        jobs = [make_job("J1")]

        submissions = policy.schedule(jobs, sites)
        self.assertEqual([("NIKHEF", 1)], submissions)


if __name__ == "__main__":
    unittest.main()
