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
    )


def make_job(job_id: str) -> Job:
    return Job(
        job_id=job_id,
        submit_time=datetime(2026, 1, 1, 0, 0, 0),
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

    def test_zero_green_fraction_uses_random_site_order(self):
        policy = ReplayCarbonPolicy(green_fraction=0.0, random_seed=0)
        sites = {
            "LOW": make_site("LOW", max_running_jobs=1, e_fixed=0.1),
            "HIGH": make_site("HIGH", max_running_jobs=1, e_fixed=0.9),
        }
        jobs = [make_job("J1")]

        submissions = policy.schedule(jobs, sites)
        self.assertEqual(1, len(submissions))
        self.assertEqual(1, submissions[0][1])

    def test_fractional_green_splits_between_green_and_random_capacity(self):
        policy = ReplayCarbonPolicy(green_fraction=0.5, random_seed=1)
        sites = {
            "LOW": make_site("LOW", max_running_jobs=2, e_fixed=0.1),
            "HIGH": make_site("HIGH", max_running_jobs=2, e_fixed=0.9),
        }
        jobs = [make_job(f"J{i}") for i in range(1, 5)]

        submissions = dict(policy.schedule(jobs, sites))
        self.assertEqual(4, sum(submissions.values()))
        self.assertEqual(2, submissions["HIGH"])


if __name__ == "__main__":
    unittest.main()
