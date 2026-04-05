import unittest
from datetime import datetime
from pathlib import Path
import sys

# Allow running tests from any cwd (IDE or CLI).
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import Job, Pilot, Site
from policy import ReplayCarbonPolicy


def make_site(name: str, max_pilots: int = 2, e_fixed: float = 0.5) -> Site:
    return Site(
        name=name,
        max_pilots=max_pilots,
        startup_delay_min=1,
        jobs_per_pilot=1,
        e_fixed=e_fixed,
        latitude=0.0,
        longitude=0.0,
        avg_tdp_w=150.0,
        avg_total_cores=12,
        tags={f"site:{name}", "sw:root", "cpu:x86_64"},
    )


def make_job(job_id: str, any_tags=None, all_tags=None) -> Job:
    return Job(
        job_id=job_id,
        tq="TQ",
        submit_time=datetime(2026, 1, 1, 0, 0, 0),
        runtime_min=2,
        required_all_tags=set(all_tags or []),
        required_any_tags=set(any_tags or []),
        cpu_seconds=60,
        wallclock_seconds=120,
        cores_used=1,
    )


class PolicyTests(unittest.TestCase):
    def test_compatible_requires_all_and_any(self):
        policy = ReplayCarbonPolicy()
        tags = {"site:SARA", "sw:root", "cpu:x86_64"}

        job_ok = make_job("J1", any_tags={"site:SARA", "site:NIKHEF"}, all_tags={"sw:root"})
        self.assertTrue(policy.compatible(tags, job_ok))

        job_missing_all = make_job("J2", any_tags={"site:SARA"}, all_tags={"sw:gaudi"})
        self.assertFalse(policy.compatible(tags, job_missing_all))

        job_missing_any = make_job("J3", any_tags={"site:NIKHEF"}, all_tags={"sw:root"})
        self.assertFalse(policy.compatible(tags, job_missing_any))

    def test_unmet_jobs_shared_idle_pilot_counted_once(self):
        policy = ReplayCarbonPolicy()
        sara = make_site("SARA")
        sara.pilots.append(
            Pilot("P1", "SARA", tags={"site:SARA", "sw:root"}, startup_left_min=0, jobs_left=1, status="idle")
        )
        sites = {"SARA": sara}
        jobs = [
            make_job("J1", any_tags={"site:SARA"}, all_tags={"sw:root"}),
            make_job("J2", any_tags={"site:SARA"}, all_tags={"sw:root"}),
        ]

        unmet = policy.unmet_jobs(jobs, sites)
        self.assertEqual(1, len(unmet))
        self.assertEqual("J2", unmet[0].job_id)

    def test_schedule_places_only_on_compatible_site(self):
        policy = ReplayCarbonPolicy()
        sites = {
            "SARA": make_site("SARA", max_pilots=1, e_fixed=0.1),
            "NIKHEF": make_site("NIKHEF", max_pilots=1, e_fixed=0.9),
        }
        jobs = [make_job("J1", any_tags={"site:NIKHEF"}, all_tags={"sw:root"})]

        submissions = policy.schedule(jobs, sites)
        self.assertEqual([("NIKHEF", 1)], submissions)


if __name__ == "__main__":
    unittest.main()
