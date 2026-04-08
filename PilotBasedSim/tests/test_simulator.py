import unittest
from datetime import datetime
from pathlib import Path
import sys

# Allow running tests from any cwd (IDE or CLI).
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import Job, Pilot, Site
from simulator import ReplaySimulator


def make_site(name: str, startup: int = 0, jobs_per_pilot: int = 1) -> Site:
    return Site(
        name=name,
        max_pilots=2,
        startup_delay_min=startup,
        jobs_per_pilot=jobs_per_pilot,
        e_fixed=0.5,
        latitude=52.0,
        longitude=4.0,
        avg_tdp_w=180.0,
        avg_total_cores=24,
        perf_hs06=1.0,
        avg_wallclock_cpu_ratio=1.0,
        tags={f"site:{name}", "sw:root", "cpu:x86_64"},
    )


def make_job(job_id: str, submit: datetime) -> Job:
    return Job(
        job_id=job_id,
        tq="TQ",
        submit_time=submit,
        runtime_min=2,
        required_all_tags={"sw:root"},
        required_any_tags={"site:SARA"},
        norm_cpu_seconds=90.0,
        cores_used=1,
    )


class DummyCIProvider:
    def __init__(self):
        self.calls = []

    def get_ci(self, site_name, midpoint_ts, latitude, longitude):
        self.calls.append((site_name, midpoint_ts, latitude, longitude))
        return 200.0


class SimulatorTests(unittest.TestCase):
    def test_compute_energy_kwh_matches_formula(self):
        site = make_site("SARA")
        job = make_job("J1", datetime(2026, 1, 1, 0, 0, 0))
        sim = ReplaySimulator(sites={"SARA": site}, jobs=[job], ci_series={}, tick_minutes=1)

        energy = sim.compute_energy_kwh(job, site)
        # derived wallclock=max(cpu,60)=90
        # E=((1-f)*90 + f*90) * (1/24) * 180 / 3_600_000 = 0.0001875
        self.assertAlmostEqual(0.0001875, energy, places=9)

    def test_ci_for_job_uses_midpoint_with_provider(self):
        site = make_site("SARA")
        job = make_job("J1", datetime(2026, 1, 1, 12, 0, 0))
        provider = DummyCIProvider()
        sim = ReplaySimulator(
            sites={"SARA": site},
            jobs=[job],
            ci_series={},
            tick_minutes=1,
            ci_provider=provider,
        )

        ci = sim.ci_for_job(site, job)
        self.assertEqual(200.0, ci)
        self.assertEqual(1, len(provider.calls))
        site_name, midpoint, lat, lon = provider.calls[0]
        self.assertEqual("SARA", site_name)
        self.assertEqual(datetime(2026, 1, 1, 12, 1, 0), midpoint)  # submit + runtime/2
        self.assertEqual(52.0, lat)
        self.assertEqual(4.0, lon)

    def test_step_match_and_execute_completes_job_and_retires_pilot(self):
        site = make_site("SARA", startup=0, jobs_per_pilot=1)
        job = make_job("J1", datetime(2026, 1, 1, 0, 0, 0))
        job.activate()
        pilot = Pilot(
            pilot_id="P1",
            site="SARA",
            tags={"site:SARA", "sw:root"},
            startup_left_min=0,
            jobs_left=1,
            status="idle",
        )
        site.pilots.append(pilot)

        sim = ReplaySimulator(
            sites={"SARA": site},
            jobs=[job],
            ci_series={"SARA": [(datetime(2026, 1, 1, 0, 0, 0), 210.0)]},
            tick_minutes=1,
        )
        sim.current_time = datetime(2026, 1, 1, 0, 0, 0)

        sim.step_match()
        self.assertEqual("running", job.status)
        self.assertEqual("SARA", job.site)

        sim.step_execute()
        self.assertEqual("running", job.status)
        self.assertGreater(job.carbon_kg, 0.0)

        sim.step_execute()
        self.assertEqual("done", job.status)
        self.assertEqual(0, len(site.pilots))  # jobs_per_pilot=1 => retire


if __name__ == "__main__":
    unittest.main()
