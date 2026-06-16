#!/usr/bin/env python3
from __future__ import annotations

import math
import random
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

# Hourly cadence anchor.
HOUR_INTERVAL = timedelta(hours=1)

# Submission slot cadence within each hour.
SLOT_INTERVAL = timedelta(minutes=10)
SLOTS_PER_HOUR = 6

# Number of jobs per hour.
JOBS_PER_HOUR = 500

# Poisson service-time mean in minutes.
SERVICE_TIME_MEAN_MIN = 5.0

# Guardrail for very long sampled runtimes.
MAX_RUNTIME_MIN = 180

# Print one line per submitted job (can be noisy at high rate).
VERBOSE_JOB_LOG = True


# ------------------------------------------------------------
# RANDOM SAMPLING
# ------------------------------------------------------------

def poisson_sample(lam: float) -> int:
    """Sample k ~ Poisson(lam) using Knuth's algorithm."""
    if lam <= 0.0:
        raise ValueError("Poisson lambda must be > 0")
    l = math.exp(-lam)
    k = 0
    p = 1.0
    while p > l:
        k += 1
        p *= random.random()
    return k - 1


def sample_runtime_seconds() -> float:
    runtime_min = poisson_sample(SERVICE_TIME_MEAN_MIN)
    runtime_min = max(1, runtime_min)
    runtime_min = min(runtime_min, MAX_RUNTIME_MIN)
    return float(runtime_min) * 60.0


# ------------------------------------------------------------
# WORKER SCRIPT
# ------------------------------------------------------------

def write_worker_script(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import math
import random
import sys
import time

seconds = float(sys.argv[1])
deadline = time.time() + seconds

x = 0.0
while time.time() < deadline:
    for _ in range(100000):
        x += math.sqrt(random.random())

print(f"sqrt-loop done, checksum={x:.6f}")
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


# ------------------------------------------------------------
# JDL CREATION + SUBMIT
# ------------------------------------------------------------

def write_jdl(path: Path, worker_path: Path, runtime_seconds: float, job_name: str) -> None:
    worker_name = worker_path.name
    worker_abs = str(worker_path.resolve())
    path.write_text(
        "\n".join(
            [
                f'JobName = "{job_name}";',
                'Type = "Job";',
                f'Executable = "{worker_name}";',
                f'Arguments = "{runtime_seconds:.3f}";',
                'StdOutput = "std.out";',
                'StdError = "std.err";',
                f'InputSandbox = {{"{worker_abs}"}};',
                'OutputSandbox = {"std.out", "std.err"};',
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def submit_job(jdl_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["dirac-wms-job-submit", str(jdl_path)],
        check=False,
        text=True,
        capture_output=True,
    )


def jobs_for_slot(slot_index: int) -> int:
    base = JOBS_PER_HOUR // SLOTS_PER_HOUR
    remainder = JOBS_PER_HOUR % SLOTS_PER_HOUR
    return base + (1 if slot_index < remainder else 0)


# ------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------

def main() -> int:
    startup = datetime.utcnow()
    hour_id = 0
    print(
        f"Starting continuous submission at {startup.isoformat()}Z "
        f"(jobs/hour={JOBS_PER_HOUR}, slot={SLOT_INTERVAL}, "
        f"Poisson mean service={SERVICE_TIME_MEAN_MIN} min)"
    )

    with tempfile.TemporaryDirectory(prefix="dirac-hourly-uniform10m-") as tmp_dir:
        tmp = Path(tmp_dir)
        worker = tmp / "sqrt_worker.py"
        write_worker_script(worker)

        while True:
            hour_start = startup + (HOUR_INTERVAL * hour_id)
            now = datetime.utcnow()
            sleep_s = (hour_start - now).total_seconds()
            if sleep_s > 0:
                time.sleep(sleep_s)

            print(f"[hour {hour_id}] start={datetime.utcnow().isoformat()}Z scheduled={hour_start.isoformat()}Z")
            hour_ok = 0
            hour_failed = 0

            for slot in range(SLOTS_PER_HOUR):
                slot_start = hour_start + (SLOT_INTERVAL * slot)
                now = datetime.utcnow()
                sleep_s = (slot_start - now).total_seconds()
                if sleep_s > 0:
                    time.sleep(sleep_s)

                n_jobs = jobs_for_slot(slot)
                slot_ok = 0
                slot_failed = 0
                print(f"  [hour {hour_id} slot {slot}] start={datetime.utcnow().isoformat()}Z jobs={n_jobs}")

                for i in range(1, n_jobs + 1):
                    runtime_seconds = sample_runtime_seconds()
                    job_name = f"poisson-uniform10m-h{hour_id}-s{slot}-j{i}"
                    jdl = tmp / f"hour_{hour_id}_slot_{slot}_job_{i}.jdl"
                    if VERBOSE_JOB_LOG:
                        print(
                            f"    [hour {hour_id} slot {slot} job {i}/{n_jobs}] "
                            f"submitting name={job_name} runtime_min={runtime_seconds/60.0:.2f}"
                        )
                    write_jdl(
                        path=jdl,
                        worker_path=worker,
                        runtime_seconds=runtime_seconds,
                        job_name=job_name,
                    )
                    result = submit_job(jdl)
                    if result.returncode == 0:
                        slot_ok += 1
                        if VERBOSE_JOB_LOG:
                            print(
                                f"    [hour {hour_id} slot {slot} job {i}/{n_jobs}] "
                                f"submitted ok"
                            )
                    else:
                        slot_failed += 1
                        print(
                            f"    [hour {hour_id} slot {slot} job {i}] submit failed "
                            f"stdout={(result.stdout or '').strip()} "
                            f"stderr={(result.stderr or '').strip()}"
                        )

                hour_ok += slot_ok
                hour_failed += slot_failed
                print(f"  [hour {hour_id} slot {slot}] done ok={slot_ok} failed={slot_failed}")

            print(f"[hour {hour_id}] done={datetime.utcnow().isoformat()}Z ok={hour_ok} failed={hour_failed}")
            hour_id += 1


if __name__ == "__main__":
    main()
