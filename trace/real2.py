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

# Submit one batch every hour, forever.
BATCH_INTERVAL = timedelta(hours=1)

# Number of jobs per hourly batch.
JOBS_PER_BATCH = 1000

# Poisson service-time mean in minutes.
SERVICE_TIME_MEAN_MIN = 5.0

# Guardrail for very long sampled runtimes.
MAX_RUNTIME_MIN = 180


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
    # Avoid 0-second jobs; keep at least one minute.
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


# ------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------

def main() -> int:
    startup = datetime.utcnow()
    next_batch_at = startup
    batch_id = 0
    print(
        f"Starting continuous hourly submission at {startup.isoformat()}Z "
        f"(batch interval={BATCH_INTERVAL}, jobs/batch={JOBS_PER_BATCH}, "
        f"Poisson mean service={SERVICE_TIME_MEAN_MIN} min)"
    )

    with tempfile.TemporaryDirectory(prefix="dirac-hourly-poisson-") as tmp_dir:
        tmp = Path(tmp_dir)
        worker = tmp / "sqrt_worker.py"
        write_worker_script(worker)

        while True:
            now = datetime.utcnow()
            sleep_s = (next_batch_at - now).total_seconds()
            if sleep_s > 0:
                time.sleep(sleep_s)

            batch_start = datetime.utcnow()
            print(
                f"[batch {batch_id}] start={batch_start.isoformat()}Z "
                f"scheduled={next_batch_at.isoformat()}Z"
            )

            ok = 0
            failed = 0
            for i in range(1, JOBS_PER_BATCH + 1):
                runtime_seconds = sample_runtime_seconds()
                job_name = f"poisson-hourly-b{batch_id}-j{i}"
                jdl = tmp / f"batch_{batch_id}_job_{i}.jdl"
                write_jdl(
                    path=jdl,
                    worker_path=worker,
                    runtime_seconds=runtime_seconds,
                    job_name=job_name,
                )

                result = submit_job(jdl)
                if result.returncode == 0:
                    ok += 1
                else:
                    failed += 1
                    print(
                        f"  [batch {batch_id} job {i}] submit failed "
                        f"stdout={(result.stdout or '').strip()} "
                        f"stderr={(result.stderr or '').strip()}"
                    )

            batch_end = datetime.utcnow()
            print(
                f"[batch {batch_id}] done={batch_end.isoformat()}Z "
                f"ok={ok} failed={failed}"
            )

            batch_id += 1
            next_batch_at = startup + (BATCH_INTERVAL * batch_id)


if __name__ == "__main__":
    main()
