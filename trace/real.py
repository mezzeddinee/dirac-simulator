#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import tempfile
import time
import random
from dataclasses import dataclass
from pathlib import Path

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

# Mean inter-arrival time (seconds) → Poisson process
MEAN_INTERARRIVAL = 10.0  # 1 job every 10 sec

# Mean runtime (seconds) → exponential distribution
MEAN_RUNTIME = 300.0  # 5 minutes

# Total number of jobs to submit
N_JOBS = 100

# Optional cap to avoid extreme exponential tail
MAX_RUNTIME = 3600.0  # 1 hour max


# ------------------------------------------------------------
# DATA MODEL
# ------------------------------------------------------------

@dataclass
class SyntheticJob:
    job_id: str


# ------------------------------------------------------------
# RANDOM SAMPLING
# ------------------------------------------------------------

def exp_sample(mean: float) -> float:
    """Sample from exponential distribution with given mean."""
    return random.expovariate(1.0 / mean)


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
# JDL CREATION
# ------------------------------------------------------------

def write_jdl(path: Path, worker_path: Path, runtime_seconds: float, job_id: str) -> None:
    worker_name = worker_path.name
    worker_abs = str(worker_path.resolve())

    path.write_text(
        "\n".join(
            [
                f'JobName = "poisson-job-{job_id}";',
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


# ------------------------------------------------------------
# DIRAC SUBMISSION
# ------------------------------------------------------------

def submit_job(jdl_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["dirac-wms-job-submit", str(jdl_path)],
        check=False,
        text=True,
        capture_output=True,
    )


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main() -> int:
    print(
        f"Submitting {N_JOBS} jobs with:\n"
        f"- Poisson arrivals (mean inter-arrival = {MEAN_INTERARRIVAL}s)\n"
        f"- Exponential runtime (mean = {MEAN_RUNTIME/60:.1f} min)"
    )

    with tempfile.TemporaryDirectory(prefix="dirac-poisson-") as tmp_dir:
        tmp = Path(tmp_dir)

        # Prepare worker
        worker = tmp / "sqrt_worker.py"
        write_worker_script(worker)

        for i in range(1, N_JOBS + 1):
            job = SyntheticJob(job_id=str(i))

            # --------------------------------------------------
            # 1. Inter-arrival time (Poisson process)
            # --------------------------------------------------
            interarrival = exp_sample(MEAN_INTERARRIVAL)
            time.sleep(interarrival)

            # --------------------------------------------------
            # 2. Runtime (exponential)
            # --------------------------------------------------
            runtime_seconds = exp_sample(MEAN_RUNTIME)
            runtime_seconds = min(runtime_seconds, MAX_RUNTIME)

            # --------------------------------------------------
            # 3. Create JDL
            # --------------------------------------------------
            jdl = tmp / f"job_{job.job_id}.jdl"
            write_jdl(
                path=jdl,
                worker_path=worker,
                runtime_seconds=runtime_seconds,
                job_id=job.job_id,
            )

            print(
                f"[{i}/{N_JOBS}] submit job_id={job.job_id} "
                f"interarrival={interarrival:.2f}s "
                f"runtime={runtime_seconds/60:.2f} min"
            )

            # --------------------------------------------------
            # 4. Submit
            # --------------------------------------------------
            result = submit_job(jdl)

            if result.returncode == 0:
                print("  submitted:", (result.stdout or "").strip())
            else:
                print("  submit failed")
                if result.stdout:
                    print("  stdout:", result.stdout.strip())
                if result.stderr:
                    print("  stderr:", result.stderr.strip())

    return 0


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------

if __name__ == "__main__":
    main()