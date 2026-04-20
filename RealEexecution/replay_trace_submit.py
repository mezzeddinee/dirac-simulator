#!/usr/bin/env python3
from __future__ import annotations

import csv
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

TRACE_FILE = Path(__file__).with_name("trace.csv")


@dataclass
class TraceJob:
    job_id: str
    submit_time: datetime
    runtime_min: float


def parse_submit_time(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported submit_time format: {value}")


def load_jobs(path: Path) -> list[TraceJob]:
    jobs: list[TraceJob] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            runtime_min = float(row.get("runtime_min", "0") or 0.0)
            if runtime_min <= 0.0:
                continue
            jobs.append(
                TraceJob(
                    job_id=str(row["job_id"]).strip(),
                    submit_time=parse_submit_time(str(row["submit_time"])),
                    runtime_min=runtime_min,
                )
            )
    jobs.sort(key=lambda j: (j.submit_time, j.job_id))
    return jobs


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


def write_jdl(path: Path, worker_path: Path, runtime_seconds: float, trace_job_id: str) -> None:
    worker_name = worker_path.name
    worker_abs = str(worker_path.resolve())
    path.write_text(
        "\n".join(
            [
                f"JobName = \"trace-replay-{trace_job_id}\";",
                "Type = \"Job\";",
                f"Executable = \"{worker_name}\";",
                f"Arguments = \"{runtime_seconds:.3f}\";",
                "StdOutput = \"std.out\";",
                "StdError = \"std.err\";",
                f"InputSandbox = {{\"{worker_abs}\"}};",
                "OutputSandbox = {\"std.out\", \"std.err\"};",
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


def main() -> int:
    jobs = load_jobs(TRACE_FILE)
    if not jobs:
        print(f"No jobs found in {TRACE_FILE}")
        return 0

    first_submit_time = jobs[0].submit_time
    replay_start = time.time()
    print(f"Submitting {len(jobs)} jobs from {TRACE_FILE} using relative submit-time replay")
    print(f"Replay t=0 is first submit_time: {first_submit_time}")

    with tempfile.TemporaryDirectory(prefix="dirac-trace-replay-") as tmp_dir:
        tmp = Path(tmp_dir)
        worker = tmp / "sqrt_worker.py"
        write_worker_script(worker)

        for i, job in enumerate(jobs, start=1):
            delta_submit_s = (job.submit_time - first_submit_time).total_seconds()
            due_time = replay_start + delta_submit_s
            sleep_for = due_time - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

            runtime_seconds = job.runtime_min * 60.0
            jdl = tmp / f"job_{job.job_id}.jdl"
            write_jdl(
                path=jdl,
                worker_path=worker,
                runtime_seconds=runtime_seconds,
                trace_job_id=job.job_id,
            )

            print(
                f"[{i}/{len(jobs)}] submit trace_job_id={job.job_id} "
                f"submit_time={job.submit_time} delta_submit_s={delta_submit_s:.1f} "
                f"runtime_min={job.runtime_min:.3f}"
            )
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


if __name__ == "__main__":
    main()
