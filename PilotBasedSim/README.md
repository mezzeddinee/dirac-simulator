# dirac-simulator

Standalone project containing only the `simulator` package and CSV trace inputs.

## What It Simulates

- A discrete-time DIRAC-like pilot simulation.
- One simulation step equals one minute.
- Jobs arrive from trace data (`jobs.csv`) and are matched globally to idle compatible pilots.
- New pilots are provisioned by a carbon/congestion policy when there is unmet compatible demand.
- Carbon is computed from energy and CI (CSV-based CI by default, optional live CI API at job midpoint).

## Input Files

- `sites.csv`: site capacity, startup delay, jobs-per-pilot, static tags, average TDP/cores, performance factor (`perf_hs06`), wallclock slowdown ratio (`avg_wallclock_cpu_ratio`), coordinates.
- `jobs.csv`: trace-style arrivals and job requirements (`required_all_tags`, `required_any_tags`) plus normalized CPU workload (`norm_cpu_seconds`).
- `site_ci.csv`: time-indexed CI values (gCO2/kWh), used as fallback/default source.

## Simulation Step (1 Minute)

At each tick, the simulator executes the following function chain in order:

1. `release_jobs()`
- Activates pending jobs whose `submit_time <= current_time`.
- State change: `pending -> waiting`.

2. `step_start()`
- Decrements `startup_left_min` for `starting` pilots.
- When delay reaches zero, pilots become `idle`.

3. `step_match()`
- Collects all idle pilots globally.
- Collects waiting jobs globally, sorted by `(submit_time, job_id)`.
- For each idle pilot, picks the first compatible waiting job:
  - compatibility uses tag constraints:
    - `required_all_tags` must all be present in pilot tags
    - `required_any_tags` must intersect pilot tags (if non-empty)
- On assignment:
  - job becomes `running`
  - `start_time` and assigned `site` are set
  - site-specific runtime is derived from normalized workload:
    - `cpu_seconds_sim = norm_cpu_seconds / perf_hs06_site`
    - `wallclock_seconds_sim = max(cpu_seconds_sim * avg_wallclock_cpu_ratio_site, 60)`
    - `runtime_min_sim = ceil(wallclock_seconds_sim / 60)`
  - job energy basis is computed (per-site average TDP/cores model)
  - job CI is fixed at midpoint time (`submit_time + assigned_runtime/2`) via:
    - live API provider if enabled, else CI CSV lookup

4. `step_execute()`
- Runs one minute of work for every running job.
- Carbon is precomputed at assignment time (one-shot):
  - `job_carbon_kg = total_energy_kwh * assigned_ci_gco2_per_kwh / 1000`
- Decrements `remaining_min`.
- When a job completes:
  - state change: `running -> done`
  - `finish_time` set
  - pilot `jobs_left` decremented
  - pilot returns to `idle` or is retired if `jobs_left == 0`

5. `step_schedule()`
- Builds waiting-job set and computes compatibility-aware unmet demand.
- Current first-trial ranking uses only `E` (carbon signal):
  - `E`: site fixed carbon score (`e_fixed`)
  - sites are sorted by ascending `E_norm` (lower is greener/better)
- `D` (delay/congestion signal) exists in code but is currently disabled/commented for this trial.
- Submits new pilots only on sites that can satisfy currently unmet jobs and have capacity.

6. `current_time += tick`
- Advances simulation clock by one minute.

## Core Modules

- `main.py`: entrypoint
- `app.py`: wiring and run loop
- `simulator.py`: per-tick state machine
- `policy.py`: compatibility-aware carbon/congestion pilot provisioning
- `ci_provider.py`: midpoint CI HTTP client with cache/fallback
- `models.py`: `Job`, `Pilot`, `Site` dataclasses
- `csv_io.py`: CSV loaders
- `metrics.py`: KPI summary (wait/turnaround percentiles, average carbon/job)

## Quick start

```bash
cd /home/mezzeddi/PycharmProjects/testsim/dirac-simulator
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 main.py
```

## Optional live CI endpoint mode

```bash
export SIMULATOR_USE_LIVE_CI=1
export CIM_EMAIL="your_email"
export CIM_PASSWORD="your_password"
# optional override (default: ./cim.conf)
# export SIMULATOR_CI_CONF="/path/to/cim.conf"
# optional: provide token directly instead of email/password
# export SIMULATOR_CI_TOKEN="your_token"
python3 main.py
```

## Current Policy Mode

At the moment, site ranking is intentionally simplified:

- Active: `E` only (carbon-efficiency proxy from `e_fixed`)
- Inactive (kept in code for later): `D` delay/congestion term and combined score
  - `score = -beta * E_norm + gamma * D_norm`

This keeps the first experimental version easy to interpret before re-enabling multi-signal scoring.
