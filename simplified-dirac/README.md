# simplified dirac simulation

Simplified first version of the simulator:
- No pilot objects.
- Jobs are assigned directly to sites.
- Each site has a max number of concurrent running jobs (`max_running_jobs`).
- Site ranking is based directly on `greenscore` (`e_fixed`).
- No tag-based compatibility filtering: any waiting job can run on any site.

## Input Files

- `sites.csv`: site capacity and characteristics. `max_running_jobs` is supported; if missing, loader falls back to `max_pilots`.
- `jobs.csv` / `trace.csv`:
  - base fields: `job_id,submit_time,norm_cpu_seconds,cores_used`
  - runtime-related fields: `wallclock` and `CPUNormFactor`
  - supported header aliases from DIRAC traces:
    - `wallclock` / `wallclocktime` / `WallClockTime` / `WallClockTime(s)`
    - `CPUNormFactor` / `CPUNormalizationFactor` / `cpunormlazationfactor`
    - `norm_cpu_seconds` / `cpu_seconds` / `NormCPUTime(s)`
  - if only `runtime_min` is present, wallclock seconds are derived as `runtime_min * 60`
- `cim.conf`: CI provider configuration (CIM/KPI endpoints and defaults).

## Step Flow (1 minute per tick)

1. `release_jobs()`
- Move jobs from `pending` to `waiting` when `submit_time <= current_time`.

2. `step_match()`
- Policy computes unmet waiting demand and returns `(site, k)` submissions.
- Simulator starts up to `k` waiting jobs on each site, limited by `available_slots()`.
- For each started job:
  - state becomes `running`
  - site-specific runtime is derived from job parameters and site performance:
    - `cpu_seconds_sim = norm_cpu_seconds / perf_hs06`
    - `wallclock_seconds_sim = (wallclock * cpu_norm_factor) / perf_hs06`
    - `runtime_min = ceil(wallclock_seconds_sim / 60)`
  - total energy is computed
  - CI is fixed at runtime midpoint via `MidpointCIProvider`
  - carbon is computed once: `carbon_kg = total_energy_kwh * ci / 1000`

3. `step_execute()`
- Decrement `remaining_min` for each running job.
- Completed jobs move to `done` with `finish_time` set.

4. Advance time by one tick.

## Policy

- Unmet demand is computed against currently free site slots.
- `green` flag controls site ordering:
  - `green=1`: rank by `greenscore = e_fixed` (descending, higher is better).
  - `green=0`: ignore `e_fixed` and shuffle sites randomly before assignment.
- Runtime switch is done with `SIMULATOR_GREEN` (default is `1`).

## Run

```bash
cd /home/mezzeddi/PycharmProjects/testsim/dirac-simulator/simplified-dirac
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
# Green mode (score-based ranking)
SIMULATOR_GREEN=1 python3 main.py
# Non-green mode (randomized site ordering)
SIMULATOR_GREEN=0 python3 main.py
```

