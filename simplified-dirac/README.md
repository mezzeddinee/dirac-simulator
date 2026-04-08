# simple5

Simplified first version of the simulator:
- No pilot objects.
- Jobs are assigned directly to sites.
- Each site has a max number of concurrent running jobs (`max_running_jobs`).
- Site ranking is based directly on `greenscore` (`e_fixed`).
- No tag-based compatibility filtering: any waiting job can run on any site.

## Input Files

- `sites.csv`: site capacity and characteristics. `max_running_jobs` is supported; if missing, loader falls back to `max_pilots`.
- `jobs.csv`: job arrivals and normalized workload (`norm_cpu_seconds`).
- `site_ci.csv`: fallback CI time series by site.

## Step Flow (1 minute per tick)

1. `release_jobs()`
- Move jobs from `pending` to `waiting` when `submit_time <= current_time`.

2. `step_match()`
- Policy computes unmet waiting demand and returns `(site, k)` submissions.
- Simulator starts up to `k` waiting jobs on each site, limited by `available_slots()`.
- For each started job:
  - state becomes `running`
  - site-specific runtime is derived from `norm_cpu_seconds`, `perf_hs06`, and `avg_wallclock_cpu_ratio`
  - total energy is computed
  - CI is fixed at runtime midpoint (live API if enabled, otherwise CSV lookup)
  - carbon is computed once: `carbon_kg = total_energy_kwh * ci / 1000`

3. `step_execute()`
- Decrement `remaining_min` for each running job.
- Completed jobs move to `done` with `finish_time` set.

4. Advance time by one tick.

## Policy

- Unmet demand is computed against currently free site slots.
- Ranking currently uses `greenscore = e_fixed` directly (descending, higher is better).

## Run

```bash
cd /home/mezzeddi/PycharmProjects/testsim/simple5
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 main.py
```

## Tests

```bash
cd /home/mezzeddi/PycharmProjects/testsim/simple5
python3 -m unittest discover -s tests -p 'test_*.py' -v
```
