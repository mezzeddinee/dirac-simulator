# dirac-simulator

Standalone project containing only the `simulator` package and CSV trace inputs.

## Quick start

```bash
cd /home/mezzeddi/PycharmProjects/testsim/dirac-simulator
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m simulator.main
```

## Optional live CI endpoint mode

```bash
export SIMPLE4_USE_LIVE_CI=1
export SIMPLE4_CI_TOKEN="your_token"
export SIMPLE4_CI_API_BASE="https://.../gd-kpi-api/v1"
python3 -m simulator.main
```
