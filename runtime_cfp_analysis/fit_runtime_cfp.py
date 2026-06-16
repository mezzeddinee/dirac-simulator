from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Iterable

import numpy as np

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None


BASE = Path(__file__).resolve().parent
CSV_PATH = BASE / "results.csv"
PLOT_PATH = BASE / "runtime_cfp_fits.png"
ACTUAL_CURVE_PATH = BASE / "runtime_cfp_actual_curves.png"


def load_points(path: Path) -> tuple[list[str], np.ndarray, np.ndarray]:
    scenarios: list[str] = []
    runtime: list[float] = []
    cfp: list[float] = []

    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            scenarios.append(row["scenario"])
            runtime.append(float(row["runtime_min"]))
            cfp.append(float(row["cfp_gco2"]))

    return scenarios, np.array(runtime, dtype=float), np.array(cfp, dtype=float)


def r2_score(y: np.ndarray, y_hat: np.ndarray) -> float:
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    return 1.0 - (ss_res / ss_tot) if ss_tot > 0.0 else 0.0


def rmse(y: np.ndarray, y_hat: np.ndarray) -> float:
    return math.sqrt(float(np.mean((y - y_hat) ** 2)))


def print_predictions(scenarios: Iterable[str], runtime: np.ndarray, cfp: np.ndarray, pred: np.ndarray) -> None:
    print("scenario,runtime_min,actual_cfp_gco2,predicted_cfp_gco2,residual_gco2")
    for scenario, x, y, y_hat in zip(scenarios, runtime, cfp, pred):
        print(f"{scenario},{x:.2f},{y:.2f},{y_hat:.2f},{(y - y_hat):.2f}")


def save_plots(
    scenarios: list[str],
    runtime: np.ndarray,
    cfp: np.ndarray,
    power_a: float,
    power_b: float,
) -> None:
    if plt is None:
        print("matplotlib is not installed; plot export skipped")
        return

    x_min = max(0.0, float(np.min(runtime)) * 0.95)
    x_max = float(np.max(runtime)) * 1.05
    x_curve = np.linspace(x_min, x_max, 300)
    power_curve = power_a * (x_curve ** power_b)

    order = np.argsort(runtime)
    fig, ax = plt.subplots(figsize=(13, 8))
    ax.plot(
        runtime[order],
        cfp[order],
        color="black",
        linestyle="--",
        marker="o",
        markersize=6,
        linewidth=1.8,
        label="Actual points connected by runtime",
        zorder=5,
    )
    ax.plot(x_curve, power_curve, color="tab:green", linewidth=2.4, label="Power model")

    for scenario, x, y in zip(scenarios, runtime, cfp):
        ax.annotate(
            scenario,
            (x, y),
            xytext=(6, 5),
            textcoords="offset points",
            fontsize=8,
            alpha=0.85,
        )

    ax.set_title("Runtime vs Carbon Footprint - Power Model")
    ax.set_xlabel("Runtime / makespan (min)")
    ax.set_ylabel("Total CFP (gCO2)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOT_PATH, dpi=170)
    plt.close(fig)

    fraction_mask = np.array(["% green" in scenario for scenario in scenarios], dtype=bool)
    subset_mask = ~fraction_mask

    fig, ax = plt.subplots(figsize=(13, 8))
    for label, mask, color in (
        ("Green fraction scenarios", fraction_mask, "tab:green"),
        ("Site subset scenarios", subset_mask, "tab:purple"),
    ):
        subset_order = np.argsort(runtime[mask])
        ax.plot(
            runtime[mask][subset_order],
            cfp[mask][subset_order],
            marker="o",
            linewidth=2,
            color=color,
            label=label,
        )

    for scenario, x, y in zip(scenarios, runtime, cfp):
        ax.annotate(scenario, (x, y), xytext=(6, 5), textcoords="offset points", fontsize=8, alpha=0.85)

    ax.set_title("Actual Runtime vs CFP Curves")
    ax.set_xlabel("Runtime / makespan (min)")
    ax.set_ylabel("Total CFP (gCO2)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(ACTUAL_CURVE_PATH, dpi=170)
    plt.close(fig)

    print(f"Saved fit plot: {PLOT_PATH}")
    print(f"Saved actual curve plot: {ACTUAL_CURVE_PATH}")


def main() -> None:
    scenarios, runtime, cfp = load_points(CSV_PATH)

    # Power model: CFP = a * runtime^b.
    # Fit by log transform: log(CFP) = log(a) + b * log(runtime).
    log_runtime = np.log(runtime)
    log_cfp = np.log(cfp)
    power_log_coef = np.polyfit(log_runtime, log_cfp, deg=1)
    power_b = float(power_log_coef[0])
    power_a = float(math.exp(power_log_coef[1]))
    power_pred = power_a * (runtime ** power_b)

    print(f"Loaded points: {len(runtime)} from {CSV_PATH}")
    print("")
    print("Power fit: CFP_gCO2 = a * runtime_min^b")
    print(f"a = {power_a:.8f}")
    print(f"b = {power_b:.8f}")
    print(f"R2 = {r2_score(cfp, power_pred):.6f}")
    print(f"RMSE = {rmse(cfp, power_pred):.2f} gCO2")
    print("")

    print("Predictions from power model:")
    print_predictions(scenarios, runtime, cfp, power_pred)

    save_plots(
        scenarios=scenarios,
        runtime=runtime,
        cfp=cfp,
        power_a=power_a,
        power_b=power_b,
    )


if __name__ == "__main__":
    main()
