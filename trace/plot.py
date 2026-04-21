import pandas as pd
import matplotlib.pyplot as plt


def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, usecols=["submit_time", "runtime_min"])
    df["submit_time"] = pd.to_datetime(df["submit_time"])
    df["runtime_min"] = pd.to_numeric(df["runtime_min"], errors="coerce").fillna(0.0)
    return df


def load_waiting_data(filepath: str) -> pd.Series:
    df = pd.read_csv(filepath, usecols=["timestamp", "waiting_jobs"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["waiting_jobs"] = pd.to_numeric(df["waiting_jobs"], errors="coerce").fillna(0)
    return pd.Series(df["waiting_jobs"].values, index=df["timestamp"], name="waiting_jobs")


def load_jobs_per_site_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    site_cols = [c for c in df.columns if c != "timestamp"]
    for col in site_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.set_index("timestamp")


def compute_metrics_per_min(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    df = df.set_index("submit_time")

    arrivals = df.resample("1min").size()
    arrivals = arrivals.asfreq("1min", fill_value=0)

    runtime_sum = df["runtime_min"].resample("1min").sum()
    runtime_sum = runtime_sum.asfreq("1min", fill_value=0.0)
    return arrivals, runtime_sum


def plot_metrics(
    arrivals: pd.Series,
    runtime_sum: pd.Series,
    waiting_jobs: pd.Series,
    jobs_per_site: pd.DataFrame,
    current_jobs_per_site: pd.DataFrame,
) -> None:
    fig, axes = plt.subplots(5, 1, figsize=(14, 18), sharex=True)

    axes[0].step(arrivals.index, arrivals.values, where="post")
    axes[0].set_ylabel("Arrivals / min")
    axes[0].set_title("Job arrivals per minute (1-min buckets)")

    axes[1].step(runtime_sum.index, runtime_sum.values, where="post", color="tab:orange")
    axes[1].set_ylabel("Total runtime_min / min")
    axes[1].set_title("Total runtime of jobs arrived each minute")

    axes[2].step(waiting_jobs.index, waiting_jobs.values, where="post", color="tab:green")
    axes[2].set_ylabel("Waiting jobs")
    axes[2].set_title("Waiting jobs over time")

    axes[3].stackplot(
        jobs_per_site.index,
        [jobs_per_site[c].values for c in jobs_per_site.columns],
        labels=list(jobs_per_site.columns),
        alpha=0.85,
    )
    axes[3].set_ylabel("Jobs/site")
    axes[3].set_title("Jobs per site over time (from scheduler submissions)")
    axes[3].legend(loc="upper left", ncol=3, fontsize=8)

    for col in current_jobs_per_site.columns:
        axes[4].plot(current_jobs_per_site.index, current_jobs_per_site[col], label=col, linewidth=1.2)
    axes[4].axhline(150, color="black", linestyle="--", linewidth=1, label="Site max (150)")
    axes[4].set_ylabel("Current jobs/site")
    axes[4].set_title("Current existing jobs per site over time (logs-reconstructed, non-stacked)")
    axes[4].set_xlabel("Time")
    axes[4].legend(loc="upper left", ncol=3, fontsize=8)

    for ax in axes:
        ax.tick_params(axis="x", rotation=45)

    fig.tight_layout()
    plt.show()


def main():
    filepath = "trace6"  # <-- input trace
    waiting_filepath = "waiting_over_time.csv"
    jobs_per_site_filepath = "jobs_per_site_over_time.csv"
    current_jobs_per_site_filepath = "current_jobs_per_site_over_time.csv"

    df = load_data(filepath)
    arrivals, runtime_sum = compute_metrics_per_min(df)
    waiting_jobs = load_waiting_data(waiting_filepath)
    jobs_per_site = load_jobs_per_site_data(jobs_per_site_filepath)
    current_jobs_per_site = load_jobs_per_site_data(current_jobs_per_site_filepath)
    plot_metrics(arrivals, runtime_sum, waiting_jobs, jobs_per_site, current_jobs_per_site)


if __name__ == "__main__":
    main()
