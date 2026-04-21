#!/usr/bin/env python3

import requests
import csv
import time

# ==========================================================
# CONFIG
# ==========================================================

PAGE_SIZE = 1000

START = "2026-02-01T12:00:00Z"
END   = "2024-02-01T18:00:00Z"

ES_URL = "https://elias-beta.cc.in2p3.fr:9200"
RAW_INDEX = "egi-fg-dirac-_elasticjobparameters_index_*"

OUTPUT_FILE = "trace.csv"

CERT_DIR = "/home/mezzeddi/PycharmProjects/testsim/trace"
CERT = (f"{CERT_DIR}/egirobot.crt", f"{CERT_DIR}/egirobot.key")
CA_CERT = f"{CERT_DIR}/ca.crt"

# ==========================================================
# QUERY BUILDER
# ==========================================================

def build_query(search_after=None):
    q = {
        "size": PAGE_SIZE,
        "_source": [
            "JobID",
            "SubmissionTime",
            "WallClockTime(s)",
            "NormCPUTime(s)",
            "NCores"
        ],
        "query": {
            "bool": {
                "filter": [
                    {"term": {"Status": "Done"}},
                    {
                        "range": {
                            "SubmissionTime": {
                                "gte": START,
                                "lt": END
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {"SubmissionTime": "asc"},
            {"JobID": "asc"}   # tie-breaker (important!)
        ]
    }

    if search_after:
        q["search_after"] = search_after

    return q


# ==========================================================
# HELPERS
# ==========================================================

def safe_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def fetch_page(query):
    url = f"{ES_URL}/{RAW_INDEX}/_search"

    r = requests.get(
        url,
        json=query,
        cert=CERT,
        verify=CA_CERT,
        timeout=60
    )

    r.raise_for_status()
    return r.json()


# ==========================================================
# MAIN EXTRACTION
# ==========================================================

def main():
    print("[INFO] Starting extraction...")

    total_jobs = 0
    search_after = None

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)

        # header
        writer.writerow([
            "job_id",
            "submit_time",
            "runtime_min",
            "norm_cpu_seconds",
            "cores_used"
        ])

        while True:
            query = build_query(search_after)

            data = fetch_page(query)
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                print("[INFO] No more data.")
                break

            for hit in hits:
                src = hit.get("_source", {})

                job_id = src.get("JobID")
                submit_time = src.get("SubmissionTime")

                wallclock = safe_float(src.get("WallClockTime(s)"))
                runtime_min = wallclock / 60.0

                norm_cpu = safe_float(src.get("NormCPUTime(s)"))
                cores = src.get("NCores", 1)

                if job_id is None or submit_time is None:
                    continue

                writer.writerow([
                    job_id,
                    submit_time,
                    round(runtime_min, 3),
                    round(norm_cpu, 3),
                    cores
                ])

                total_jobs += 1

            # prepare next page
            search_after = hits[-1]["sort"]

            print(f"[INFO] Processed {total_jobs} jobs...")

    print(f"[SUCCESS] Finished. Total jobs: {total_jobs}")
    print(f"[OUTPUT] {OUTPUT_FILE}")


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    main()