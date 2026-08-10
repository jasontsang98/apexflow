import os

from ingestors.season import discover_race_sessions


def run_bootstrap():
    bucket_name = os.getenv("APEXFLOW_BUCKET", "apexflow-raw-data")
    return discover_race_sessions(2025, bucket_name)


if __name__ == "__main__":
    run_bootstrap()
