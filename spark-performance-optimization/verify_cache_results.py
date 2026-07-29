#!/usr/bin/env python3

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

RESULTS_FILE = (
    BASE_DIR
    / "benchmark-results"
    / "cache_results.csv"
)

EXPECTED_STRATEGIES = {
    "no_cache",
    "cache",
    "memory_and_disk",
}


def main() -> None:
    print("===== CACHE BENCHMARK VERIFICATION =====")

    if not RESULTS_FILE.is_file():
        raise FileNotFoundError(
            f"Result file not found: {RESULTS_FILE}"
        )

    with RESULTS_FILE.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as input_file:
        rows = list(
            csv.DictReader(input_file)
        )

    if len(rows) != 3:
        raise RuntimeError(
            f"Expected 3 strategy rows, found {len(rows)}."
        )

    actual_strategies = {
        row["strategy"]
        for row in rows
    }

    if actual_strategies != EXPECTED_STRATEGIES:
        raise RuntimeError(
            "Caching strategy results are incomplete."
        )

    for row in rows:
        strategy = row["strategy"]

        trial_values = [
            float(row["trial_1_seconds"]),
            float(row["trial_2_seconds"]),
            float(row["trial_3_seconds"]),
        ]

        median_time = float(
            row["median_seconds"]
        )

        result_rows = int(
            row["result_rows"]
        )

        if any(value <= 0 for value in trial_values):
            raise RuntimeError(
                f"{strategy} contains an invalid timing."
            )

        if median_time <= 0:
            raise RuntimeError(
                f"{strategy} contains an invalid median."
            )

        if result_rows != 30:
            raise RuntimeError(
                f"{strategy} expected 30 aggregate rows "
                f"across three iterations, found "
                f"{result_rows}."
            )

    ranked_rows = sorted(
        rows,
        key=lambda row: float(
            row["median_seconds"]
        ),
    )

    print("Result file: PASSED")
    print(f"Strategies:  {len(rows)}")
    print()

    print("===== PERFORMANCE RANKING =====")

    for rank, row in enumerate(
        ranked_rows,
        start=1,
    ):
        print(
            f"{rank}. {row['strategy']}: "
            f"{float(row['median_seconds']):.3f} seconds "
            f"median"
        )

    print()
    print(
        "Fastest observed strategy: "
        f"{ranked_rows[0]['strategy']}"
    )

    print(
        "Cache benchmark verification: PASSED"
    )


if __name__ == "__main__":
    main()
