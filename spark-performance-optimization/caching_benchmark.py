#!/usr/bin/env python3

import argparse
import csv
import statistics
import time
from pathlib import Path
from typing import Callable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


BASE_DIR = Path(__file__).resolve().parent

DEFAULT_INPUT = BASE_DIR / "data" / "sales_data.json"

DEFAULT_OUTPUT = (
    BASE_DIR
    / "benchmark-results"
    / "cache_results.csv"
)

SALES_SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            LongType(),
            nullable=False,
        ),
        StructField(
            "category",
            StringType(),
            nullable=False,
        ),
        StructField(
            "region",
            StringType(),
            nullable=False,
        ),
        StructField(
            "amount",
            DoubleType(),
            nullable=False,
        ),
        StructField(
            "quantity",
            IntegerType(),
            nullable=False,
        ),
    ]
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare Spark execution with no cache, cache, "
            "and MEMORY_AND_DISK persistence."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Input JSON-lines dataset.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Benchmark result CSV.",
    )

    parser.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Number of measured trials per strategy.",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Repeated analytical iterations per trial.",
    )

    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Spark Caching Strategy Benchmark")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.eventLog.enabled", "true")
        .config(
            "spark.eventLog.dir",
            f"file://{BASE_DIR / 'spark-events'}",
        )
        .getOrCreate()
    )


def load_dataset(
    spark: SparkSession,
    input_path: Path,
) -> DataFrame:
    return (
        spark.read
        .schema(SALES_SCHEMA)
        .json(str(input_path))
    )


def run_analytical_workload(
    dataframe: DataFrame,
    iterations: int,
) -> int:
    total_result_rows = 0

    for iteration_number in range(1, iterations + 1):
        category_metrics = (
            dataframe
            .groupBy("category")
            .agg(
                spark_round(
                    avg("amount"),
                    2,
                ).alias("average_amount"),
                spark_round(
                    spark_sum("amount"),
                    2,
                ).alias("total_amount"),
                count("*").alias("transaction_count"),
            )
            .orderBy(col("category"))
        )

        region_metrics = (
            dataframe
            .groupBy("region")
            .agg(
                spark_sum("quantity").alias("total_quantity"),
                spark_round(
                    avg("amount"),
                    2,
                ).alias("average_amount"),
                count("*").alias("transaction_count"),
            )
            .orderBy(col("region"))
        )

        category_rows = category_metrics.collect()
        region_rows = region_metrics.collect()

        total_result_rows += len(category_rows)
        total_result_rows += len(region_rows)

        print(
            f"Iteration {iteration_number}: "
            f"{len(category_rows)} category rows, "
            f"{len(region_rows)} region rows"
        )

    return total_result_rows


def benchmark_no_cache(
    spark: SparkSession,
    input_path: Path,
    iterations: int,
) -> tuple[float, int, float]:
    started_at = time.perf_counter()

    dataframe = load_dataset(
        spark,
        input_path,
    )

    result_rows = run_analytical_workload(
        dataframe,
        iterations,
    )

    elapsed = time.perf_counter() - started_at

    return elapsed, result_rows, 0.0


def benchmark_cache(
    spark: SparkSession,
    input_path: Path,
    iterations: int,
) -> tuple[float, int, float]:
    dataframe = load_dataset(
        spark,
        input_path,
    ).cache()

    materialisation_started = time.perf_counter()

    record_count = dataframe.count()

    materialisation_time = (
        time.perf_counter()
        - materialisation_started
    )

    print(
        f"Cache materialised with "
        f"{record_count:,} records in "
        f"{materialisation_time:.3f} seconds"
    )

    workload_started = time.perf_counter()

    try:
        result_rows = run_analytical_workload(
            dataframe,
            iterations,
        )
    finally:
        dataframe.unpersist(blocking=True)

    workload_elapsed = (
        time.perf_counter()
        - workload_started
    )

    total_elapsed = (
        materialisation_time
        + workload_elapsed
    )

    return total_elapsed, result_rows, materialisation_time


def benchmark_memory_and_disk(
    spark: SparkSession,
    input_path: Path,
    iterations: int,
) -> tuple[float, int, float]:
    dataframe = load_dataset(
        spark,
        input_path,
    ).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    materialisation_started = time.perf_counter()

    record_count = dataframe.count()

    materialisation_time = (
        time.perf_counter()
        - materialisation_started
    )

    print(
        f"MEMORY_AND_DISK materialised with "
        f"{record_count:,} records in "
        f"{materialisation_time:.3f} seconds"
    )

    workload_started = time.perf_counter()

    try:
        result_rows = run_analytical_workload(
            dataframe,
            iterations,
        )
    finally:
        dataframe.unpersist(blocking=True)

    workload_elapsed = (
        time.perf_counter()
        - workload_started
    )

    total_elapsed = (
        materialisation_time
        + workload_elapsed
    )

    return total_elapsed, result_rows, materialisation_time


def write_results(
    output_path: Path,
    rows: list[dict[str, object]],
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "strategy",
                "trial_1_seconds",
                "trial_2_seconds",
                "trial_3_seconds",
                "median_seconds",
                "minimum_seconds",
                "maximum_seconds",
                "average_materialisation_seconds",
                "result_rows",
                "improvement_vs_no_cache_percent",
            ],
        )

        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    arguments = parse_arguments()

    if not arguments.input.is_file():
        raise FileNotFoundError(
            f"Input dataset not found: {arguments.input}"
        )

    if arguments.trials != 3:
        raise ValueError(
            "This benchmark records exactly three trials."
        )

    if arguments.iterations <= 0:
        raise ValueError(
            "Iterations must be greater than zero."
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    strategies: list[
        tuple[
            str,
            Callable[
                [SparkSession, Path, int],
                tuple[float, int, float],
            ],
        ]
    ] = [
        (
            "no_cache",
            benchmark_no_cache,
        ),
        (
            "cache",
            benchmark_cache,
        ),
        (
            "memory_and_disk",
            benchmark_memory_and_disk,
        ),
    ]

    timings: dict[str, list[float]] = {
        strategy_name: []
        for strategy_name, _ in strategies
    }

    materialisation_times: dict[str, list[float]] = {
        strategy_name: []
        for strategy_name, _ in strategies
    }

    result_counts: dict[str, int] = {}

    try:
        print()
        print("===== DATASET VALIDATION =====")

        validation_dataframe = load_dataset(
            spark,
            arguments.input,
        )

        record_count = validation_dataframe.count()

        print(f"Records:           {record_count:,}")
        print(
            f"Input partitions:  "
            f"{validation_dataframe.rdd.getNumPartitions()}"
        )
        print(f"Repeated iterations per trial: {arguments.iterations}")
        print("Spark UI:          http://localhost:4040")

        if record_count != 1_000_000:
            raise RuntimeError(
                "Expected exactly 1,000,000 records, "
                f"but found {record_count:,}."
            )

        print()
        print("===== JVM WARM-UP =====")

        warmup_dataframe = load_dataset(
            spark,
            arguments.input,
        )

        warmup_dataframe.groupBy(
            "category"
        ).count().collect()

        print("JVM warm-up complete.")

        print()
        print("===== CACHING STRATEGY BENCHMARK =====")

        for trial_number in range(
            1,
            arguments.trials + 1,
        ):
            print()
            print(f"######## TRIAL {trial_number} ########")

            trial_order = (
                strategies
                if trial_number % 2 == 1
                else list(reversed(strategies))
            )

            for strategy_name, strategy_function in trial_order:
                print()
                print(
                    f"--- Strategy: {strategy_name} ---"
                )

                elapsed, result_rows, materialisation_time = (
                    strategy_function(
                        spark,
                        arguments.input,
                        arguments.iterations,
                    )
                )

                timings[strategy_name].append(elapsed)

                materialisation_times[
                    strategy_name
                ].append(materialisation_time)

                result_counts[strategy_name] = result_rows

                spark.catalog.clearCache()

                print(
                    f"Strategy {strategy_name} completed in "
                    f"{elapsed:.3f} seconds"
                )

        no_cache_median = statistics.median(
            timings["no_cache"]
        )

        result_rows: list[dict[str, object]] = []

        for strategy_name, _ in strategies:
            strategy_timings = timings[strategy_name]

            median_time = statistics.median(
                strategy_timings
            )

            if strategy_name == "no_cache":
                improvement = 0.0
            else:
                improvement = (
                    (
                        no_cache_median
                        - median_time
                    )
                    / no_cache_median
                ) * 100

            result_rows.append(
                {
                    "strategy": strategy_name,
                    "trial_1_seconds": (
                        f"{strategy_timings[0]:.6f}"
                    ),
                    "trial_2_seconds": (
                        f"{strategy_timings[1]:.6f}"
                    ),
                    "trial_3_seconds": (
                        f"{strategy_timings[2]:.6f}"
                    ),
                    "median_seconds": (
                        f"{median_time:.6f}"
                    ),
                    "minimum_seconds": (
                        f"{min(strategy_timings):.6f}"
                    ),
                    "maximum_seconds": (
                        f"{max(strategy_timings):.6f}"
                    ),
                    "average_materialisation_seconds": (
                        f"{statistics.mean(materialisation_times[strategy_name]):.6f}"
                    ),
                    "result_rows": result_counts[strategy_name],
                    "improvement_vs_no_cache_percent": (
                        f"{improvement:.2f}"
                    ),
                }
            )

        write_results(
            arguments.output,
            result_rows,
        )

        fastest_strategy = min(
            timings,
            key=lambda strategy_name: statistics.median(
                timings[strategy_name]
            ),
        )

        print()
        print("===== MEDIAN RESULTS =====")
        print(
            "Strategy          | Median Time | "
            "Minimum Time | Maximum Time | Improvement"
        )
        print("-" * 81)

        for strategy_name, _ in strategies:
            strategy_timings = timings[strategy_name]

            median_time = statistics.median(
                strategy_timings
            )

            improvement = (
                0.0
                if strategy_name == "no_cache"
                else (
                    (
                        no_cache_median
                        - median_time
                    )
                    / no_cache_median
                ) * 100
            )

            print(
                f"{strategy_name:<17} | "
                f"{median_time:>10.3f}s | "
                f"{min(strategy_timings):>11.3f}s | "
                f"{max(strategy_timings):>11.3f}s | "
                f"{improvement:>9.2f}%"
            )

        print()
        print("===== FASTEST OBSERVED STRATEGY =====")
        print(f"Strategy: {fastest_strategy}")
        print(
            "Median time: "
            f"{statistics.median(timings[fastest_strategy]):.3f} "
            "seconds"
        )
        print(
            f"Results saved: {arguments.output.resolve()}"
        )

    finally:
        spark.catalog.clearCache()
        spark.stop()
        print("Spark session stopped cleanly.")


if __name__ == "__main__":
    main()
