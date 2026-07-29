#!/usr/bin/env python3

import argparse
import csv
import random
import statistics
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
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
    / "partition_results.csv"
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
        description="Benchmark Spark aggregation performance "
        "across partition counts."
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
        help="Output CSV for benchmark measurements.",
    )

    parser.add_argument(
        "--partitions",
        type=int,
        nargs="+",
        default=[2, 4, 8, 16, 32],
        help="Partition counts to benchmark.",
    )

    parser.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Measured trials for each partition count.",
    )

    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Partition Performance Benchmark")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.eventLog.enabled", "true")
        .config(
            "spark.eventLog.dir",
            f"file://{BASE_DIR / 'spark-events'}",
        )
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
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


def run_aggregation(
    dataframe: DataFrame,
    partition_count: int,
) -> tuple[float, int]:
    started_at = time.perf_counter()

    result = (
        dataframe
        .repartition(partition_count)
        .groupBy(
            "category",
            "region",
        )
        .agg(
            spark_round(
                spark_sum("amount"),
                2,
            ).alias("total_sales"),
            spark_sum("quantity").alias("total_quantity"),
            count("*").alias("transaction_count"),
        )
        .orderBy(
            col("category"),
            col("region"),
        )
    )

    result_rows = result.collect()

    elapsed = time.perf_counter() - started_at

    return elapsed, len(result_rows)


def write_results(
    output_path: Path,
    measurements: list[dict[str, object]],
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "partition_count",
                "trial_1_seconds",
                "trial_2_seconds",
                "trial_3_seconds",
                "median_seconds",
                "minimum_seconds",
                "maximum_seconds",
                "result_rows",
            ],
        )

        writer.writeheader()
        writer.writerows(measurements)


def main() -> None:
    arguments = parse_arguments()

    if not arguments.input.is_file():
        raise FileNotFoundError(
            f"Input dataset not found: {arguments.input}"
        )

    if arguments.trials != 3:
        raise ValueError(
            "This implementation records exactly three trials "
            "per partition count."
        )

    if any(value <= 0 for value in arguments.partitions):
        raise ValueError(
            "Every partition count must be greater than zero."
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print()
        print("===== DATASET LOADING =====")

        dataframe = load_dataset(
            spark,
            arguments.input,
        )

        total_records = dataframe.count()
        input_partitions = dataframe.rdd.getNumPartitions()

        print(f"Records:            {total_records:,}")
        print(f"Input partitions:   {input_partitions}")
        print(f"Logical CPU cores:  4")
        print(f"Spark UI:           http://localhost:4040")

        if total_records != 1_000_000:
            raise RuntimeError(
                "Expected exactly 1,000,000 records, "
                f"but found {total_records:,}."
            )

        print()
        print("===== JVM WARM-UP =====")

        warmup_elapsed, warmup_rows = run_aggregation(
            dataframe,
            partition_count=4,
        )

        print(
            f"Warm-up completed in {warmup_elapsed:.3f} seconds "
            f"with {warmup_rows} result rows."
        )

        benchmark_order = list(arguments.partitions)

        random.Random(42).shuffle(benchmark_order)

        trial_results: dict[int, list[float]] = {
            partition_count: []
            for partition_count in arguments.partitions
        }

        result_counts: dict[int, int] = {}

        print()
        print("===== PARTITION BENCHMARK =====")
        print(f"Randomised order: {benchmark_order}")
        print()

        for trial_number in range(1, arguments.trials + 1):
            print(f"--- Trial {trial_number} ---")

            for partition_count in benchmark_order:
                elapsed, result_rows = run_aggregation(
                    dataframe,
                    partition_count,
                )

                trial_results[partition_count].append(elapsed)
                result_counts[partition_count] = result_rows

                print(
                    f"Partitions: {partition_count:>2} | "
                    f"Elapsed: {elapsed:>8.3f} seconds | "
                    f"Rows: {result_rows}"
                )

            benchmark_order.reverse()
            print()

        measurements: list[dict[str, object]] = []

        for partition_count in sorted(trial_results):
            timings = trial_results[partition_count]

            measurements.append(
                {
                    "partition_count": partition_count,
                    "trial_1_seconds": f"{timings[0]:.6f}",
                    "trial_2_seconds": f"{timings[1]:.6f}",
                    "trial_3_seconds": f"{timings[2]:.6f}",
                    "median_seconds": (
                        f"{statistics.median(timings):.6f}"
                    ),
                    "minimum_seconds": f"{min(timings):.6f}",
                    "maximum_seconds": f"{max(timings):.6f}",
                    "result_rows": result_counts[partition_count],
                }
            )

        write_results(
            arguments.output,
            measurements,
        )

        best_partition_count = min(
            trial_results,
            key=lambda partition_count: statistics.median(
                trial_results[partition_count]
            ),
        )

        best_median = statistics.median(
            trial_results[best_partition_count]
        )

        print("===== MEDIAN RESULTS =====")
        print(
            "Partition Count | Median Time | "
            "Minimum Time | Maximum Time"
        )
        print("-" * 63)

        for partition_count in sorted(trial_results):
            timings = trial_results[partition_count]

            print(
                f"{partition_count:>15} | "
                f"{statistics.median(timings):>10.3f}s | "
                f"{min(timings):>11.3f}s | "
                f"{max(timings):>11.3f}s"
            )

        print()
        print("===== BEST OBSERVED CONFIGURATION =====")
        print(f"Partition count: {best_partition_count}")
        print(f"Median time:     {best_median:.3f} seconds")
        print(f"Results saved:   {arguments.output.resolve()}")

    finally:
        spark.stop()
        print("Spark session stopped cleanly.")


if __name__ == "__main__":
    main()
