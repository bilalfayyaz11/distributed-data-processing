#!/usr/bin/env python3

import csv
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


BASE_DIR = Path(__file__).resolve().parent
DATASET = BASE_DIR / "data" / "sales_data.json"

RESULTS = (
    BASE_DIR
    / "benchmark-results"
    / "partition_results.csv"
)

EXPECTED_RECORDS = 1_000_000
EXPECTED_BENCHMARK_ROWS = 5

SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), False),
        StructField("category", StringType(), False),
        StructField("region", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
    ]
)


def verify_physical_dataset() -> None:
    if not DATASET.is_file():
        raise FileNotFoundError(
            f"Dataset does not exist: {DATASET}"
        )

    physical_lines = 0

    with DATASET.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            json.loads(line)
            physical_lines = line_number

    if physical_lines != EXPECTED_RECORDS:
        raise RuntimeError(
            f"Expected {EXPECTED_RECORDS:,} JSON records, "
            f"but found {physical_lines:,}."
        )

    print(
        f"Physical JSON validation: PASSED "
        f"({physical_lines:,} valid records)"
    )


def verify_benchmark_csv() -> None:
    if not RESULTS.is_file():
        raise FileNotFoundError(
            f"Benchmark output does not exist: {RESULTS}"
        )

    with RESULTS.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as input_file:
        rows = list(csv.DictReader(input_file))

    if len(rows) != EXPECTED_BENCHMARK_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_BENCHMARK_ROWS} benchmark rows, "
            f"but found {len(rows)}."
        )

    expected_partitions = {2, 4, 8, 16, 32}

    actual_partitions = {
        int(row["partition_count"])
        for row in rows
    }

    if actual_partitions != expected_partitions:
        raise RuntimeError(
            "Partition benchmark configurations are incomplete."
        )

    print(
        "Benchmark CSV validation: PASSED "
        f"({len(rows)} configurations)"
    )


def verify_with_spark() -> None:
    spark = (
        SparkSession.builder
        .appName("Partition Benchmark Verification")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        dataframe = (
            spark.read
            .schema(SCHEMA)
            .json(str(DATASET))
        )

        record_count = dataframe.count()
        default_partitions = dataframe.rdd.getNumPartitions()

        if record_count != EXPECTED_RECORDS:
            raise RuntimeError(
                f"Spark expected {EXPECTED_RECORDS:,} records, "
                f"but read {record_count:,}."
            )

        print(
            f"Spark record validation: PASSED "
            f"({record_count:,} records)"
        )

        print(f"Default input partitions: {default_partitions}")

        dataframe.show(
            5,
            truncate=False,
        )

    finally:
        spark.stop()


def main() -> None:
    print("===== RESULT VERIFICATION =====")

    verify_physical_dataset()
    verify_benchmark_csv()
    verify_with_spark()

    print()
    print("All Task 1 verification checks passed.")


if __name__ == "__main__":
    main()
