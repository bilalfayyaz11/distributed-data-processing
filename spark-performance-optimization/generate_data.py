#!/usr/bin/env python3

import argparse
import json
import random
import time
from pathlib import Path


CATEGORIES = (
    "Electronics",
    "Clothing",
    "Food",
    "Books",
    "Toys",
)

REGIONS = (
    "North",
    "South",
    "East",
    "West",
    "Central",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate reproducible newline-delimited sales JSON."
    )

    parser.add_argument(
        "--records",
        type=int,
        default=1_000_000,
        help="Number of records to generate.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sales_data.json"),
        help="Output JSON-lines file.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Records buffered before writing.",
    )

    return parser.parse_args()


def generate_sales_data(
    record_count: int,
    output_path: Path,
    seed: int,
    batch_size: int,
) -> None:
    if record_count <= 0:
        raise ValueError("Record count must be greater than zero.")

    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero.")

    random_generator = random.Random(seed)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = time.perf_counter()

    with output_path.open("w", encoding="utf-8") as output_file:
        buffer: list[str] = []

        for transaction_id in range(1, record_count + 1):
            record = {
                "transaction_id": transaction_id,
                "category": random_generator.choice(CATEGORIES),
                "region": random_generator.choice(REGIONS),
                "amount": round(random_generator.uniform(10.0, 1000.0), 2),
                "quantity": random_generator.randint(1, 50),
            }

            buffer.append(
                json.dumps(
                    record,
                    separators=(",", ":"),
                )
            )

            if len(buffer) >= batch_size:
                output_file.write("\n".join(buffer))
                output_file.write("\n")
                buffer.clear()

            if transaction_id % 100_000 == 0:
                print(
                    f"Generated {transaction_id:,} "
                    f"of {record_count:,} records"
                )

        if buffer:
            output_file.write("\n".join(buffer))
            output_file.write("\n")

    elapsed = time.perf_counter() - started_at
    file_size_mb = output_path.stat().st_size / (1024 * 1024)

    print()
    print("Data generation complete.")
    print(f"Records:      {record_count:,}")
    print(f"Output:       {output_path.resolve()}")
    print(f"File size:    {file_size_mb:.2f} MiB")
    print(f"Elapsed time: {elapsed:.2f} seconds")


def main() -> None:
    arguments = parse_arguments()

    generate_sales_data(
        record_count=arguments.records,
        output_path=arguments.output,
        seed=arguments.seed,
        batch_size=arguments.batch_size,
    )


if __name__ == "__main__":
    main()
