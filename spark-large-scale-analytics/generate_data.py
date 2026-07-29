#!/usr/bin/env python3

import argparse
import csv
import random
import time
from datetime import date, timedelta
from pathlib import Path


CATEGORIES = (
    "Electronics",
    "Clothing",
    "Books",
    "Home",
    "Sports",
)

COUNTRIES = (
    "USA",
    "UK",
    "Canada",
    "Germany",
    "France",
    "Japan",
)


def generate_transactions(
    num_records: int,
    output_file: Path,
    seed: int = 42,
    batch_size: int = 100_000,
) -> None:
    """Generate reproducible synthetic e-commerce transactions."""

    if num_records <= 0:
        raise ValueError("num_records must be greater than zero")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    random.seed(seed)

    today = date.today()
    start_time = time.perf_counter()

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8",
        buffering=1024 * 1024,
    ) as csv_file:
        writer = csv.writer(csv_file)

        writer.writerow(
            [
                "transaction_id",
                "date",
                "customer_id",
                "product_category",
                "amount",
                "country",
            ]
        )

        rows = []

        for transaction_id in range(1, num_records + 1):
            transaction_date = today - timedelta(
                days=random.randint(0, 364)
            )

            rows.append(
                (
                    transaction_id,
                    transaction_date.isoformat(),
                    random.randint(1, 500_000),
                    random.choice(CATEGORIES),
                    f"{random.uniform(10.0, 1000.0):.2f}",
                    random.choice(COUNTRIES),
                )
            )

            if len(rows) >= batch_size:
                writer.writerows(rows)
                rows.clear()

            if transaction_id % 1_000_000 == 0:
                elapsed = time.perf_counter() - start_time
                print(
                    f"Generated {transaction_id:,} records "
                    f"in {elapsed:.1f} seconds",
                    flush=True,
                )

        if rows:
            writer.writerows(rows)

    elapsed = time.perf_counter() - start_time
    size_mb = output_file.stat().st_size / (1024 * 1024)

    print("\n===== GENERATION COMPLETE =====")
    print(f"Records: {num_records:,}")
    print(f"Output: {output_file.resolve()}")
    print(f"File size: {size_mb:.2f} MiB")
    print(f"Elapsed time: {elapsed:.2f} seconds")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce transaction data."
    )

    parser.add_argument(
        "--records",
        type=int,
        default=5_000_000,
        help="Number of records to generate.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/transactions.csv"),
        help="CSV output path.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    generate_transactions(
        num_records=args.records,
        output_file=args.output,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
