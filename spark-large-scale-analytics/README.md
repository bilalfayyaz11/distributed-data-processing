# Large-Scale Transaction Analytics with Apache Spark

## What This Does

This implementation provides an end-to-end Apache Spark data-processing pipeline for generating, loading, cleaning, analysing, optimising, and validating five million synthetic e-commerce transaction records.

The workflow uses Spark DataFrames to process a large CSV dataset across multiple local CPU cores. It calculates country-level revenue metrics, monthly product-category trends, and the top one hundred customers by total spending.

The pipeline writes analytical results in CSV, Parquet, and JSON formats and includes a separate verification application that reads each output back into Spark and confirms that the generated datasets are valid.

Two processing implementations are included. The baseline pipeline demonstrates standard DataFrame transformations and output generation, while the optimised pipeline uses caching, repartitioning, Adaptive Query Execution, and reusable intermediate data to improve repeated analytical workloads.

## Architecture

    +-----------------------------------------------------------+
    | Synthetic Data Generation                                 |
    |                                                           |
    | generate_data.py                                          |
    |                                                           |
    | - Five million transaction records                        |
    | - Reproducible random seed                                |
    | - Batched CSV writing                                     |
    | - Stable memory consumption                               |
    +----------------------------+------------------------------+
                                 |
                                 v
    +-----------------------------------------------------------+
    | Raw Transaction Dataset                                   |
    |                                                           |
    | data/transactions.csv                                     |
    |                                                           |
    | transaction_id                                            |
    | date                                                      |
    | customer_id                                               |
    | product_category                                          |
    | amount                                                    |
    | country                                                   |
    +----------------------------+------------------------------+
                                 |
                                 v
    +-----------------------------------------------------------+
    | Apache Spark DataFrame Processing                         |
    |                                                           |
    | - Explicit schema                                         |
    | - Date conversion                                         |
    | - Null removal                                            |
    | - Invalid-value filtering                                 |
    | - Parallel local execution                                |
    | - Adaptive Query Execution                                |
    +----------------------------+------------------------------+
                                 |
                +----------------+----------------+
                |                                 |
                v                                 v
    +---------------------------+     +---------------------------+
    | Baseline Pipeline         |     | Optimised Pipeline        |
    |                           |     |                           |
    | process_data.py           |     | process_data_optimized.py |
    |                           |     |                           |
    | Standard transformations  |     | Cached clean DataFrame    |
    | Repeated DataFrame actions|     | Country repartitioning    |
    | Multi-format output       |     | Reused intermediate data  |
    +-------------+-------------+     +-------------+-------------+
                  |                                 |
                  +----------------+----------------+
                                   |
                                   v
    +-----------------------------------------------------------+
    | Analytical Outputs                                        |
    |                                                           |
    | Country metrics     -> CSV                                |
    | Category trends     -> Parquet                            |
    | Top customers       -> JSON                               |
    +----------------------------+------------------------------+
                                 |
                                 v
    +-----------------------------------------------------------+
    | Output Verification                                       |
    |                                                           |
    | verify_results.py                                         |
    |                                                           |
    | - Reads every output format                               |
    | - Validates record counts                                 |
    | - Displays representative samples                         |
    | - Confirms Spark-compatible output                         |
    +-----------------------------------------------------------+

## Repository Structure

    spark-large-scale-analytics/
    ├── .gitignore
    ├── README.md
    ├── generate_data.py
    ├── process_data.py
    ├── process_data_optimized.py
    └── verify_results.py

The generated dataset and output directories are intentionally excluded from version control because they can be reproduced from the included source code.

## Prerequisites

- Ubuntu 24.04 or a compatible Linux distribution
- At least 4 CPU cores recommended
- At least 12 GB of available memory recommended
- At least 10 GB of available disk capacity
- sudo access
- OpenJDK 17
- Python 3.10 or newer
- Apache Spark 4.1.3
- Git
- wget
- tar
- procps
- tree

## Setup and Installation

Update the package index:

    sudo apt update

Install the required dependencies:

    sudo apt install -y \
      openjdk-17-jdk-headless \
      python3-pip \
      wget \
      curl \
      tar \
      procps \
      tree

Verify Java and Python:

    java -version

    javac -version

    python3 --version

Download Apache Spark:

    cd ~

    wget https://downloads.apache.org/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz

Extract the archive:

    tar -xzf spark-4.1.3-bin-hadoop3.tgz

Install Spark under `/opt`:

    sudo mv spark-4.1.3-bin-hadoop3 /opt/spark

Configure the shell environment:

    cat >> ~/.bashrc << 'SPARK_ENV'

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    export PYSPARK_PYTHON=python3

    SPARK_ENV

Reload the shell environment:

    source ~/.bashrc

Verify the Spark installation:

    spark-submit --version

## How to Reproduce

Clone the repository:

    git clone https://github.com/bilalfayyaz11/distributed-data-processing.git

Enter the implementation directory:

    cd distributed-data-processing/spark-large-scale-analytics

Validate the Python source files:

    python3 -m py_compile \
      generate_data.py \
      process_data.py \
      process_data_optimized.py \
      verify_results.py

Create the data and output directories:

    mkdir -p data output

Generate five million transaction records:

    time python3 generate_data.py \
      --records 5000000 \
      --output data/transactions.csv

Inspect the generated dataset:

    ls -lh data/transactions.csv

    du -h data/transactions.csv

    head -5 data/transactions.csv

    wc -l data/transactions.csv

The expected physical line count is 5,000,001 because the CSV contains five million transaction records plus one header row.

Run the baseline Spark processing pipeline:

    time spark-submit \
      --driver-memory 6g \
      process_data.py

Run the optimised Spark processing pipeline:

    time spark-submit \
      --driver-memory 6g \
      process_data_optimized.py

Verify the generated analytical outputs:

    spark-submit \
      --driver-memory 4g \
      verify_results.py

Inspect the output structure:

    tree output

Inspect the country metrics:

    head -20 output/country_metrics/part-*.csv

Inspect the top-customer records:

    head -10 output/top_customers/part-*.json

Inspect the Parquet output files:

    ls -lh output/category_trends

## Dataset Schema

The generated transaction dataset contains the following columns:

| Column | Type | Description |
|---|---|---|
| transaction_id | Long | Unique sequential transaction identifier |
| date | Date | Transaction date within the previous 365 days |
| customer_id | Integer | Synthetic customer identifier |
| product_category | String | Product category associated with the transaction |
| amount | Double | Transaction value between 10 and 1,000 |
| country | String | Country associated with the transaction |

## Data-Processing Workflow

The Spark workflow performs the following operations:

1. Loads the transaction CSV with a predefined schema.
2. Converts the date field into a Spark date type.
3. Removes records containing null values.
4. Filters transactions with invalid amounts.
5. Counts the loaded and cleaned records.
6. Aggregates transaction metrics by country.
7. Calculates monthly sales trends by product category.
8. Identifies the top one hundred customers by total spending.
9. Writes each analytical result in a suitable storage format.
10. Reads the generated outputs back into Spark for validation.

## Country Metrics

The country-level analysis calculates:

- Total transaction count
- Total generated revenue
- Average transaction value

Results are ordered by total revenue and written in CSV format for straightforward reporting and inspection.

## Category Trends

The category analysis extracts the year and month from each transaction date and groups records by:

- Product category
- Year
- Month

It then calculates:

- Total monthly sales
- Monthly transaction count

The results are written in Parquet format because Parquet provides columnar storage, schema preservation, compression, and efficient analytical reads.

## Top Customers

The customer analysis groups transactions by customer identifier and calculates:

- Total customer spending
- Purchase count

The records are ordered by total spending, limited to the highest one hundred customers, and exported in JSON format.

## Performance Optimisation

The optimised pipeline introduces several Spark performance techniques.

### Explicit Schema

An explicit schema avoids the additional dataset scan required by automatic CSV schema inference. This improves startup efficiency and ensures predictable field types.

### Caching

The cleaned DataFrame is reused by three separate analytical workloads. Caching allows Spark to retain the processed intermediate dataset instead of repeatedly reading and cleaning the source CSV.

### Cache Materialisation

The optimised pipeline performs an action after caching the DataFrame. This materialises the cache before the analytical workloads begin.

### Repartitioning

The cleaned dataset is repartitioned by country before country-level aggregation. This groups records with related keys into a more appropriate partitioning arrangement for the country analysis.

### Adaptive Query Execution

Adaptive Query Execution allows Spark to modify query plans at runtime based on observed data statistics. It can reduce unnecessary shuffle partitions and improve join and aggregation execution.

### Controlled Parallelism

The Spark session uses available local CPU cores and a configured shuffle-partition count to prevent uncontrolled creation of excessively small partitions.

## Output Formats

### CSV

Country metrics are exported as CSV because they form a small, human-readable reporting dataset that can be opened with spreadsheet and business-intelligence tools.

### Parquet

Category trends are written as Parquet because the data is structured, analytical, and likely to be queried by selected columns.

### JSON

Top-customer records are exported as JSON to demonstrate compatibility with applications, APIs, document-oriented systems, and downstream data services.

## Tools Used

- Apache Spark 4.1.3
- PySpark
- Spark DataFrames
- Spark SQL functions
- Adaptive Query Execution
- OpenJDK 17
- Python 3
- CSV
- Apache Parquet
- JSON
- Ubuntu Linux
- Bash
- Git
- wget
- tree

## Key Skills Demonstrated

- Large-scale synthetic data generation
- Memory-efficient batched file creation
- Spark session configuration
- DataFrame schema definition
- Large CSV ingestion
- Data cleaning and validation
- Distributed DataFrame transformations
- Country-level aggregation
- Time-series category analysis
- Customer-spending analysis
- Multi-format data export
- Spark caching
- Cache materialisation
- Data repartitioning
- Adaptive Query Execution
- Processing-time benchmarking
- Analytical output verification
- Linux resource planning
- Reproducible data-pipeline development
- Production-aware repository management

## Real-World Use Case

This architecture can support e-commerce analytics, financial transaction analysis, customer segmentation, sales reporting, fraud-detection preparation, product-performance monitoring, recommendation-system feature preparation, and machine-learning dataset engineering.

In a production environment, the input data could originate from cloud object storage, distributed file systems, transactional databases, event streams, or data warehouses. The same Spark DataFrame transformations could run across a multi-node cluster through Kubernetes, YARN, or a managed Spark service.

## Lessons Learned

- Large datasets should be generated and written in controlled batches rather than accumulated entirely in Python memory.
- Explicit Spark schemas provide stronger type consistency and avoid the cost of automatic schema inference.
- DataFrames remain lazily evaluated until an action triggers execution.
- Cached data must be materialised before it provides value to subsequent actions.
- Caching is useful only when the same intermediate DataFrame is reused.
- Repartitioning should correspond to downstream access patterns rather than being applied without a clear reason.
- Parquet is generally more suitable than CSV for reusable analytical datasets.
- Successful file creation is not sufficient validation; generated outputs should be read back and inspected.
- Performance optimisation should be measured because caching and repartitioning also introduce overhead.
- Generated datasets should not be committed when they can be reproduced reliably from source code.

## Troubleshooting Log

Issue:
The original instructions used Apache Spark 3.4.1, an archived release.

Resolution:
Used Apache Spark 4.1.3 with OpenJDK 17 and Python 3.12 compatibility.

Issue:
The instructions downloaded a complete Spark distribution and separately installed PySpark through pip.

Resolution:
Used the PySpark runtime supplied with the Apache Spark installation to prevent version conflicts between the Python package and Spark JVM libraries.

Issue:
The original scripts used automatic CSV schema inference.

Resolution:
Defined an explicit Spark schema to avoid an additional dataset scan and guarantee predictable column types.

Issue:
The original application relied on relative paths tied to the current terminal directory.

Resolution:
Resolved input and output paths relative to the Python script location.

Issue:
The original generator contained incomplete placeholder logic.

Resolution:
Implemented reproducible transaction generation with batched CSV writing, progress reporting, argument parsing, output-directory creation, and execution-time reporting.

Issue:
The original pipeline did not validate the expected number of generated records.

Resolution:
Added physical line-count inspection and Spark-level record-count validation.

Issue:
The original optimisation instructions cached the DataFrame without explicitly ensuring that the cache was materialised.

Resolution:
Triggered an action immediately after caching so that subsequent analyses could reuse the cached result.

Issue:
The original workflow did not verify that each generated output could be read successfully.

Resolution:
Added a dedicated Spark verification application for the CSV, Parquet, and JSON outputs.

Issue:
The phrase GB-scale could incorrectly imply that every generated dataset is exactly one gigabyte.

Resolution:
Measured and reported the real generated file size instead of relying on an estimated size.

Issue:
The original repository workflow could accidentally commit the generated five-million-record dataset.

Resolution:
Added repository exclusions for generated datasets, output directories, Python caches, Spark runtime files, and benchmark logs.
