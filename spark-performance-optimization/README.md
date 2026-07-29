# Apache Spark Performance Optimisation

## What This Does

This implementation provides a repeatable Apache Spark performance-analysis workflow for measuring how partition counts, DataFrame caching, and persistence strategies affect analytical execution time.

The system generates one million reproducible sales transactions and uses Spark DataFrame workloads to benchmark multiple partition configurations across a four-core machine.

It then compares three data-reuse strategies:

- No caching
- Spark DataFrame caching
- `MEMORY_AND_DISK` persistence

Each benchmark uses repeated trials, controlled Spark configuration, JVM warm-up, explicit schemas, median-based comparison, machine-readable result output, and automated verification.

The purpose is not to assume that a particular optimisation is always faster. The purpose is to measure how Spark behaves for a specific dataset, workload, and hardware configuration and then select the best observed configuration from evidence.

## Architecture

    +-------------------------------------------------------------+
    | Reproducible Data Generation                                |
    |                                                             |
    | generate_data.py                                            |
    |                                                             |
    | - One million sales records                                 |
    | - Deterministic random seed                                 |
    | - Batched JSON-lines writing                                |
    | - Stable Python memory usage                                |
    +------------------------------+------------------------------+
                                   |
                                   v
    +-------------------------------------------------------------+
    | Generated Sales Dataset                                     |
    |                                                             |
    | data/sales_data.json                                        |
    |                                                             |
    | transaction_id                                              |
    | category                                                    |
    | region                                                      |
    | amount                                                      |
    | quantity                                                    |
    +------------------------------+------------------------------+
                                   |
                     +-------------+-------------+
                     |                           |
                     v                           v
    +--------------------------------+   +--------------------------------+
    | Partition Benchmark            |   | Data-Reuse Benchmark           |
    |                                |   |                                |
    | partition_tuning.py            |   | caching_benchmark.py           |
    |                                |   |                                |
    | - 2 partitions                 |   | - No cache                     |
    | - 4 partitions                 |   | - cache()                      |
    | - 8 partitions                 |   | - MEMORY_AND_DISK              |
    | - 16 partitions                |   | - Cache materialisation        |
    | - 32 partitions                |   | - Repeated aggregations        |
    +----------------+---------------+   +----------------+---------------+
                     |                                    |
                     +------------------+-----------------+
                                        |
                                        v
    +-------------------------------------------------------------+
    | Benchmark Measurement                                       |
    |                                                             |
    | - Three measured trials                                     |
    | - JVM warm-up                                               |
    | - Median execution time                                     |
    | - Minimum and maximum time                                  |
    | - Performance improvement percentage                        |
    | - Machine-readable CSV output                               |
    +------------------------------+------------------------------+
                                   |
                                   v
    +-------------------------------------------------------------+
    | Verification                                                 |
    |                                                             |
    | verify_results.py                                           |
    | verify_cache_results.py                                     |
    |                                                             |
    | - Dataset record validation                                 |
    | - JSON syntax validation                                    |
    | - Spark record validation                                   |
    | - Benchmark configuration validation                        |
    | - Aggregate-result validation                               |
    | - Strategy ranking                                          |
    +-------------------------------------------------------------+

## Repository Structure

    spark-performance-optimization/
    ├── .gitignore
    ├── README.md
    ├── caching_benchmark.py
    ├── generate_data.py
    ├── partition_tuning.py
    ├── verify_cache_results.py
    └── verify_results.py

Generated data, benchmark output, Spark event logs, and runtime files are excluded from version control because they can be reproduced from the source code.

## Prerequisites

- Ubuntu 24.04 or a compatible Linux distribution
- Four or more logical CPU cores recommended
- At least 12 GB of memory recommended
- At least 5 GB of available disk capacity
- OpenJDK 17
- Python 3.10 or newer
- Apache Spark 4.2.0
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

    cd /tmp

    wget https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz

Validate and extract the archive:

    tar -tzf spark-4.2.0-bin-hadoop3.tgz >/dev/null

    sudo tar -xzf spark-4.2.0-bin-hadoop3.tgz -C /opt

    sudo mv /opt/spark-4.2.0-bin-hadoop3 /opt/spark

Configure the shell environment:

    cat >> ~/.bashrc << 'SPARK_ENV'

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export SPARK_HOME=/opt/spark
    export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
    export PYSPARK_PYTHON=python3

    SPARK_ENV

Reload the shell environment:

    source ~/.bashrc

Verify Spark:

    spark-submit --version

    pyspark --version

## How to Reproduce

Clone the repository:

    git clone https://github.com/bilalfayyaz11/distributed-data-processing.git

Enter the implementation directory:

    cd distributed-data-processing/spark-performance-optimization

Create the generated-data directories:

    mkdir -p \
      data \
      benchmark-results \
      spark-events

Validate the Python source:

    python3 -m py_compile \
      generate_data.py \
      partition_tuning.py \
      caching_benchmark.py \
      verify_results.py \
      verify_cache_results.py

Generate one million sales records:

    time python3 generate_data.py \
      --records 1000000 \
      --output data/sales_data.json \
      --seed 42 \
      --batch-size 10000

Inspect the generated dataset:

    ls -lh data/sales_data.json

    wc -l data/sales_data.json

    head -3 data/sales_data.json

Run the partition benchmark:

    time spark-submit \
      --driver-memory 6g \
      partition_tuning.py \
      --input data/sales_data.json \
      --output benchmark-results/partition_results.csv \
      --partitions 2 4 8 16 32 \
      --trials 3

Display the partition results:

    column -s, -t \
      < benchmark-results/partition_results.csv

Verify the dataset and partition results:

    spark-submit \
      --driver-memory 4g \
      verify_results.py

Run the caching and persistence benchmark:

    time spark-submit \
      --driver-memory 6g \
      caching_benchmark.py \
      --input data/sales_data.json \
      --output benchmark-results/cache_results.csv \
      --trials 3 \
      --iterations 3

Display the caching results:

    column -s, -t \
      < benchmark-results/cache_results.csv

Verify the caching results:

    python3 verify_cache_results.py

Inspect Spark event logs:

    find spark-events \
      -maxdepth 1 \
      -type f \
      -printf '%f %k KB\n' \
      | sort

## Dataset Schema

| Column | Spark Type | Description |
|---|---|---|
| transaction_id | Long | Unique transaction identifier |
| category | String | Product category |
| region | String | Sales region |
| amount | Double | Transaction value |
| quantity | Integer | Number of items purchased |

The generator writes newline-delimited JSON and uses a fixed random seed so repeated executions produce equivalent input data.

## Partition Benchmark

The partition benchmark measures the following configurations:

    2
    4
    8
    16
    32

The analytical workload:

1. Loads the source data using an explicit schema.
2. Repartitions the DataFrame.
3. Groups records by category and region.
4. Calculates total sales.
5. Calculates total quantity.
6. Calculates transaction count.
7. Collects the small aggregate result.
8. Records execution time.

Each partition configuration runs three times.

The implementation reports:

- Trial execution times
- Median execution time
- Minimum execution time
- Maximum execution time
- Aggregate result-row count
- Best observed partition count

## Why Partition Count Matters

A Spark partition represents a portion of data that can be processed by one Spark task.

Too few partitions can leave available CPU cores idle and create large tasks.

Too many partitions can create excessive scheduling, serialisation, shuffle, and task-management overhead.

The most effective partition count depends on:

- CPU-core availability
- Dataset size
- Record size
- Transformation complexity
- Shuffle behaviour
- Storage performance
- Executor memory
- Data skew

Rules such as two to four partitions per CPU core are useful starting points, but measured benchmark results are more reliable than fixed assumptions.

## Benchmark Reliability

Several controls improve result quality.

### Explicit Schema

The scripts define the Spark schema rather than using automatic JSON inference. This reduces startup work and provides predictable field types.

### JVM Warm-Up

A preliminary Spark action runs before recorded measurements. This reduces the influence of class loading and JVM initialisation on the first measured result.

### Repeated Trials

Each configuration runs three times. A single timing can be distorted by system load, filesystem cache state, JVM activity, or temporary operating-system behaviour.

### Median Comparison

The median is used to rank configurations because it is less sensitive to one unusually slow or fast run.

### Controlled Adaptive Execution

Adaptive Query Execution is disabled during the controlled partition comparison so Spark does not automatically modify the tested partition arrangement.

### Alternating Run Order

Benchmark order changes between trials to reduce the chance that one strategy always benefits from running first or last.

## Caching Benchmark

The caching benchmark compares three strategies.

### No Cache

The source DataFrame is not persisted.

Every analytical action may require Spark to read and process the source data again.

### DataFrame Cache

The DataFrame is cached and explicitly materialised with a count action before the repeated analytical workload begins.

The cached data is released with:

    unpersist(blocking=True)

### MEMORY_AND_DISK

The DataFrame is persisted using:

    StorageLevel.MEMORY_AND_DISK

Spark retains as much data as possible in memory and writes remaining partitions to disk when memory is insufficient.

## Cache Materialisation

Spark transformations are lazy.

Calling `cache()` or `persist()` marks a DataFrame for storage but does not immediately compute it.

An action such as:

    dataframe.count()

is required to populate the cache.

Without materialisation, the first timed analytical operation would include the cost of reading and caching the original dataset.

## Analytical Workload

Each data-reuse strategy performs repeated category and regional aggregations.

Category calculations include:

- Average transaction amount
- Total transaction amount
- Transaction count

Regional calculations include:

- Total quantity
- Average transaction amount
- Transaction count

The aggregate results are collected because they contain only a small number of rows.

## Interpreting the Results

Caching is not automatically faster for every workload.

Cache performance depends on:

- Number of repeated actions
- Cost of recomputing the source DataFrame
- Available memory
- Dataset size
- Serialisation overhead
- Storage speed
- Cache-population cost
- Competition from other processes

A small dataset or one-time computation may complete faster without caching because cache materialisation introduces additional work.

Caching becomes more valuable when the same expensive intermediate DataFrame is reused repeatedly.

## MEMORY_AND_DISK Versus Memory-Only Storage

`MEMORY_AND_DISK` is useful when the DataFrame may not fit entirely in available memory.

Its advantages include:

- Reduced risk of recomputing evicted partitions
- More predictable execution for larger datasets
- Graceful fallback to disk storage

Its trade-offs include:

- Additional disk input and output
- Potentially slower access than memory-only storage
- Increased local-storage consumption

## Spark Event Logs

Spark event logging is enabled during benchmark execution.

Event files can be analysed using Spark History Server to inspect:

- Jobs
- Stages
- Tasks
- Executors
- Storage usage
- Shuffle reads and writes
- Task duration
- Scheduling delay
- Data locality
- Failed operations

A local History Server can be started with:

    $SPARK_HOME/sbin/start-history-server.sh

The default interface is:

    http://localhost:18080

The configured event-log directory is:

    spark-events/

## Spark UI

During active execution, Spark exposes a live interface at:

    http://localhost:4040

The interface can help identify:

- Long-running stages
- Uneven task durations
- Data skew
- Excessive shuffle activity
- Cached DataFrames
- Storage-level behaviour
- Executor memory usage
- Failed or retried tasks

The live interface is available only while the Spark application is running.

## Benchmark Outputs

The partition benchmark produces:

    benchmark-results/partition_results.csv

The caching benchmark produces:

    benchmark-results/cache_results.csv

These generated files are excluded from version control because their values depend on the machine and runtime environment.

## Tools Used

- Apache Spark 4.2.0
- PySpark
- Spark DataFrames
- Spark SQL functions
- Spark storage levels
- Spark event logging
- Spark UI
- OpenJDK 17
- Python 3
- JSON Lines
- CSV
- Ubuntu Linux
- Bash
- Git
- procps
- tree

## Key Skills Demonstrated

- Spark performance benchmarking
- Partition-count optimisation
- Controlled experiment design
- DataFrame caching
- Cache materialisation
- Data persistence
- Storage-level comparison
- Iterative analytical computation
- Explicit schema design
- Spark aggregation
- JVM warm-up
- Repeated benchmark trials
- Median-based ranking
- Performance-improvement calculation
- Spark event-log generation
- Spark UI analysis
- Automated dataset validation
- Automated benchmark validation
- Machine-readable result generation
- Linux resource analysis
- Reproducible synthetic data generation
- Production-aware repository management

## Real-World Use Case

This workflow can support performance tuning for e-commerce analytics, financial processing, customer segmentation, machine-learning feature preparation, event analysis, fraud-detection pipelines, recommendation systems, and scheduled data-processing workloads.

The same measurement approach can be applied before deploying a Spark application to Kubernetes, YARN, cloud-managed Spark platforms, or multi-node standalone clusters.

By measuring partition and caching behaviour locally, engineers can identify poor assumptions before consuming larger cluster resources.

## Lessons Learned

- Spark performance tuning should be based on measurements rather than fixed rules.
- Partition count should reflect both available parallelism and workload size.
- Too few partitions can underutilise CPU resources.
- Too many partitions can create task-scheduling and shuffle overhead.
- Repeated trials are more trustworthy than a single benchmark.
- Median timing reduces the influence of unusual measurements.
- Caching provides value only when cached data is reused.
- Cached data must be materialised with an action.
- Persistence strategies should reflect available memory and recomputation cost.
- `MEMORY_AND_DISK` provides resilience when a dataset cannot fit entirely in memory.
- Spark event logs provide evidence for analysing stages and tasks after execution.
- The fastest result on one machine may not remain fastest on another machine.
- Generated benchmark output should not be presented as universally applicable.
- Generated datasets and runtime artefacts should remain outside version control.

## Troubleshooting Log

Issue:
The original setup used an archived Apache Spark release.

Resolution:
Used Apache Spark 4.2.0 with OpenJDK 17 and Python 3.12 compatibility.

Issue:
The original setup installed both the Spark binary distribution and a separate pip-based PySpark package.

Resolution:
Used the PySpark runtime included with the Spark distribution to prevent JVM and Python library version conflicts.

Issue:
The original partition comparison used only one timing per configuration.

Resolution:
Added three measured trials, JVM warm-up, alternating execution order, and median-based ranking.

Issue:
The original scripts used automatic JSON schema inference.

Resolution:
Defined an explicit Spark schema for predictable types and reduced startup work.

Issue:
Cached data can appear slower when cache-population time is misunderstood.

Resolution:
Measured cache materialisation separately while also reporting complete strategy execution time.

Issue:
Calling `cache()` does not immediately populate the cache.

Resolution:
Triggered a count action before repeated analytical operations.

Issue:
Cached data can remain allocated after execution.

Resolution:
Used blocking `unpersist()` and cleared the Spark catalogue cache between strategies.

Issue:
A fixed claim that caching must improve performance by a particular percentage can be misleading.

Resolution:
Reported the measured result from the actual environment and allowed slower cached outcomes to remain valid evidence.

Issue:
Benchmark results can be accidentally committed even though they are machine-specific.

Resolution:
Excluded generated benchmark CSV files, input data, Spark event logs, and runtime artefacts from version control.
