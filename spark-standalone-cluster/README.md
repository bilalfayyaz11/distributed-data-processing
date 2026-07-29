# Apache Spark Standalone Cluster

## What This Does

This implementation deploys and validates an Apache Spark standalone cluster consisting of one master process and two independently configured worker processes on an Ubuntu system.

The cluster accepts distributed applications through the Spark master endpoint, allocates CPU and memory resources across available workers, and executes parallel workloads using Spark's resilient distributed dataset processing model.

A production-style PySpark word-count application is included to demonstrate distributed transformations, partitioned execution, result aggregation, application validation, and controlled Spark session shutdown. The environment is also verified through Apache Spark's official SparkPi workload and interactive Spark shell execution.

This implementation demonstrates the core architecture used by distributed data-processing and machine-learning platforms before those environments are expanded across multiple physical machines, virtual machines, or containerised infrastructure.

## Architecture

    +---------------------------------------------------+
    | Application Submission Layer                      |
    |                                                   |
    | spark-submit                                      |
    | spark-shell                                       |
    | PySpark WordCount                                 |
    | Apache Spark SparkPi                              |
    +-------------------------+-------------------------+
                              |
                              | spark://localhost:7077
                              v
    +---------------------------------------------------+
    | Spark Standalone Master                           |
    |                                                   |
    | Process: Master                                   |
    | RPC Port: 7077                                    |
    | Web UI: 8080                                      |
    |                                                   |
    | Responsibilities:                                 |
    | - Application registration                        |
    | - Worker registration                             |
    | - Resource allocation                             |
    | - Job scheduling coordination                     |
    +-------------------------+-------------------------+
                              |
                    +---------+---------+
                    |                   |
                    v                   v
    +-------------------------+   +-------------------------+
    | Spark Worker 1          |   | Spark Worker 2          |
    |                         |   |                         |
    | Service Port: 7078      |   | Service Port: 7079      |
    | Web UI: 8081            |   | Web UI: 8082            |
    | CPU Allocation: 1 core  |   | CPU Allocation: 1 core  |
    | Memory: 1 GB            |   | Memory: 1 GB            |
    +------------+------------+   +------------+------------+
                 |                             |
                 +--------------+--------------+
                                |
                                v
    +---------------------------------------------------+
    | Distributed Execution Layer                       |
    |                                                   |
    | RDD partitioning                                  |
    | flatMap transformations                           |
    | key-value mapping                                 |
    | reduceByKey aggregation                           |
    | executor-side Python processing                   |
    +-------------------------+-------------------------+
                              |
                              v
    +---------------------------------------------------+
    | Validated Application Output                      |
    |                                                   |
    | Distributed word counts                           |
    | Expected-value validation                         |
    | SparkPi result                                    |
    | Master and worker logs                            |
    +---------------------------------------------------+

## Prerequisites

- Ubuntu 24.04 or a compatible Linux distribution
- At least 4 GB of system memory
- At least 2 available CPU cores
- sudo access
- OpenJDK 17
- Python 3
- Python pip
- Git
- wget
- tar
- procps
- net-tools
- tree
- Network access to download Apache Spark
- Local access to TCP ports 7077–7079 and 8080–8082

## Setup & Installation

Update the package index and install the required system dependencies:

sudo apt update

sudo apt install -y \
  openjdk-17-jdk-headless \
  python3-pip \
  wget \
  curl \
  procps \
  net-tools \
  tree

Verify Java and Python:

java -version

javac -version

python3 --version

jps

Download Apache Spark:

cd ~

wget https://downloads.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz

Extract the downloaded archive:

tar -xzf spark-3.5.8-bin-hadoop3.tgz

Move Spark into its system installation directory:

sudo mv spark-3.5.8-bin-hadoop3 /opt/spark

Configure the shell environment:

cat >> ~/.bashrc << 'SPARK_ENV'

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

SPARK_ENV

Reload the shell configuration:

source ~/.bashrc

Verify the Spark installation:

spark-submit --version

## Spark Configuration

Create the Spark environment configuration:

cd $SPARK_HOME/conf

cp spark-env.sh.template spark-env.sh

cat >> spark-env.sh << 'SPARK_WORKER_CONFIG'

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_WORKER_MEMORY=1g
export SPARK_WORKER_CORES=1

SPARK_WORKER_CONFIG

Create the default application configuration:

cp spark-defaults.conf.template spark-defaults.conf

cat >> spark-defaults.conf << 'SPARK_DEFAULTS'

spark.master                     spark://localhost:7077
spark.executor.memory            512m
spark.driver.memory              512m
spark.executor.cores             1

SPARK_DEFAULTS

The reduced executor and worker memory values allow the cluster to run reliably on a system with approximately 4 GB of memory while leaving capacity for the operating system, driver, master, worker JVMs, and Python executor processes.

## How to Reproduce

Start the Spark master:

$SPARK_HOME/sbin/start-master.sh

Wait for the service to initialise:

sleep 5

Confirm that the master is running:

jps | grep Master

Start the first worker with dedicated service and Web UI ports:

SPARK_WORKER_PORT=7078 \
SPARK_WORKER_WEBUI_PORT=8081 \
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

Wait for worker registration:

sleep 5

Start the second worker with different ports:

SPARK_WORKER_PORT=7079 \
SPARK_WORKER_WEBUI_PORT=8082 \
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

Wait for worker registration:

sleep 5

Verify all Spark processes:

jps

Expected Spark processes include:

Master

Worker

Worker

Verify the network listeners:

ss -ltnp | grep -E '7077|7078|7079|8080|8081|8082'

The master Web UI is available locally at:

http://localhost:8080

Worker Web UIs are available locally at:

http://localhost:8081

http://localhost:8082

On a remote virtual machine, replace localhost with the machine's public IP address or public DNS name. Restrict any inbound firewall or cloud security-group access to trusted source IP addresses.

Create the application directory:

mkdir -p ~/spark-apps

cd ~/spark-apps

Run Python syntax validation:

python3 -m py_compile wordcount.py

Submit the distributed word-count application:

spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --executor-memory 512m \
  --total-executor-cores 2 \
  wordcount.py

The application should display distributed word counts and confirm:

Distributed word-count validation: PASSED

Run Apache Spark's official SparkPi workload:

spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --executor-memory 512m \
  --total-executor-cores 2 \
  --class org.apache.spark.examples.SparkPi \
  "$SPARK_HOME"/examples/jars/spark-examples_*.jar \
  100

A successful execution prints a calculated value similar to:

Pi is roughly 3.14

Launch an interactive Spark shell:

spark-shell --master spark://localhost:7077

Run an interactive distributed calculation:

val data = sc.parallelize(1 to 1000)

val result = data.map(x => x * 2).reduce(_ + _)

println(s"Result: $result")

Exit the shell:

:quit

Inspect the Spark logs:

ls -lh $SPARK_HOME/logs

tail -n 30 $SPARK_HOME/logs/*master*.out

tail -n 30 $SPARK_HOME/logs/*worker*.out

Stop the workers:

$SPARK_HOME/sbin/stop-worker.sh

Stop the master:

$SPARK_HOME/sbin/stop-master.sh

Verify that the cluster processes have stopped:

jps

## Application Behaviour

The included PySpark application performs the following workflow:

1. Creates a SparkSession connected to the standalone master.
2. Distributes the input text across two RDD partitions.
3. Splits each line into individual words.
4. Normalises words to lowercase.
5. Removes basic punctuation.
6. Filters empty values.
7. Maps each word to a key-value pair.
8. Aggregates occurrences with reduceByKey.
9. Sorts and collects the final result.
10. Validates selected word counts against expected values.
11. Stops the Spark session safely.

The validation checks confirm that the words spark, distributed, and computing each appear twice in the sample dataset.

## Tools Used

- Apache Spark 3.5.8
- Spark Standalone Cluster Manager
- PySpark
- Spark SQL
- SparkSession
- SparkContext
- Resilient Distributed Datasets
- OpenJDK 17
- Python 3
- Scala-based Spark shell
- Bash
- Ubuntu Linux
- Git
- wget
- procps
- ss
- net-tools
- tree

## Key Skills Demonstrated

- Apache Spark standalone cluster deployment
- Distributed master-worker architecture
- Spark resource allocation
- Worker process isolation
- Distributed application submission
- PySpark application development
- RDD partitioning and transformation
- Key-value aggregation with reduceByKey
- Executor and driver memory configuration
- CPU-core allocation
- JVM process inspection
- Service-port validation
- Cluster log analysis
- Distributed workload verification
- Interactive Spark shell usage
- Application-level result validation
- Graceful Spark session shutdown
- Linux-based data platform administration
- Distributed-computing troubleshooting
- Production-aware service exposure

## Real-World Use Case

A Spark cluster is commonly used by data engineering, machine learning, analytics, and platform teams to process datasets that are too large or computationally expensive for a single sequential application. Typical workloads include feature engineering, model-training data preparation, log processing, fraud detection, recommendation pipelines, batch analytics, telemetry aggregation, and extract-transform-load workflows. In a production environment, the master and workers would normally run on separate machines or be managed through a platform such as Kubernetes, but the scheduling, resource-allocation, application-submission, logging, and monitoring principles demonstrated here remain directly relevant.

## Lessons Learned

- Spark's master process coordinates applications and resources but delegates actual workload execution to registered workers.
- Multiple workers running on one machine require distinct service and Web UI ports to avoid process conflicts.
- Resource settings must account for the operating system, JVM overhead, driver process, executors, and Python workers rather than allocating all physical memory directly to Spark.
- A successful Spark process start does not guarantee a healthy cluster; worker registration, open ports, logs, and completed distributed jobs must also be verified.
- Application-level validation provides stronger evidence than relying only on successful command exit codes.
- Spark's official example workloads are useful as independent infrastructure tests when debugging custom applications.
- Standalone cluster security is not enabled by default, so Web UIs and service ports should never be exposed broadly to the public internet.

## Troubleshooting Log

Issue:
The original implementation specified Apache Spark 3.4.1, which is an older release and does not provide the preferred baseline for a current Ubuntu 24.04 environment.

Resolution:
Installed Apache Spark 3.5.8 and retained the Hadoop 3 binary distribution to provide a more current and compatible runtime.

Issue:
The original instructions used Java 11, while the Ubuntu 24.04 environment did not include Java or the Java compiler.

Resolution:
Installed OpenJDK 17, configured JAVA_HOME as /usr/lib/jvm/java-17-openjdk-amd64, and verified java, javac, and jps.

Issue:
The supplied configuration allocated 2 GB of memory to each of two workers on a machine with a minimum requirement of only 4 GB.

Resolution:
Reduced each worker to 1 GB and configured driver and executor memory at 512 MB to preserve capacity for the operating system and Spark control processes.

Issue:
The original instructions attempted to start the same worker command twice without assigning distinct service or Web UI ports.

Resolution:
Configured Worker 1 with service port 7078 and Web UI port 8081, and Worker 2 with service port 7079 and Web UI port 8082.

Issue:
The supplied configuration combined SPARK_WORKER_INSTANCES=2 with two manual worker-start commands.

Resolution:
Removed SPARK_WORKER_INSTANCES and launched each worker explicitly to make process creation, port allocation, and troubleshooting deterministic.

Issue:
The Web UI instructions referenced localhost, which is inaccessible from a user's local browser when Spark runs on a remote virtual machine.

Resolution:
Documented access through the instance public IP address or public DNS and required firewall or cloud security-group restrictions.

Issue:
The initial Python application contained incomplete TODO sections and pass statements.

Resolution:
Implemented SparkSession creation, RDD partitioning, flatMap processing, word normalisation, reduceByKey aggregation, sorted result collection, exception handling, result validation, and clean session shutdown.

Issue:
A running master and worker processes alone did not prove that distributed execution was functional.

Resolution:
Validated the cluster with the custom PySpark word-count application, Apache Spark's official SparkPi workload, interactive Spark shell processing, process inspection, port verification, and Spark log review.
