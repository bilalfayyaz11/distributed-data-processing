from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark session connected to the standalone cluster."""
    return (
        SparkSession.builder
        .appName("DistributedWordCount")
        .master("spark://localhost:7077")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )


def perform_wordcount(spark, text_data):
    """Count words across a distributed Spark RDD."""
    return (
        spark.sparkContext
        .parallelize(text_data, 2)
        .flatMap(lambda line: line.split())
        .map(lambda word: word.lower().strip(".,:;!?"))
        .filter(lambda word: bool(word))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda first, second: first + second)
        .sortByKey()
        .collect()
    )


def main():
    sample_text = [
        "Apache Spark is a distributed computing framework",
        "Spark provides high-level APIs in Java Scala Python",
        "Distributed computing enables processing large datasets",
    ]

    spark = None

    try:
        spark = create_spark_session()

        print("\n===== SPARK APPLICATION =====")
        print(f"Application: {spark.sparkContext.appName}")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Application ID: {spark.sparkContext.applicationId}")

        results = perform_wordcount(spark, sample_text)

        print("\n===== WORD COUNT RESULTS =====")

        for word, count in results:
            print(f"{word}: {count}")

        expected_counts = {
            "spark": 2,
            "distributed": 2,
            "computing": 2,
        }

        actual_counts = dict(results)

        for word, expected_count in expected_counts.items():
            actual_count = actual_counts.get(word)

            if actual_count != expected_count:
                raise RuntimeError(
                    f"Validation failed for '{word}': "
                    f"expected {expected_count}, received {actual_count}"
                )

        print("\n===== VALIDATION =====")
        print("Distributed word-count validation: PASSED")

    except Exception as error:
        print(f"\nApplication failed: {error}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped cleanly.")


if __name__ == "__main__":
    main()
