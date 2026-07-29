from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    month,
    round,
    sum,
    to_date,
    year,
)

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "transactions.csv"
OUTPUT_DIR = BASE_DIR / "output"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GB Scale Processing")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_data(spark):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(INPUT_FILE))
    )

    return df.withColumn("date", to_date("date"))


def clean_data(df):
    return (
        df.na.drop()
          .filter(col("amount") > 0)
    )


def calculate_country_metrics(df):

    return (
        df.groupBy("country")
          .agg(
                count("*").alias("total_transactions"),
                round(sum("amount"),2).alias("total_revenue"),
                round(avg("amount"),2).alias("avg_transaction_value")
          )
          .orderBy(col("total_revenue").desc())
    )


def calculate_category_trends(df):

    return (
        df.withColumn("year",year("date"))
          .withColumn("month",month("date"))
          .groupBy("product_category","year","month")
          .agg(
                round(sum("amount"),2).alias("total_sales"),
                count("*").alias("transaction_count")
          )
          .orderBy("year","month",col("total_sales").desc())
    )


def find_top_customers(df, top_n=100):

    return (
        df.groupBy("customer_id")
          .agg(
                round(sum("amount"),2).alias("total_spent"),
                count("*").alias("purchase_count")
          )
          .orderBy(col("total_spent").desc())
          .limit(top_n)
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    OUTPUT_DIR.mkdir(exist_ok=True)

    print("\nLoading data...")

    df = load_data(spark)

    df.printSchema()

    print(f"Total records loaded: {df.count():,}")

    print("\nCleaning data...")

    clean_df = clean_data(df)

    print(f"Records after cleaning: {clean_df.count():,}")

    print("\nCalculating country metrics...")

    country_metrics = calculate_country_metrics(clean_df)

    country_metrics.show(10,False)

    (
        country_metrics
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header",True)
        .csv(str(OUTPUT_DIR/"country_metrics"))
    )

    print("\nCalculating category trends...")

    category_trends = calculate_category_trends(clean_df)

    category_trends.show(20,False)

    (
        category_trends
        .write
        .mode("overwrite")
        .parquet(str(OUTPUT_DIR/"category_trends"))
    )

    print("\nFinding top customers...")

    top_customers = find_top_customers(clean_df,100)

    top_customers.show(100,False)

    (
        top_customers
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(str(OUTPUT_DIR/"top_customers"))
    )

    spark.stop()

    print("\nProcessing complete!")


if __name__ == "__main__":
    main()
