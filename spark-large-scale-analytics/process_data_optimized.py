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


def create_spark():

    return (
        SparkSession.builder
        .appName("Optimized GB Processing")
        .master("local[*]")
        .config("spark.driver.memory","6g")
        .config("spark.sql.shuffle.partitions","8")
        .config("spark.sql.adaptive.enabled","true")
        .getOrCreate()
    )


spark=create_spark()

spark.sparkContext.setLogLevel("WARN")

df=(
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(str(INPUT_FILE))
)

df=df.withColumn("date",to_date("date"))

clean_df=(
    df.na.drop()
      .filter(col("amount")>0)
      .repartition("country")
      .cache()
)

clean_df.count()

country_metrics=(
    clean_df.groupBy("country")
            .agg(
                count("*").alias("total_transactions"),
                round(sum("amount"),2).alias("total_revenue"),
                round(avg("amount"),2).alias("avg_transaction")
            )
)

category_trends=(
    clean_df
    .withColumn("year",year("date"))
    .withColumn("month",month("date"))
    .groupBy("product_category","year","month")
    .agg(
        round(sum("amount"),2).alias("total_sales"),
        count("*").alias("transaction_count")
    )
)

top_customers=(
    clean_df.groupBy("customer_id")
            .agg(
                round(sum("amount"),2).alias("total_spent"),
                count("*").alias("purchase_count")
            )
            .orderBy(col("total_spent").desc())
            .limit(100)
)

country_metrics.write.mode("overwrite").option("header",True).csv(str(OUTPUT_DIR/"country_metrics"))

category_trends.write.mode("overwrite").parquet(str(OUTPUT_DIR/"category_trends"))

top_customers.write.mode("overwrite").json(str(OUTPUT_DIR/"top_customers"))

spark.stop()

print("Optimized processing complete.")
