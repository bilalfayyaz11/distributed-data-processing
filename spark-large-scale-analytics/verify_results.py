from pathlib import Path
from pyspark.sql import SparkSession

BASE=Path(__file__).resolve().parent

spark=(
    SparkSession.builder
    .appName("Verification")
    .master("local[*]")
    .getOrCreate()
)

country=spark.read.option("header",True).csv(str(BASE/"output/country_metrics"))

category=spark.read.parquet(str(BASE/"output/category_trends"))

customers=spark.read.json(str(BASE/"output/top_customers"))

print()

print("===== COUNTRY METRICS =====")
print(country.count())
country.show(5,False)

print()

print("===== CATEGORY TRENDS =====")
print(category.count())
category.show(5,False)

print()

print("===== TOP CUSTOMERS =====")
print(customers.count())
customers.show(5,False)

spark.stop()
