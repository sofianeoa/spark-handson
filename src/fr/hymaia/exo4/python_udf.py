import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def main():
    spark = SparkSession.builder \
        .appName("exo4") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    df_category = df.withColumn("category_name",
                                extract_category_name_udf(f.col("category")))
    df_category.write.parquet("outputPythonUdf")


def extract_category_name(category):
    if int(category) < 6:
        return "food"
    else:
        return "furniture"

extract_category_name_udf = f.udf(extract_category_name, StringType())
