import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("exo4") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    df_category = extract_category_name(df)
    df_category.write.parquet("outputNoUdf")


def extract_category_name(df, category_col="category", category_name_col="category_name"):
    return df.withColumn(category_name_col, f.when(f.col(category_col) < 6, "food").otherwise("furniture"))

