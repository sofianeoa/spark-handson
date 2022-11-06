import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import unittest


def main():
    spark = SparkSession.builder    \
                        .appName('wordcount')    \
                        .master('local[*]') \
                        .getOrCreate()
                        
    df = spark.read.option("header", "true").parquet("src/resources/exo2/clean")
    df_result = pop_by_dep(df, f.col("departement"))

    df_result.coalesce(1).write.mode("overwrite").csv("data/exo2/aggregate.csv")


def pop_by_dep(df, col_name):
    df_result = df.groupBy(col_name).count().sort(f.desc("count"), f.asc("departement"))
    df_result.show(df_result.count())
    return df_result 