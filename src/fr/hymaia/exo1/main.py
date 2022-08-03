import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    print("Hello world!")
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")

    df_wordcount = wordcount(df, 'text')

    df_wordcount.write \
        .mode("overwrite") \
        .option("header", True) \
        .partitionBy("count") \
        .parquet("data/exo1/output")

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
