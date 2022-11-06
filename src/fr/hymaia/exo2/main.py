import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder    \
                        .appName('wordcount')    \
                        .master('local[*]') \
                        .getOrCreate()
    df_cityzipcode = spark.read.option("header", "true").csv("src/resources/exo2/city_zipcode.csv")
    df_clients = spark.read.option("header", "true").csv("src/resources/exo2/clients_bdd.csv")

    df_clients2 = filter(df_clients)
    df_join = df_clients2.join(df_cityzipcode,"zip","left")


    df_departement = addColumn(df_join, "departement", f.col("zip"))
    df_departement.show(df_departement.count())

    df_departement.write.mode("overwrite").parquet("data/exo2/output")


def filter(df):
    return df.where(f.col("age")>=18)

def addColumn(df, name_col, focus_col ):
    df.printSchema()
    return df.withColumn(name_col, f.when((focus_col > "20000") & (focus_col <= "20190" ), "2A")
                                .when((focus_col > "20190") & (focus_col <= "20999"), "2B")
                                .otherwise(f.substring(focus_col, 1,2)))