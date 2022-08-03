import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    df_city = spark.read.option("header", True).csv("src/resources/exo2/city_zipcode.csv")
    df_clients = spark.read.option("header", True).csv("src/resources/exo2/clients_bdd.csv")

    # Filter age > 18
    df_clients_major = filter_on_age(df_clients)

    # Join with cities
    df_clients_major_cities = join_on_zip(df_clients_major, df_city)

    # Add departement
    df_clients_major_cities_dpt = add_departement(df_clients_major_cities)

    df_clients_major_cities_dpt.write \
        .mode("overwrite") \
        .option("header", True) \
        .parquet("data/exo2/clean")


def filter_on_age(df, col_name="age"):
    return df.filter(f.col(col_name) >= 18)


def join_on_zip(df, df_city, on="zip", how="inner"):
    return df.join(other=df_city, on=on, how=how)


def add_departement(df, zip_col="zip", dpt_col_name="departement"):
    df_dpt_raw = df.withColumn(dpt_col_name,
                               f.when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) <= "20190"), "2A") \
                                .when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) > "20190"), "2B") \
                                .otherwise(f.substring(f.col(zip_col), 1, 2)))
    return df_dpt_raw
