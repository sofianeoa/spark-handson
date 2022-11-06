import unittest
import pyspark.sql.functions as f
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_clean_job import filter, add_column
from src.fr.hymaia.exo2.spark_aggregate_job import pop_by_dep


def join_clients_city(df_clients, df_city):
        df_clients2 = filter(df_clients)
        return df_clients2.join(df_city,"zip","left")


class TestExo2(unittest.TestCase):
    def test_filter(self):
# GIVEN
        input_data = spark.createDataFrame(
            [
                Row(25),
                Row(18),
                Row(12)
            ]
        ).toDF("age")
# WHEN
        expected_data = spark.createDataFrame(
            [
                Row(25),
                Row(18),
            ]
        )
#THEN
        actual = filter(input_data)

        self.assertCountEqual(actual.collect(), expected_data.collect())

    def test_add_column(self):
# GIVEN
        input_data = spark.createDataFrame(
            [
                Row("17540"),
                Row("11410"),
                Row("20110"),
                Row("20200")
            ]
        ).toDF("zip")
# WHEN
        expected_data = spark.createDataFrame(
            [
                Row("17540","17"),
                Row("11410","11"),
                Row("20110","2A"),
                Row("20200","2B")
            ]
        ).toDF("zip","dep")
#THEN
        actual = add_column(input_data, "result", f.col("zip"))

        self.assertEqual(actual.collect(), expected_data.collect())

    def test_pop_by_dep(self):
# GIVEN
        input_data = spark.createDataFrame(
            [
                Row("42"),
                Row("42"),
                Row("17"),
                Row("11"),
                Row("17"),
                Row("17"),
                Row("8"),
                Row("11"),
                Row("42"),
                Row("42"),
                Row("42")
            ]
        ).toDF("departement")
# WHEN
        expected_data = spark.createDataFrame(
            [
                Row("11",2),
                Row("42",5),
                Row("17",3),
                Row("8",1)
            ]
        ).toDF("departement","count")
#THEN
        actual = pop_by_dep(input_data, f.col("departement"))

        self.assertEqual(actual.collect(), expected_data.collect())
    
    def test_integration_join(self):
# GIVEN
        input_data_clients = spark.createDataFrame(
            [
                Row("Michel", 27 , "42800"),
                Row("Alice", 16 , "75020"),
                Row("Pierre", 45 , "17540"),
                Row("Brigite", 35 , "11410")
            ]
        ).toDF("name", "age", "zip")

        input_data_city= spark.createDataFrame(
            [
                Row("11410", "BELFLOU"),
                Row("17540", "ANGLIERS"),
                Row("75020", "paris"),
                Row("42800", "Lyon"),
                Row("50000", "rien")
            ]
        ).toDF("zip", "city")
# WHEN
        expected_data_join = spark.createDataFrame(
            [
                Row("75020", "Cussac", 27, "paris"),
                Row("17540", "Pierre", 45, "ANGLIERS"),
                Row("11410", "Brigite", 35, "BELFLOU"),
                Row("42800", "Michel", 27, "Lyon")
            ]
        ).toDF("zip", "name", "age", "city")
#THEN
        actual = join_clients_city(input_data_clients, input_data_city)

        self.assertEqual(actual.collect(), expected_data_join.collect())
#END_________________________________________________________________________________________________________________________________sfn 