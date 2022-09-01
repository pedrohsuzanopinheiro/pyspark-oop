import json
import logging
import os
import re
import sys
from typing import Callable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger("py4j")

sys.path.insert(1, project_dir)
from classes import class_pyspark


def main(project_dir: str) -> None:
    """Starts a Spark job"""

    conf = open_file(f"{project_dir}/json/sales.json")
    spark_session = spark_start(conf=conf)

    transactions_df = import_data(
        spark_session, f"{project_dir}/test-data/sales/transactions", ".json$"
    )
    customers_df = import_data(
        spark_session, f"{project_dir}/test-data/sales/customers.csv"
    )
    products_df = import_data(
        spark_session, f"{project_dir}/test-data/sales/products.csv"
    )
    transform_data(spark_session, transactions_df, customers_df, products_df)

    spark_stop(spark_session)


def open_file(file_path: str) -> dict:
    if isinstance(file_path, str) and os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
        return data


def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return class_pyspark.SparkClass(config={}).spark_start(conf)


def spark_stop(spark: SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None


def import_data(
    spark: SparkSession, data_path: str, pattern: Optional[str] = None
) -> DataFrame:
    if isinstance(spark, SparkSession):
        return class_pyspark.SparkClass(config={}).import_data(
            spark, data_path, pattern
        )


def show_my_schema(df: DataFrame) -> None:
    if isinstance(df, DataFrame):
        df.show()
        df.printSchema()
        print(f"Total rows: {df.count()}")


def transform_data(
    spark: SparkSession,
    transactions_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    def clean_transactions(df: DataFrame) -> DataFrame:
        if isinstance(df, DataFrame):
            df1 = df.withColumn("basket_explode", explode(col("basket"))).drop(
                "basket"
            )
            df2 = (
                df1.select(
                    col("customer_id"),
                    col("date_of_purchase"),
                    col("basket_explode.*"),
                )
                .withColumn(
                    "date_of_purchase_formated",
                    col("date_of_purchase").cast("Date"),
                )
                .withColumn("price", col("price").cast("Integer"))
            )
            return df2

    def clean_customers(df: DataFrame) -> DataFrame:
        if isinstance(df, DataFrame):
            df1 = df.withColumn(
                "loyalty_score", col("loyalty_score").cast("Integer")
            )
            return df1

    clean_transactions(transactions_df)
    clean_customers(customers_df)


if __name__ == "__main__":
    main(project_dir)
