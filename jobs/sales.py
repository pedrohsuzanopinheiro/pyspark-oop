import json
import logging
import os
import re
import sys
from typing import Callable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

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
    spark_stop(spark_session)


def open_file(file_path: str) -> dict:
    if isinstance(file_path, str) and os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
        return data


def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        class_pyspark.SparkClass(config={}).spark_start(conf)


def spark_stop(spark: SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None


if __name__ == "__main__":
    main(project_dir)
