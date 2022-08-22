import json
import os
import re
import sys
from typing import Callable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class SparkClass:
    def __init__(self, conf: dict) -> None:
        self.config = conf

    def spark_start(self, kwargs: dict) -> SparkSession:
        """Crates a Spark Session"""
        master = kwargs["spark_conf"]["master"]
        app_name = kwargs["spark_conf"]["app_name"]
        log_level = kwargs["log"]["level"]

        spark = (
            SparkSession.builder.appName(app_name).master(master).getOrCreate()
        )
        return spark
