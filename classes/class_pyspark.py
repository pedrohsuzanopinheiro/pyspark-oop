import json
import os
import re
import sys
from typing import Callable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class SparkClass:
    def __init__(self, config: dict) -> None:
        self.config = config

    def spark_start(self, kwargs: dict) -> SparkSession:
        """Crates a Spark Session"""
        master = kwargs["spark_conf"]["master"]
        app_name = kwargs["spark_conf"]["app_name"]
        log_level = kwargs["log"]["level"]

        def create_session(
            master: Optional[str] = "local[*]",
            app_name: Optional[str] = "my_app",
        ) -> SparkSession:
            spark = (
                SparkSession.builder.appName(app_name)
                .master(master)
                .getOrCreate()
            )
            return spark

        def set_logging(
            spark_session: SparkSession, log_level: Optional[str] = None
        ) -> None:
            spark_session.sparkContext.setLogLevel(log_level) if isinstance(
                log_level, str
            ) else None

        def get_settings(spark_session: SparkSession) -> None:
            """Show Spark settings"""
            print(
                f"\033[1;33m{spark_session.sparkContext.getConf().getAll()}\033[0m"
            )

        spark_session = create_session(master, app_name)
        set_logging(spark_session, log_level)
        get_settings(spark_session)
        return spark_session
