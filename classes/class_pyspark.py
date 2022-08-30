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
        # get_settings(spark_session)
        return spark_session

    def import_data(
        self,
        spark: SparkSession,
        data_path: str,
        pattern: Optional[str] = None,
    ) -> DataFrame:
        def file_or_directory(data_path: str) -> str:
            if isinstance(data_path, str) and os.path.exists(data_path):
                if os.path.isdir(data_path):
                    return "dir"
                elif os.path.isfile(data_path):
                    return "file"

        def open_directory(data_path: str, pattern: Optional[str] = None):
            if isinstance(data_path, str) and os.path.exists(data_path):
                new_list = SparkClass(self.config).list_directory(
                    data_path, pattern
                )
                print(new_list)

        def open_file(data_path: str):
            if isinstance(data_path, str) and os.path.exists(data_path):
                pass

        path_type = file_or_directory(data_path)
        open_directory(data_path, pattern) if path_type == "dir" else None

    def list_directory(
        self, directory: str, pattern: Optional[str] = None
    ) -> list:
        """Recursevely list the files of a directory"""

        def recursive_file_list(directory: str) -> list:
            if os.path.exists(directory):
                file_list = []
                for dir_path, dir_name, filenames in os.walk(directory):
                    for filename in filenames:
                        file_list.append(f"{dir_path}/{filename}")
                return file_list

        def filter_files(file_list: list, pattern: str):
            """If pattern is included then filter files"""
            return [x for x in file_list if re.search(rf"{pattern}", x)]

        file_list = recursive_file_list(directory)
        return (
            file_list
            if pattern == None
            else filter_files(file_list, pattern)
            if pattern != ""
            else None
        )

    def create_dataframe(
        self, spark: SparkSession, file_list: list, file_type: str
    ) -> DataFrame:
        def df_from_csv(file_list: list) -> DataFrame:
            pass

        def df_from_json(file_list: list) -> DataFrame:
            pass
