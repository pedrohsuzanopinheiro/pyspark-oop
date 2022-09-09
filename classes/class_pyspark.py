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

    def open_json(self, file_path: str) -> dict:
        if isinstance(file_path, str) and os.path.exists(file_path):
            with open(file_path, "r") as file:
                data = json.load(file)
            return data

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

        def open_directory(
            spark: SparkSession,
            data_path: str,
            pattern: Optional[str] = None,
        ):
            if isinstance(data_path, str) and os.path.exists(data_path):
                file_list = SparkClass(self.config).list_directory(
                    data_path, pattern
                )
                file_type = get_unique_file_extensions(file_list)
                if file_type:
                    return SparkClass(self.config).create_dataframe(
                        spark, file_list, file_type
                    )

        def open_file(get_file_extension: Callable, file_path: str):
            if isinstance(file_path, str) and os.path.exists(file_path):
                file_list = [file_path]
                file_type = get_file_extension(file_path)
                return SparkClass(self.config).create_dataframe(
                    spark, file_list, file_type
                )

        def get_unique_file_extensions(file_list: list) -> list:
            if isinstance(file_list, list) and len(file_list) > 0:
                exts = list(set(os.path.splitext(f)[1] for f in file_list))
                return exts[0][1:] if len(exts) == 1 else None

        path_type = file_or_directory(data_path)
        return (
            open_directory(spark, data_path, pattern)
            if path_type == "dir"
            else open_file(
                SparkClass(self.config).get_file_extension, data_path
            )
            if path_type == "file"
            else None
        )

    def get_file_extension(self, file_path: str) -> str:
        """Get file extension from single file"""
        if isinstance(file_path, str) and os.path.exists(file_path):
            file_name, file_extension = os.path.splitext(file_path)
            return file_extension[1:] if file_extension else None

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
            if isinstance(pattern, str):
                return [x for x in file_list if re.search(rf"{pattern}", x)]
            else:
                return file_list

        file_list = recursive_file_list(directory)
        return filter_files(file_list, pattern)

    def create_dataframe(
        self, spark: SparkSession, file_list: list, file_type: str
    ) -> DataFrame:
        def df_from_csv(file_list: list) -> DataFrame:
            if isinstance(file_list, list) and len(file_list) > 0:
                df = (
                    spark.read.format("csv")
                    .option("header", "true")
                    .option("mode", "DROPMALFORMED")
                    .load(file_list)
                )
                return df

        def df_from_json(file_list: list) -> DataFrame:
            if isinstance(file_list, list) and len(file_list) > 0:
                df = (
                    spark.read.format("json")
                    .option("mode", "PERMISSIVE")
                    .option("primitiveAsString", "true")
                    .load(file_list)
                )
                return df

        def make_df(file_list: list, file_type: str) -> DataFrame:
            return (
                df_from_csv(file_list)
                if file_type == "csv"
                else df_from_json(file_list)
                if file_type == "json"
                else None
            )

        return make_df(file_list, file_type)

    def debug_dataframe(self, df: DataFrame, filename: str) -> None:
        print(filename)
