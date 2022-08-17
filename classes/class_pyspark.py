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
        pass
