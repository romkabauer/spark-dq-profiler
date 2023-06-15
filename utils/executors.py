from abc import abstractmethod
import pandas as pd
import sqlalchemy.exc
from snowflake.sqlalchemy import URL
from sqlalchemy.engine import create_engine
from pyspark.sql import SparkSession, DataFrame

from helpers.exceptions import UndefinedDataFrameError


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Executor(metaclass=Singleton):
    @abstractmethod
    def execute_select(self, sql: str, **kwargs) -> DataFrame:
        pass


class SnowflakeExecutor(Executor):
    def __init__(self, snf_config: dict):
        self.engine = create_engine(URL(**snf_config))
        self.spark_session = SparkSession.builder.getOrCreate()

    async def execute_select(self, sql: str, **kwargs) -> DataFrame:
        try:
            df = pd.read_sql_query(sql, self.engine)
        except sqlalchemy.exc.ProgrammingError as e:
            df = pd.DataFrame([e.args[0], e.statement])
        df = self.spark_session.createDataFrame(df)
        return df

    def shutdown(self):
        self.engine.dispose()


class SparkExecutor(Executor):
    def __init__(self):
        self.spark_session = SparkSession.builder.getOrCreate()

    async def execute_select(self, sql: str, **kwargs) -> DataFrame:
        try:
            df_table = kwargs["df_table"]
            df_table.createOrReplaceTempView(df_table.name)
            df = self.spark_session.sql(sql)
        except KeyError:
            raise UndefinedDataFrameError
        except Exception as e:
            df = self.spark_session.createDataFrame([(e, sql)])
        return df
