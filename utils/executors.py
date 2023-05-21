from abc import abstractmethod
import pandas as pd
from pandasql import sqldf
import sqlalchemy.exc
from snowflake.sqlalchemy import URL
from sqlalchemy.engine import create_engine

from helpers.exceptions import UndefinedDataFrameError


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Executor(metaclass=Singleton):
    @abstractmethod
    def execute_select(self, sql: str, **kwargs) -> pd.DataFrame:
        pass


class SnowflakeExecutor(Executor):
    def __init__(self, snf_config: dict):
        self.engine = create_engine(URL(**snf_config))

    async def execute_select(self, sql: str, **kwargs) -> pd.DataFrame:
        try:
            df = pd.read_sql_query(sql, self.engine)
        except sqlalchemy.exc.ProgrammingError as e:
            df = pd.DataFrame([e.args[0], e.statement])
        return df

    def shutdown(self):
        self.engine.dispose()


class CSVExecutor(Executor):
    async def execute_select(self, sql: str, **kwargs) -> pd.DataFrame:
        try:
            df_table = kwargs["df_table"]
            df = sqldf(sql, locals())
        except KeyError:
            raise UndefinedDataFrameError
        except Exception as e:
            df = pd.DataFrame([e, sql])
        return df
