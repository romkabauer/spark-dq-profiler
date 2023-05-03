import sqlalchemy.exc
from snowflake.sqlalchemy import URL
from sqlalchemy.engine import create_engine
import pandas as pd

from snf_config import SNF_USER, SNF_PASS, SNF_ORG, SNF_DB, SNF_WH, SNF_ROLE


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SnowflakeExecutor(metaclass=Singleton):
    def __init__(self):
        self.engine = create_engine(URL(
            account=SNF_ORG,
            user=SNF_USER,
            password=SNF_PASS,
            database=SNF_DB,
            warehouse=SNF_WH,
            role=SNF_ROLE,
        ))

    async def execute_select(self, sql: str):
        try:
            df = pd.read_sql_query(sql, self.engine)
        except sqlalchemy.exc.ProgrammingError as e:
            df = pd.DataFrame([e.args[0], e.statement])
        return df

    def shutdown(self):
        self.engine.dispose()
