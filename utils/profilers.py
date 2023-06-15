from abc import abstractmethod
import asyncio

from pyspark.sql import DataFrame, types

from config.snf_config import SNF_CONFIG
from utils.executors import SnowflakeExecutor, SparkExecutor
from helpers.object_types import TableType, ColumnType
from helpers.db_objects import SNFTable, SNFTableColumn, TableColumn
from helpers.exceptions import IncorrectConfigError, UnexpectedTableType


class Profiler:
    def __init__(self, table_config: list[dict], executor: SnowflakeExecutor | SparkExecutor = None):
        self.executor = executor
        self._table_config = table_config
        self.supported_datasource_type = "DEFAULT"

    @property
    def table_config(self):
        return self._table_config

    @table_config.setter
    def table_config(self, table_config: list[dict]):
        self.__init__(table_config=table_config,
                      executor=self.executor)

    @abstractmethod
    def get_tables_descriptions(self):
        raise NotImplementedError


class SNFProfiler(Profiler):
    def __init__(self, table_config: list[dict], executor: SnowflakeExecutor | SparkExecutor = None):
        super().__init__(table_config=table_config,
                         executor=SnowflakeExecutor(SNF_CONFIG))
        self.supported_datasource_type = "SNF"

    async def get_tables_descriptions(self):
        tables_to_profile = []

        for table_info in self.table_config:
            if not table_info.get("datasource_type") == TableType.SNF.value:
                raise UnexpectedTableType(TableType.SNF.value)
            if not table_info.get("schema") or not table_info.get("name"):
                raise IncorrectConfigError()

            table = SNFTable(table_info.get("schema"), table_info.get("name"), table_info.get("columns"))
            tables_to_profile.append(table)

        tables_description = [self.__describe_table(tbl, tbl.columns) for tbl in tables_to_profile]
        return await asyncio.gather(*tables_description)

    async def __describe_table(self, table: SNFTable, columns: list[str] = None) -> dict:
        table_cnt_info = await table.get_count(self.executor)

        if not table_cnt_info:
            return {}
        elif table_cnt_info.get("ERROR"):
            return table_cnt_info

        table_description = {
            "TABLE_NAME": f"{table.name}",
            "TABLE_PROFILING_INFO": {
                **table_cnt_info,
                "COLUMNS": {}
            }
        }
        columns_to_describe = columns if columns else await table.get_columns_list(self.executor)

        for col in columns_to_describe:
            table_description["TABLE_PROFILING_INFO"]["COLUMNS"][col] = await self.__collect_column_stat(SNFTableColumn(
                table.schema,
                table.name,
                col))
        return table_description

    async def __collect_column_stat(self, column: SNFTableColumn) -> dict:
        common_stat = await column.get_count(self.executor)
        quantitative_stat = await column.calc_column_stat(self.executor)
        return {
            **common_stat,
            **quantitative_stat
        }


class SparkProfiler(Profiler):
    def __init__(self,
                 table_config: list[dict],
                 csv_separator: str = ',',
                 executor: SnowflakeExecutor | SparkExecutor = SparkExecutor()):
        super().__init__(table_config=table_config,
                         executor=executor)
        self.supported_datasource_type = "SPARK"
        self.csv_separator = csv_separator

    def read_data_inferring_data_type(self, table_info: dict):
        match table_info.get('path').split('.')[-1]:
            case 'csv':
                table = self.executor.spark_session.read.csv(path=table_info.get('path'),
                                                             inferSchema=True,
                                                             header=True,
                                                             sep=self.csv_separator)
                table.name = table_info.get('name')
                return table
            case 'parquet':
                table = self.executor.spark_session.read.option("mergeSchema", "true").parquet(table_info.get('path'))
                table.name = table_info.get('name')
                return table
            case _:
                print(f'Empty dataframe will be created instead of data from {table_info.get("path")}'
                      f'since file type is not supported by this profiler')
                return self.executor.spark_session.createDataFrame([])

    async def get_tables_descriptions(self):
        tables_to_profile = []

        for table_info in self.table_config:
            if table_info.get("datasource_type") and not table_info.get("datasource_type") == TableType.SPARK.value:
                raise UnexpectedTableType(TableType.SPARK.value)
            if not table_info.get("path"):
                raise IncorrectConfigError()

            tables_to_profile.append(self.read_data_inferring_data_type(table_info))

        tables_description = [self.__describe_table(tbl) for tbl in tables_to_profile]
        return await asyncio.gather(*tables_description)

    async def __describe_table(self, table: DataFrame):
        if table.isEmpty():
            return {
                "ERROR": "Empty dataframe",
            }

        table_description = {
            "TABLE_NAME": f"{table.name}",
            "TABLE_PROFILING_INFO": {
                "TABLE_COUNT": table.count(),
                "COLUMNS": {},
            }
        }

        for col in table.columns:
            col_type = ColumnType.TEXT.value

            if table.schema[col] in [types.LongType(), types.NumericType(), types.FloatType(),
                                     types.DecimalType(), types.DoubleType()]:
                col_type = ColumnType.NUMERIC.value
            elif table.schema[col] in [types.DateType()]:
                col_type = ColumnType.TIMESTAMP.value

            table_description["TABLE_PROFILING_INFO"]["COLUMNS"][col] = await self.__collect_column_stat(TableColumn(
                schema='',
                table_name=table.name,
                configured_table_name=table.name,
                column_name=col,
                df_table=table,
                col_type=col_type))

        return table_description

    async def __collect_column_stat(self, column: TableColumn) -> dict:
        common_stat = await column.get_count(self.executor)
        quantitative_stat = await column.calc_column_stat(self.executor)
        return {
            **common_stat,
            **quantitative_stat
        }
