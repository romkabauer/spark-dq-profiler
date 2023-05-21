from abc import abstractmethod
import asyncio

import pandas as pd
import dateutil.parser

from config.snf_config import SNF_CONFIG
from utils.executors import SnowflakeExecutor, CSVExecutor
from helpers.object_types import TableType, ColumnType
from helpers.db_objects import SNFTable, SNFTableColumn, CSVTableColumn
from helpers.exceptions import IncorrectConfigError, UnexpectedTableType


class Profiler:
    def __init__(self, table_config: list[dict], executor: SnowflakeExecutor | CSVExecutor = None):
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
    def __init__(self, table_config: list[dict], executor: SnowflakeExecutor | CSVExecutor = None):
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
        columns_to_describe = columns

        if not columns:
            columns_to_describe = await table.get_columns_list(self.executor)

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


class CSVProfiler(Profiler):
    def __init__(self,
                 table_config: list[dict],
                 separator: str = ',',
                 executor: SnowflakeExecutor | CSVExecutor = CSVExecutor()):
        super().__init__(table_config=table_config,
                         executor=executor)
        self.supported_datasource_type = "CSV"
        self.separator = separator

    async def get_tables_descriptions(self):
        tables_to_profile = []

        for table_info in self.table_config:
            if not table_info.get("datasource_type") == TableType.CSV.value:
                raise UnexpectedTableType(TableType.CSV.value)
            if not table_info.get("path"):
                raise IncorrectConfigError()

            table = pd.read_csv(table_info.get("path"),
                                sep=self.separator)
            table.name = table_info.get("name")
            tables_to_profile.append(table)

        tables_description = [self.__describe_table(tbl, tbl.name) for tbl in tables_to_profile]
        return await asyncio.gather(*tables_description)

    async def __describe_table(self, table: pd.DataFrame, table_name: str = None):
        if table.empty:
            return {
                "ERROR": "Empty dataframe",
            }

        table_description = {
            "TABLE_NAME": f"{table_name}",
            "TABLE_PROFILING_INFO": {
                "TABLE_COUNT": table.shape[0],
                "COLUMNS": {},
            }
        }
        table_description_info = table.describe(include='all')
        for col in table.columns:
            col_type = ColumnType.TEXT.value

            try:
                dateutil.parser.parse(str(table[col][0]))
                col_type = ColumnType.TIMESTAMP.value
            except ValueError:
                pass

            if any(dtype in str(table_description_info[col].dtype) for dtype in ["float", "int"]):
                col_type = ColumnType.NUMERIC.value

            table_description["TABLE_PROFILING_INFO"]["COLUMNS"][col] = await self.__collect_column_stat(CSVTableColumn(
                configured_table_name=table_name,
                column_name=col,
                df_table=table,
                col_type=col_type))

        return table_description

    async def __collect_column_stat(self, column: CSVTableColumn) -> dict:
        common_stat = await column.get_count(self.executor)
        quantitative_stat = await column.calc_column_stat(self.executor)
        return {
            **common_stat,
            **quantitative_stat
        }
