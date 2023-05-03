import asyncio

from executor import SnowflakeExecutor
from db_objects import Table, TableColumn
from exceptions import IncorrectConfigError


class Profiler:
    def __init__(self, table_config: list[dict]):
        self.executor = SnowflakeExecutor()
        self.table_config = table_config

    async def get_tables_descriptions(self):
        tables_to_profile = []

        for table_info in self.table_config:
            if not table_info.get("schema") or not table_info.get("name"):
                raise IncorrectConfigError()

            table = Table(table_info.get("schema"), table_info.get("name"), table_info.get("columns"))
            tables_to_profile.append(table)

        tables_description = [self.__describe_table(tbl, tbl.columns) for tbl in tables_to_profile]
        return await asyncio.gather(*tables_description)

    async def __describe_table(self, table: Table, columns: list[str] = None) -> dict:
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
            table_description["TABLE_PROFILING_INFO"]["COLUMNS"][col] = await self.__collect_column_stat(TableColumn(
                table.schema,
                table.name,
                col))
        return table_description

    async def __collect_column_stat(self, column: TableColumn) -> dict:
        common_stat = await column.get_count(self.executor)
        quantitative_stat = await column.calc_column_stat(self.executor)
        return {
            **common_stat,
            **quantitative_stat
        }
