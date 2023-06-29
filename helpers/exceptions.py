from helpers.object_types import TableType


class IncorrectConfigError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""Config lacks 'schema', 'name' or 'path' keys for the table to profile.
                         Example of config:
                         [
                            {
                                "datasource_type": "SNF",
                                "schema": "UKI_DTM_SNU",
                                "name": "DIM_CUSTOMER",
                            },
                            {
                                "datasource_type": "CSV",
                                "path": "some/path/file_name.csv",
                                "name": "DIM_CUSTOMER",
                            },
                         ]""")


class UnexpectedTableType(Exception):
    def __init__(self, expected_type: str, message: str = None):
        super().__init__(f"""Table type in config should be one of {expected_type}.
                             Example of config:
                             [
                                {{
                                    "datasource_type": "{expected_type}",
                                    "path": "data/UKI_DTM_SNU.csv",
                                    "name": "DIM_CUSTOMER",
                                }}
                             ]""")


class LackDataForAnalysisError(Exception):
    def __init__(self, data_provided, message: str = None):
        super().__init__(f"""Data for analysis should contain info about table name and columns statistic.
                         Example data structure for analysis:
                         [
                            {{
                                "TABLE_NAME": "EXAMPLE_TABLE_NAME",
                                "TABLE_PROFILING_INFO": {{
                                    "COLUMNS": {{
                                        "EXAMPLE_COLUMN_NAME": {{
                                            "col_type": "TEXT",
                                            "count": 5,
                                            "share": 1.0,
                                            "uniq": 5,
                                            "uniq_upper": 5,
                                            "top_value": "1",
                                            "top_freq": 1,
                                            "top_share": 0.2
                                        }},
                                    }}
                                }}
                            }}
                        ]
                        Data provided:
                        {data_provided}""")


class UndefinedColumnTypeError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""Data stat for analysis lacks column type.
                         Example data structure for column stat:
                         {
                            "col_type": "TEXT",
                            "count": 5,
                            "share": 1.0,
                            "uniq": 5,
                            "uniq_upper": 5,
                            "top_value": "1",
                            "top_freq": 1,
                            "top_share": 0.2
                         }""")


class UndefinedDataFrameError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""CSVExecutor.execute_select method cannot operate without pandas.DataFrame defined
        within 'df_table' argument""")
