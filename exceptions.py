class IncorrectConfigError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""Config lacks 'schema', 'name' or both keys for the table to profile.
                         Example of config:
                         [
                            {
                                "schema": "UKI_DTM_SNU",
                                "name": "DIM_CUSTOMER",
                            }
                         ]""")


class LackDataForAnalysisError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""Data for analysis should contain info about table name and columns statistic.
                         Example of data structure for analysis:
                         [
                            {
                                "TABLE_NAME": "EXAMPLE_TABLE_NAME",
                                "TABLE_PROFILING_INFO": {
                                    "COLUMNS": {
                                        "EXAMPLE_COLUMN_NAME": {
                                            "count": 5,
                                            "share": 1.0,
                                            "uniq": 5,
                                            "uniq_upper": 5,
                                            "top_value": "1",
                                            "top_freq": 1,
                                            "top_share": 0.2
                                        },
                                    }
                                }
                            }
                        ]""")
