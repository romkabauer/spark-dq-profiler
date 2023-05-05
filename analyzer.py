from exceptions import LackDataForAnalysisError


class Analyzer:
    def __init__(self, profiling_results: list):
        a = [
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
        ]
        self.profiling_results = [x for x in profiling_results if not x.get("ERROR")]

    def suggest_constraints(self) -> list[dict]:
        possible_constraints = []
        for table in self.profiling_results:
            suggestions_for_table = {
                "TABLE_NAME": table.get("TABLE_NAME"),
                "SUGGESTED_CONSTRAINTS": {},
            }

            if not table.get("TABLE_PROFILING_INFO") \
               or not table["TABLE_PROFILING_INFO"].get("COLUMNS"):
                raise LackDataForAnalysisError

            for col_name, col_stat in table["TABLE_PROFILING_INFO"]["COLUMNS"].items():
                suggestions_for_table["SUGGESTED_CONSTRAINTS"][col_name] = \
                    self.__identify_constraints_for_column(col_stat)

            possible_constraints.append(suggestions_for_table)

        return possible_constraints

    def __identify_constraints_for_column(self, col_stat: dict) -> list[dict]:
        return [{
            "DESCRIPTION": "EXISTENCE",
            "BASE_INFORMATION": col_stat,
            "STATEMENT": "MERGE STATEMENT FOR ADF FRAMEWORK",
        }]
