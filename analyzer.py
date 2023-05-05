from exceptions import LackDataForAnalysisError, UndefinedColumnTypeError


class Analyzer:
    def __init__(self, profiling_results: list):
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
        if not col_stat.get("col_type"):
            raise UndefinedColumnTypeError

        match col_stat["col_type"]:
            case "NUMERIC":
                return self.__identify_constraints_for_column_numeric(col_stat)
            case "TIMESTAMP":
                return self.__identify_constraints_for_column_timestamp(col_stat)
            case "TEXT":
                return self.__identify_constraints_for_column_text(col_stat)
            case _:
                return [
                    {
                        "DESCRIPTION": "No identified constraints",
                        "BASE_INFORMATION": col_stat,
                        "STATEMENT": "No identified constraints",
                    }
                ]

    @staticmethod
    def __identify_constraints_for_column_numeric(col_stat: dict) -> list[dict]:
        return [
            {
                "DESCRIPTION": "Should contain only values from determined list",
                "BASE_INFORMATION": col_stat,
                "STATEMENT": "MERGE STATEMENT FOR ADF TESTS",
            }
        ]

    @staticmethod
    def __identify_constraints_for_column_timestamp(col_stat: dict) -> list[dict]:
        return [
            {
                "DESCRIPTION": "Should not contain dates in future",
                "BASE_INFORMATION": col_stat,
                "STATEMENT": "MERGE STATEMENT FOR ADF TESTS",
            }
        ]

    @staticmethod
    def __identify_constraints_for_column_text(col_stat: dict) -> list[dict]:
        return [
            {
                "DESCRIPTION": "Catch values with same meaning. but different spelling",
                "BASE_INFORMATION": col_stat,
                "STATEMENT": "MERGE STATEMENT FOR ADF TESTS",
            },
            {
                "DESCRIPTION": "Should contain only values from determined list",
                "BASE_INFORMATION": col_stat,
                "STATEMENT": "MERGE STATEMENT FOR ADF TESTS",
            }
        ]
