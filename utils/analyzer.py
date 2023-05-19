from helpers.exceptions import LackDataForAnalysisError, UndefinedColumnTypeError
from utils.constraint_identifier import ConstraintIdentifierBuilder


class Analyzer:
    def __init__(self, profiling_results: list):
        self._profiling_results = [x for x in profiling_results if not x.get("ERROR")]

    @property
    def profiling_results(self):
        return self._profiling_results

    @profiling_results.setter
    def profiling_results(self, profiling_results: list):
        self.__init__(profiling_results)

    def suggest_constraints(self) -> list[dict]:
        suggested_constraints = []
        for table in self.profiling_results:
            suggestions_for_table = {
                "TABLE_NAME": table.get("TABLE_NAME"),
                "SUGGESTED_CONSTRAINTS": {},
            }

            if not table.get("TABLE_PROFILING_INFO") \
               or not table["TABLE_PROFILING_INFO"].get("COLUMNS"):
                raise LackDataForAnalysisError(data_provided=table)

            for col_name, col_stat in table["TABLE_PROFILING_INFO"]["COLUMNS"].items():
                suggestions_for_table["SUGGESTED_CONSTRAINTS"][col_name] = \
                    self.__identify_constraints_for_column(col_stat=col_stat,
                                                           col_name=col_name,
                                                           tbl_name=table.get("TABLE_NAME"))

            suggested_constraints.append(suggestions_for_table)

        return suggested_constraints

    @staticmethod
    def __identify_constraints_for_column(col_stat: dict,
                                          col_name: str,
                                          tbl_name: str) -> list[dict]:
        if not col_stat.get("col_type"):
            raise UndefinedColumnTypeError

        match col_stat["col_type"]:
            case "NUMERIC":
                return ConstraintIdentifierBuilder(base_info=col_stat,
                                                   related_column=col_name,
                                                   related_table=tbl_name) \
                    .include_identification_nullability() \
                    .include_identification_minmax() \
                    .include_identification_inconsistent_names() \
                    .include_identification_determined_list(list_size_threshold=2) \
                    .build_identifier() \
                    .identify_constraints()
            case "TIMESTAMP":
                return ConstraintIdentifierBuilder(base_info=col_stat,
                                                   related_column=col_name,
                                                   related_table=tbl_name) \
                    .include_identification_nullability() \
                    .include_identification_future_dates() \
                    .build_identifier() \
                    .identify_constraints()
            case "TEXT":
                return ConstraintIdentifierBuilder(base_info=col_stat,
                                                   related_column=col_name,
                                                   related_table=tbl_name) \
                    .include_identification_nullability() \
                    .include_identification_inconsistent_names() \
                    .include_identification_determined_list(list_size_threshold=7) \
                    .include_identification_foreign_key() \
                    .build_identifier() \
                    .identify_constraints()
            case _:
                return [
                    {
                        "DESCRIPTION": "No identified constraints",
                        "BASE_INFORMATION": col_stat,
                        "STATEMENT": "No identified constraints",
                    }
                ]
