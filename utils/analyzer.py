from helpers.exceptions import LackDataForAnalysisError, UndefinedColumnTypeError
from helpers.object_types import ColumnType
from utils.constraint_identifier import ConstraintIdentifierBuilder


class Analyzer:
    def __init__(self,
                 profiling_results: list,
                 constraint_identification_rules: dict[str, list[dict]],
                 add_adf_framework_template: bool = False):
        self._profiling_results = [x for x in profiling_results if not x.get("ERROR")]
        self.constraint_identification_rules = constraint_identification_rules
        self.add_adf_framework_template = add_adf_framework_template

    @property
    def profiling_results(self):
        return self._profiling_results

    @profiling_results.setter
    def profiling_results(self, profiling_results: list):
        self.__init__(profiling_results, self.constraint_identification_rules)

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
                if col_stat.get("ERROR"):
                    suggestions_for_table["SUGGESTED_CONSTRAINTS"][col_name] = {
                        "POSSIBLE_CONSTRAINTS": [],
                        "BASE_INFO": col_stat,
                    }
                else:
                    suggestions_for_table["SUGGESTED_CONSTRAINTS"][col_name] = {
                        "POSSIBLE_CONSTRAINTS": self.__identify_constraints_for_column(col_stat=col_stat,
                                                                                       col_name=col_name,
                                                                                       tbl_name=table.get("TABLE_NAME")),
                        "BASE_INFO": col_stat,
                    }

            suggested_constraints.append(suggestions_for_table)

        return suggested_constraints

    def __identify_constraints_for_column(self,
                                          col_stat: dict,
                                          col_name: str,
                                          tbl_name: str) -> list[dict]:
        if not col_stat.get("col_type"):
            raise UndefinedColumnTypeError

        if col_stat["col_type"] not in ColumnType.list_possible_types():
            return [
                {
                    "DESCRIPTION": "No identified constraints",
                }
            ]

        return ConstraintIdentifierBuilder(base_info=col_stat,
                                           related_column=col_name,
                                           related_table=tbl_name,
                                           constraint_identification_rules=
                                           self.constraint_identification_rules[col_stat["col_type"]],
                                           add_adf_framework_template=self.add_adf_framework_template) \
            .build_identifier() \
            .identify_constraints()
