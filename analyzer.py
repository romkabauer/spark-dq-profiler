class Analyzer:
    def __init__(self, profiling_results: list):
        # self.profiling_results = profiling_results
        self.profiling_results = [
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
        self.profiling_results = [x for x in self.profiling_results if not x.get("ERROR")]

    def suggest_constraints(self) -> list[dict]:
        return [
            {
                "TABLE_NAME": "EXAMPLE_TABLE_NAME",
                "SUGGESTED_CONSTRAINTS": {
                    "EXAMPLE_COLUMN_NAME": [
                        {
                            "DESCRIPTION": "EXISTENCE",
                            "BASE_INFORMATION": [x["TABLE_PROFILING_INFO"]["COLUMNS"]["EXAMPLE_COLUMN_NAME"]
                                                 for x in self.profiling_results
                                                 if x["TABLE_NAME"] == "EXAMPLE_TABLE_NAME"],
                            "STATEMENT": "MERGE STATEMENT FOR ADF FRAMEWORK"
                        }
                    ]
                },
            }
        ]
