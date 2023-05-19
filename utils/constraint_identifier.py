from datetime import datetime


class ConstraintIdentifier:
    def __init__(self,
                 base_info: dict,
                 related_column: str,
                 related_table: str):
        self.base_info      = base_info
        self.related_column = related_column
        self.related_table  = related_table

        self.nullability       : dict | None = None
        self.minmax            : dict | None = None
        self.determined_list   : dict | None = None
        self.inconsistent_names: dict | None = None
        self.future_dates      : dict | None = None
        self.foreign_key       : dict | None = None

    def identify_constraints(self) -> list[dict]:
        return list(
            filter(
                lambda x: x is not None,
                [
                    self.nullability,
                    self.minmax,
                    self.determined_list,
                    self.inconsistent_names,
                    self.future_dates,
                    self.foreign_key,
                ]
            )
        )

    def identify_nullability(self, nullability_threshold: float = 0.99) -> None:
        if not self.base_info.get("share"):
            return

        if nullability_threshold < self.base_info.get("share") <= 1:
            self.nullability = {
                "DESCRIPTION": "NULLABILITY: Maybe this column should be non-nullable",
                "BASE_INFORMATION": self.base_info,
                "MERGE_INTO_ADF_FRM": f"""SELECT
                                    <DS><DL>3<NUM> as DQ_RULE_ID,
                                    (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>') AS DATASOURCE_ID,
                                    NULL DQ_ACTION_ID,
                                    NULL DQ_RULE_DESC,
                                    'COMPLETENESS' AS META_DQ_DIMENSION,
                                    'NULLABILITY' AS META_RULE_TYPE,
                                    'SMOKE,REGRESSION,INTEGRATION' AS META_SUITES,
                                    'EXTENDED' AS META_RULE_LEVEL,
                                    '{self.related_table}' AS PARAM_TABLE_NAME,
                                    '{self.related_column}' AS PARAM_TABLE_COLUMN,
                                    NULL PARAM_MIN,
                                    NULL PARAM_MAX,
                                    NULL PARAM_REGEXP,
                                    NULL PARAM_S2T_VIEW,
                                    NULL PARAM_DEFAULT,
                                    TRUE IS_ACTIVE""",
            }
        elif self.base_info.get("share") == 0:
            self.nullability = {
                "DESCRIPTION": "ONLY NULLS: column contains only nulls, maybe something wrong with ingestion",
                "BASE_INFORMATION": self.base_info,
            }

    def identify_min_max_range(self) -> None:
        if not self.base_info.get("min") or not self.base_info.get("max") or not self.base_info.get("uniq"):
            return

        if self.base_info.get("min") != self.base_info.get("max") \
           and not (self.base_info.get("min") == 0 and self.base_info.get("max") == 1)\
           and self.base_info.get("uniq") > 2:
            self.minmax = {
                "DESCRIPTION": "MINMAX: Maybe this column has business-determined validity range",
                "BASE_INFORMATION": self.base_info,
                "MERGE_INTO_ADF_FRM": f"""SELECT
                                    <DS><DL>7<NUM> as DQ_RULE_ID,
                                    (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>') AS DATASOURCE_ID,
                                    NULL DQ_ACTION_ID,
                                    NULL DQ_RULE_DESC,
                                    'VALIDITY' AS META_DQ_DIMENSION,
                                    'MINMAX' AS META_RULE_TYPE,
                                    'REGRESSION,INTEGRATION' AS META_SUITES,
                                    'EXTENDED' AS META_RULE_LEVEL,
                                    '{self.related_table}' AS PARAM_TABLE_NAME,
                                    '{self.related_column}' AS PARAM_TABLE_COLUMN,
                                    {self.base_info.get("min")} AS PARAM_MIN,
                                    {self.base_info.get("max")} AS PARAM_MAX,
                                    NULL PARAM_REGEXP,
                                    NULL PARAM_S2T_VIEW,
                                    NULL PARAM_DEFAULT,
                                    TRUE IS_ACTIVE""",
            }

    def identify_determined_list(self, list_size_threshold: int = 10) -> None:
        if not self.base_info.get("uniq"):
            return

        if 0 < self.base_info.get("uniq") < list_size_threshold:
            self.determined_list = {
                    "DESCRIPTION": "DETERMINED LIST: Maybe column should contain values only from determined list",
                    "BASE_INFORMATION": self.base_info,
                    "MERGE_INTO_ADF_FRM": f"""SELECT
                                        <DS><DL>8<NUM> as DQ_RULE_ID,
                                        (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>') AS DATASOURCE_ID,
                                        NULL DQ_ACTION_ID,
                                        NULL DQ_RULE_DESC,
                                        'VALIDITY' AS META_DQ_DIMENSION,
                                        'REGEXP' AS META_RULE_TYPE,
                                        'INTEGRATION,EXTENDED' AS META_SUITES,
                                        'EXTENDED' AS META_RULE_LEVEL,
                                        '{self.related_table}' AS PARAM_TABLE_NAME,
                                        '{self.related_column}' AS PARAM_TABLE_COLUMN,
                                        NULL PARAM_MIN,
                                        NULL PARAM_MAX,
                                        '^<VAL1>|<VAL2>|<VAL3>$' PARAM_REGEXP,
                                        NULL PARAM_S2T_VIEW,
                                        NULL PARAM_DEFAULT,
                                        TRUE IS_ACTIVE"""
                }

    def identify_inconsistent_names(self) -> None:
        if not self.base_info.get("uniq_upper") or not self.base_info.get("uniq"):
            return

        if self.base_info.get("uniq_upper") != self.base_info.get("uniq"):
            self.inconsistent_names = {
                    "DESCRIPTION": "INCONSISTENT NAMES: Maybe some unique values have same meaning and should be uppercased",
                    "BASE_INFORMATION": self.base_info,
                    "MERGE_INTO_ADF_FRM": f"""SELECT
                                        <DS><DL>8<NUM> as DQ_RULE_ID,
                                        (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>') AS DATASOURCE_ID,
                                        NULL DQ_ACTION_ID,
                                        NULL DQ_RULE_DESC,
                                        'CONSISTENCY' AS META_DQ_DIMENSION,
                                        'DEFAULT' AS META_RULE_TYPE,
                                        'REGRESSION,INTEGRATION' AS META_SUITES,
                                        'EXTENDED' AS META_RULE_LEVEL,
                                        '{self.related_table}' AS PARAM_TABLE_NAME,
                                        '{self.related_column}' AS PARAM_TABLE_COLUMN,
                                        NULL PARAM_MIN,
                                        NULL PARAM_MAX,
                                        NULL PARAM_REGEXP,
                                        NULL PARAM_S2T_VIEW,
                                        concat('select iff(count(distinct {self.related_column}) <> COUNT({self.related_column}), 1, 0) from ',
                                                (SELECT DATASOURCE_RELATED_SCHEMA FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>')::varchar,
                                                '.{self.related_table}' AS PARAM_DEFAULT,
                                        TRUE IS_ACTIVE"""
                }

    def identify_dates_in_future(self) -> None:
        if not self.base_info.get("max") \
           and not self.base_info.get("min"):
            return

        if datetime.fromisoformat(self.base_info.get("max")) > datetime.now()\
           or datetime.fromisoformat(self.base_info.get("min")) > datetime.now():
            self.future_dates = {
                "DESCRIPTION": "FUTURE DATES: Maybe this column should not contain dates from the future",
                "BASE_INFORMATION": self.base_info,
                "MERGE_INTO_ADF_FRM": f"""SELECT
                                    <DS><DL>8<NUM> as DQ_RULE_ID,
                                    (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>') AS DATASOURCE_ID,
                                    NULL DQ_ACTION_ID,
                                    NULL DQ_RULE_DESC,
                                    'VALIDITY' AS META_DQ_DIMENSION,
                                    'DEFAULT' AS META_RULE_TYPE,
                                    'REGRESSION,INTEGRATION' AS META_SUITES,
                                    'EXTENDED' AS META_RULE_LEVEL,
                                    '{self.related_table}' AS PARAM_TABLE_NAME,
                                    '{self.related_column}' AS PARAM_TABLE_COLUMN,
                                    NULL PARAM_MIN,
                                    NULL PARAM_MAX,
                                    NULL PARAM_REGEXP,
                                    NULL PARAM_S2T_VIEW,
                                    concat('select iff(count({self.related_column}) > 0), 1, 0) from ',
                                            (SELECT DATASOURCE_RELATED_SCHEMA FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = '<DS>')::varchar,
                                            '.{self.related_table} WHERE {self.related_column} > getdate()' AS PARAM_DEFAULT,
                                    TRUE IS_ACTIVE""",
            }

    def identify_foreign_key(self) -> None:
        if not self.base_info.get("count") \
           or not self.base_info.get("top_freq") \
           or not self.base_info.get("top_share"):
            return

        if round(self.base_info.get("top_freq")
           / (self.base_info.get("count") if self.base_info.get("count") != 0 else 1), 3) \
           == round(self.base_info.get("top_share"), 3):
            self.foreign_key = {
                "DESCRIPTION": "POSSIBLE FOREIGN KEY: Maybe this column is a foreign key and it is worth to check for CONSISTENCY",
                "BASE_INFORMATION": self.base_info,
            }


class ConstraintIdentifierBuilder:
    def __init__(self,
                 base_info: dict,
                 related_column: str,
                 related_table: str):
        self.identifier = ConstraintIdentifier(base_info=base_info,
                                               related_column=related_column,
                                               related_table=related_table)

    def build_identifier(self) -> ConstraintIdentifier:
        return self.identifier

    def include_identification_nullability(self, *args, **kwargs):
        self.identifier.identify_nullability(*args, **kwargs)
        return self

    def include_identification_minmax(self):
        self.identifier.identify_min_max_range()
        return self

    def include_identification_determined_list(self, *args, **kwargs):
        self.identifier.identify_determined_list(*args, **kwargs)
        return self

    def include_identification_inconsistent_names(self):
        self.identifier.identify_inconsistent_names()
        return self

    def include_identification_future_dates(self):
        self.identifier.identify_dates_in_future()
        return self

    def include_identification_foreign_key(self):
        self.identifier.identify_foreign_key()
        return self
