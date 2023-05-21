from datetime import datetime
import dateutil.parser
from pytz import UTC
from functools import reduce


class ConstraintIdentifier:
    def __init__(self,
                 base_info: dict,
                 related_column: str,
                 related_table: str,
                 add_adf_framework_template: bool = False):
        self.base_info      = base_info
        self.related_column = related_column
        self.related_table  = related_table

        self.nullability       : dict | None = None
        self.minmax            : dict | None = None
        self.determined_list   : dict | None = None
        self.inconsistent_names: dict | None = None
        self.future_dates      : dict | None = None
        self.foreign_key       : dict | None = None

        self.add_adf_framework_template_flag: bool = add_adf_framework_template

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

    def identify_nullability(self, nullability_threshold: float = 0.99, **kwargs):
        if not self.base_info.get("share"):
            return self

        if nullability_threshold < self.base_info.get("share") <= 1:
            self.nullability = {
                "DESCRIPTION": "NULLABILITY: Maybe this column should be non-nullable",
            }
            if self.add_adf_framework_template_flag:
                self.nullability["MERGE_INTO_ADF_FRM"] = f"""SELECT
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
                                    TRUE IS_ACTIVE"""

        elif self.base_info.get("share") == 0:
            self.nullability = {
                "DESCRIPTION": "ONLY NULLS: column contains only nulls, maybe something wrong with ingestion",
                "BASE_INFORMATION": self.base_info,
            }
        return self

    def identify_min_max_range(self, **kwargs):
        # FUTURE_ENHANCEMENT: exclude ID columns and leave columns with values satisfy the regexp
        if not self.base_info.get("min") or not self.base_info.get("max") or not self.base_info.get("uniq"):
            return self

        if self.base_info.get("min") != self.base_info.get("max") \
           and not (self.base_info.get("min") == 0 and self.base_info.get("max") == 1)\
           and self.base_info.get("uniq") > 2:
            self.minmax = {
                "DESCRIPTION": "MINMAX: Maybe this column has business-determined validity range",
            }
            if self.add_adf_framework_template_flag:
                self.minmax["MERGE_INTO_ADF_FRM"] = f"""SELECT
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
                                    TRUE IS_ACTIVE"""
        return self

    def identify_determined_list(self, list_size_threshold: int = 10, **kwargs):
        if not self.base_info.get("uniq"):
            return self

        if 0 < self.base_info.get("uniq") < list_size_threshold:
            self.determined_list = {
                    "DESCRIPTION": "DETERMINED LIST: Maybe column should contain values only from determined list",
                }
            if self.add_adf_framework_template_flag:
                self.determined_list["MERGE_INTO_ADF_FRM"] = f"""SELECT
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
        return self

    def identify_inconsistent_names(self, **kwargs):
        if not self.base_info.get("uniq_upper") or not self.base_info.get("uniq"):
            return self

        if self.base_info.get("uniq_upper") != self.base_info.get("uniq"):
            self.inconsistent_names = {
                    "DESCRIPTION": "INCONSISTENT NAMES: Maybe some unique values have same meaning and should be uppercased",
                }
            if self.add_adf_framework_template_flag:
                self.inconsistent_names["MERGE_INTO_ADF_FRM"] = f"""SELECT
                                        <DL>8<NUM> + 100000*(SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = <DS>) as DQ_RULE_ID,
                                        (SELECT DATASOURCE_ID FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = <DS>) AS DATASOURCE_ID,
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
                                        'select iff(count(upper({self.related_column})) <> COUNT({self.related_column}), 1, 0) from
                                                (SELECT DATASOURCE_RELATED_SCHEMA FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = <DS>)
                                                .{self.related_table}' AS PARAM_DEFAULT,
                                        TRUE IS_ACTIVE"""
        return self

    def identify_dates_in_future(self, **kwargs):
        def cast_to_datetime(str_date: str) -> datetime:
            return dateutil.parser.parse(str_date).replace(tzinfo=UTC)

        if not self.base_info.get("max") \
           and not self.base_info.get("min"):
            return self

        if cast_to_datetime(self.base_info.get("max")) > datetime.now().replace(tzinfo=UTC) \
           or cast_to_datetime(self.base_info.get("min")) > datetime.now().replace(tzinfo=UTC):
            self.future_dates = {
                "DESCRIPTION": "FUTURE DATES: Maybe this column should not contain dates from the future",
            }
            if self.add_adf_framework_template_flag:
                self.future_dates["MERGE_INTO_ADF_FRM"] = f"""SELECT
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
                                    'select iff(count({self.related_column}) > 0), 1, 0) from
                                            (SELECT DATASOURCE_RELATED_SCHEMA FROM UKI_STG_MTD.DATASOURCES WHERE DATASOURCE_DESC = <DS>)
                                    .{self.related_table} WHERE {self.related_column} > getdate()' AS PARAM_DEFAULT,
                                    TRUE IS_ACTIVE"""
        return self

    def identify_foreign_key(self, **kwargs):
        if not self.base_info.get("count") \
           or not self.base_info.get("top_freq") \
           or not self.base_info.get("top_share"):
            return self

        if round(self.base_info.get("top_freq")
           / (self.base_info.get("count") if self.base_info.get("count") != 0 else 1), 3) \
           == round(self.base_info.get("top_share"), 3):
            self.foreign_key = {
                "DESCRIPTION": "POSSIBLE FOREIGN KEY: Maybe this column is a foreign key and it is worth to check for CONSISTENCY",
            }
        return self


class ConstraintIdentifierBuilder:
    def __init__(self,
                 base_info: dict,
                 related_column: str,
                 related_table: str,
                 constraint_identification_rules: list[dict],
                 add_adf_framework_template: bool = False):
        self.identifier = ConstraintIdentifier(base_info=base_info,
                                               related_column=related_column,
                                               related_table=related_table,
                                               add_adf_framework_template=add_adf_framework_template)
        self.config_map = {
            "NULLABILITY": ConstraintIdentifier.identify_nullability,
            "MINMAX": ConstraintIdentifier.identify_min_max_range,
            "DETERMINED_LIST": ConstraintIdentifier.identify_determined_list,
            "INCONSISTENT_NAMES": ConstraintIdentifier.identify_inconsistent_names,
            "FUTURE_DATES": ConstraintIdentifier.identify_dates_in_future,
            "FOREIGN_KEYS": ConstraintIdentifier.identify_foreign_key,
        }
        self.identification_rules = constraint_identification_rules

    def build_identifier(self) -> ConstraintIdentifier:
        constraints = [self.config_map[constraint["rule"]] for constraint in self.identification_rules]
        constraint_properties = {prop: value for rule in self.identification_rules
                                 for prop, value in rule["properties"].items()}
        self.identifier = reduce(
            lambda res, func: func(res, **constraint_properties),
            constraints,
            self.identifier)
        return self.identifier
