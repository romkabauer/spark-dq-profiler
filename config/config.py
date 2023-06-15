from helpers.object_types import ColumnType

FLAG_PRINT_PROFILING_STAT = False
FLAG_SUGGEST_MERGE_STATEMENT_FOR_ADF_FRAMEWORK = True

CSV_SEPARATOR = ','

# Logic of constraint identification rules can be found at utils.constraint_identifier.ConstraintIdentifier
# Available identification rules:
# ## "NULLABILITY" with property "nullability_threshold". Property is 0.99 by default.
# ## "MINMAX"
# ## "DETERMINED_LIST" with property "list_size_threshold". Property is 10 by default.
# ## "INCONSISTENT_NAMES"
# ## "FUTURE_DATES"
# ## "FOREIGN_KEYS"

CONSTRAINT_IDENTIFICATION_RULES = {
    ColumnType.NUMERIC.value: [
        {"rule": "NULLABILITY", "properties": {"nullability_threshold": 0.99}},
        {"rule": "MINMAX", "properties": {}},
        {"rule": "INCONSISTENT_NAMES", "properties": {}},
        {"rule": "DETERMINED_LIST", "properties": {"list_size_threshold": 2}},
        {"rule": "FOREIGN_KEYS", "properties": {}},
    ],
    ColumnType.TIMESTAMP.value: [
        {"rule": "NULLABILITY", "properties": {"nullability_threshold": 0.99}},
        {"rule": "FUTURE_DATES", "properties": {}},
    ],
    ColumnType.TEXT.value: [
        {"rule": "NULLABILITY", "properties": {"nullability_threshold": 0.99}},
        {"rule": "INCONSISTENT_NAMES", "properties": {}},
        {"rule": "DETERMINED_LIST", "properties": {"list_size_threshold": 7}},
        {"rule": "FOREIGN_KEYS", "properties": {}},
    ]
}

debug_table = {
    "datasource_type": "SNF",
    "schema": "UKI_DWH_SNU",
    "name": "SN_PROD_COMMERCIAL_1_DIM",
    "columns": ["MDS_CODE", "COMMERCIAL_1_NAME", "DWH_CREATEDT"]
}
debug_table_2 = {
    "datasource_type": "SNF",
    "schema": "UKI_DTM_SNU",
    "name": "DIM_CUSTOMER",
}
debug_table_3 = {
    "datasource_type": "SNF",
    "schema": "UKI_DTM_SNUU",
    "name": "DIM_CUSTOMER",
}
debug_table_4 = {
    "datasource_type": "SNF",
    "1": "",
    "2": "",
}
csv_debug_table_1 = {
    "path": "data/IQVIA_OLP_SALES_TRAN.csv",
    "name": "IQVIA_OLP_SALES_TRAN",
}
csv_debug_table_2 = {
    "path": "data/EMPTY_TABLE.csv",
    "name": "EMPTY_TABLE",
}
SN_PPA_PRESCRIBING_TRAN = {
    "datasource_type": "SNF",
    "schema": "UKI_DWH_SNU",
    "name": "SN_PPA_PRESCRIBING_TRAN",
}
SN_PPA_PRACTICE_CUST_DIM = {
    "datasource_type": "SNF",
    "schema": "UKI_DWH_SNU",
    "name": "SN_PPA_PRACTICE_CUST_DIM",
}
DIM_PRODUCT_PPA = {
    "datasource_type": "SNF",
    "schema": "UKI_DTM_SNU",
    "name": "DIM_PRODUCT_PPA",
    "columns": ["PPA_PART_CODE", "PPA_PART_DESCRIPTION"],
}
FACT_PPA_PRESCRIBING = {
    "datasource_type": "SNF",
    "schema": "UKI_DTM_SNU",
    "name": "FACT_PPA_PRESCRIBING",
    "columns": ["MONTH_ID", "CUSTOMER_ID", "PPA_PRODUCT_ID", "GEOGRAPHY_ID", "ITEMS", "NIC", "NIC_EUR", "ACTUAL_COST", "QUANTITY", "QUANTITY_CALCULATED", "SRC_COUNTRY"]
}

TO_PROFILE = [
    # debug_table,      # testing custom columns
    # debug_table_2,  # resource intensive example, testing wide range of columns, performance and full columns pickup
    # debug_table_3,    # testing wrong reference to SNF
    # debug_table_4,  # testing wrong config
    csv_debug_table_1,    # testing SparkProfiler - non-empty csv table
    csv_debug_table_2,    # testing SparkProfiler - empty csv table
    # SN_PPA_PRESCRIBING_TRAN,
    # SN_PPA_PRACTICE_CUST_DIM,
    DIM_PRODUCT_PPA,
    # FACT_PPA_PRESCRIBING,
]
