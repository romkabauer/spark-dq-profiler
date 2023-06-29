from helpers.object_types import ColumnType

FLAG_PRINT_PROFILING_STAT = False
WRITE_TO_FILE = True
PROFILING_OUTPUT_FILE_PATH = 'profiling_results.txt'
ANALYSIS_OUTPUT_FILE_PATH = 'constraints_suggestions.txt'
FLAG_SUGGEST_MERGE_STATEMENT_FOR_ADF_FRAMEWORK = False

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
    "datasource_type": "CSV",
    "path": "data/IQVIA_OLP_SALES_TRAN.csv",
    "name": "IQVIA_OLP_SALES_TRAN",
}
csv_debug_table_2 = {
    "datasource_type": "CSV",
    "path": "data/EMPTY_TABLE.csv",
    "name": "EMPTY_TABLE",
}

TO_PROFILE = [
    debug_table,      # testing custom columns
    # debug_table_2,  # resource intensive example, testing wide range of columns, performance and full columns pickup
    # debug_table_3,    # testing wrong reference to SNF
    # debug_table_4,  # testing wrong config
    csv_debug_table_1,    # testing CSVProfiler - non-empty table
    csv_debug_table_2,    # testing CSVProfiler - empty table
]
