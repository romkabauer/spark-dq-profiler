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
debug_table_5 = {
    "datasource_type": "CSV",
    "1": "",
    "2": "",
}
TO_PROFILE = [
    debug_table,      # testing custom columns
    # debug_table_2,  # resource intensive example, testing wide range of columns, performance and full columns pickup
    debug_table_3,    # testing wrong reference to SNF
    # debug_table_4,  # testing wrong config
    debug_table_5,    # testing CSVProfiler
]
