from pyspark.sql import DataFrame

from utils.executors import Executor, SnowflakeExecutor, SparkExecutor
from helpers.object_types import ColumnType


class Table:
    def __init__(self, schema: str, name: str):
        self.schema = schema
        self.name = name


class TableColumn(Table):
    def __init__(self, schema: str, table_name: str, column_name: str, **kwargs):
        super().__init__(schema, table_name)
        self.related_schema = self.schema
        self.related_table = self.name
        self.column_name = column_name
        self.col_type = kwargs.get("col_type")
        self.df_table = kwargs.get("df_table")
        self.configured_table_name = kwargs.get("configured_table_name")

    async def get_count(self, executor: Executor) -> dict:
        schema = "" if not self.related_schema else self.related_schema + "."
        sql = f"""SELECT
                    COUNT({self.column_name}) as cnt,
                    COUNT({self.column_name})/COUNT(*) as share
                FROM {schema}{self.related_table}"""
        df = await executor.execute_select(sql, df_table=self.df_table)
        if "cnt" not in df.columns:
            return {
                "ERROR": str(df.head()[0]),
                "STATEMENT_WAS_TIRED_TO_EXECUTE": str(df.head()[1]),
            }
        return {
            "count": int(df.head()["cnt"]),
            "share": float(df.head()["share"]),
        }

    async def calc_column_stat(self, executor: SparkExecutor) -> dict:
        sql = self.build_script_for_column_stat_collection(col_type=self.col_type)

        df = await executor.execute_select(sql, df_table=self.df_table)
        if "ERROR" in df.columns:
            return {
                "ERROR": df.head()["ERROR"],
                "col_type": self.col_type,
                "uniq": 0,
                "uniq_upper": 0,
                "top_value": "NULL",
                "top_freq": 0,
                "top_share": 0,
            }
        return self.convert_df_stat_to_dict(df, col_type=self.col_type)

    def build_script_for_column_stat_collection(self, col_type: str = ColumnType.TEXT.value):
        match col_type:
            case ColumnType.NUMERIC.value:
                return self.build_script_for_numeric_column_stat_collection()
            case ColumnType.TIMESTAMP.value:
                return self.build_script_for_datetime_column_stat_collection()
            case ColumnType.TEXT.value:
                return self.build_script_for_text_column_stat_collection()
            case _:
                return "SELECT 'Unknown data type' as ERROR"

    def build_script_for_numeric_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""with quarters_base as (
                    SELECT 
                        {self.column_name},
                        NTILE(4) OVER (ORDER BY {self.column_name}) AS quarter
                    FROM {schema}{self.related_table}
                )
                SELECT
                    *
                FROM (
                    SELECT
                        '{ColumnType.NUMERIC.value}' AS col_type,
                        AVG({self.column_name}) as mean,
                        MIN({self.column_name}) as min,
                        MAX({self.column_name}) as max,
                        perc25,
                        median,
                        perc75
                    FROM {schema}{self.related_table}
                    JOIN (select max({self.column_name}) as perc25 from quarters_base where quarter = 1) ON 1=1
                    JOIN (select max({self.column_name}) as median from quarters_base where quarter = 2) ON 1=1
                    JOIN (select max({self.column_name}) as perc75 from quarters_base where quarter = 3) ON 1=1
                )
                JOIN (
                    SELECT
                        COUNT(DISTINCT {self.column_name}) as uniq,
                        COALESCE(cast({self.column_name} as varchar), 'NULL') as top_value,
                        COUNT(*) as top_freq,
                        COUNT(*) / (SELECT COUNT(*) FROM {self.related_table}) as top_share
                    FROM {schema}{self.related_table}
                    GROUP BY {self.column_name}
                    ORDER BY 3 DESC
                    LIMIT 1
                ) s ON 1=1"""

    def build_script_for_datetime_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""with quarters_base as (
            SELECT 
                {self.column_name},
                NTILE(4) OVER (ORDER BY {self.column_name}) AS quarter
            FROM {schema}{self.related_table}
        )
        SELECT
            *
        FROM (
            SELECT
                '{ColumnType.TIMESTAMP.value}' AS col_type,
                median as mean,
                MIN({self.column_name}) as min,
                MAX({self.column_name}) as max,
                perc25,
                median,
                perc75
            FROM {schema}{self.related_table}
            JOIN (select max({self.column_name}) as perc25 from quarters_base where quarter = 1) ON 1=1
            JOIN (select max({self.column_name}) as median from quarters_base where quarter = 2) ON 1=1
            JOIN (select max({self.column_name}) as perc75 from quarters_base where quarter = 3) ON 1=1
        )
        JOIN (
            SELECT
                COUNT(DISTINCT {self.column_name}) as uniq,
                COALESCE(cast({self.column_name} as varchar), 'NULL') as top_value,
                COUNT(*) as top_freq,
                COUNT(*) / (SELECT COUNT(*) FROM {self.related_table}) as top_share
            FROM {schema}{self.related_table}
            GROUP BY {self.column_name}
            ORDER BY 3 DESC
            LIMIT 1
        ) s ON 1=1"""

    def build_script_for_text_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""SELECT
                    '{ColumnType.TEXT.value}' as col_type,
                    COUNT(DISTINCT {self.column_name}) as uniq,
                    COUNT(DISTINCT UPPER({self.column_name})) as uniq_upper,
                    COALESCE({self.column_name}, 'NULL') as top_value,
                    COUNT(*) as top_freq,
                    COUNT(*) / (SELECT COUNT(*) FROM {schema}{self.related_table}) as top_share
                FROM
                    {schema}{self.related_table}
                GROUP BY {self.column_name}
                ORDER BY 5 DESC
                LIMIT 1"""

    def convert_df_stat_to_dict(self, df: DataFrame, col_type: str = ColumnType.TEXT.value):
        match col_type:
            case ColumnType.NUMERIC.value:
                return self.convert_df_with_numeric_stat_to_dict(df)
            case ColumnType.TIMESTAMP.value:
                return self.convert_df_with_datetime_stat_to_dict(df)
            case _:
                return self.convert_df_with_text_stat_to_dict(df)

    @staticmethod
    def convert_df_with_numeric_stat_to_dict(df: DataFrame) -> dict:
        return {
            "col_type": str(df.head()["col_type"]),
            "uniq": int(df.head()["uniq"]),
            "top_value": str(df.head()["top_value"]),
            "top_freq": int(df.head()["top_freq"]),
            "top_share": float(df.head()["top_share"] if df.head()["top_share"] else 0),
            "mean": float(df.head()["mean"] if df.head()["mean"] else 0),
            "min": float(df.head()["min"] if df.head()["min"] else 0),
            "perc25": float(df.head()["perc25"] if df.head()["perc25"] else 0),
            "median": float(df.head()["median"] if df.head()["median"] else 0),
            "perc75": float(df.head()["perc75"] if df.head()["perc75"] else 0),
            "max": float(df.head()["max"] if df.head()["max"] else 0),
        }

    @staticmethod
    def convert_df_with_datetime_stat_to_dict(df: DataFrame) -> dict:
        return {
            "col_type": str(df.head()["col_type"]),
            "uniq": int(df.head()["uniq"]),
            "top_value": str(df.head()["top_value"]),
            "top_freq": int(df.head()["top_freq"]),
            "top_share": float(df.head()["top_share"]),
            "mean": str(df.head()["mean"]),
            "min": str(df.head()["min"]),
            "perc25": str(df.head()["perc25"]),
            "median": str(df.head()["median"]),
            "perc75": str(df.head()["perc75"]),
            "max": str(df.head()["max"]),
        }

    @staticmethod
    def convert_df_with_text_stat_to_dict(df: DataFrame) -> dict:
        return {
            "col_type": str(df.head()["col_type"]),
            "uniq": int(df.head()["uniq"]),
            "uniq_upper": int(df.head()["uniq_upper"]),
            "top_value": str(df.head()["top_value"]),
            "top_freq": int(df.head()["top_freq"]),
            "top_share": float(df.head()["top_share"]),
        }


class SNFTable(Table):
    def __init__(self, schema: str, name: str, columns: list[str] = None):
        super().__init__(schema, name)
        self.columns = columns

    async def get_count(self, executor: SnowflakeExecutor) -> dict | None:
        sql = f"""SELECT
                    COUNT(*) as cnt
                FROM {self.schema}.{self.name}"""
        df = await executor.execute_select(sql)
        if "cnt" not in df.columns:
            print(df.head())
            return {
                "ERROR": str(df.head()[0]),
                "STATEMENT_WAS_TIRED_TO_EXECUTE": str(df.head()[1]),
            }
        elif df.head()["cnt"] == 0:
            return {
                "TABLE_NAME": self.name,
                "ERROR": "EMPTY_TABLE",
            }
        return {
            "TABLE_COUNT": int(df.head()["cnt"]),
        }

    async def get_columns_list(self, executor: SnowflakeExecutor) -> list[str] | None:
        sql = f"""WITH tmp AS (
            SELECT
                CASE WHEN COLUMN_NAME IS NULL THEN '' ELSE COLUMN_NAME END AS COLUMN_NAME,
                ORDINAL_POSITION 
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = UPPER('{self.schema}') and TABLE_NAME in ('{self.name}')
        )
        SELECT
            array_to_string(array_agg(COLUMN_NAME) WITHIN GROUP (ORDER BY ORDINAL_POSITION), ',') as table_columns
        FROM tmp
        order by ORDINAL_POSITION"""
        df = await executor.execute_select(sql)
        return df.head()["table_columns"].split(",")


class SNFTableColumn(TableColumn):
    def __init__(self, schema: str, table_name: str, column_name: str):
        super().__init__(schema, table_name, column_name)

    async def calc_column_stat(self, executor: SnowflakeExecutor) -> dict:
        sql = f"""EXECUTE IMMEDIATE
                    $$
                    DECLARE
                        col_type VARCHAR DEFAULT 'VARCHAR';
                        res RESULTSET;
                    BEGIN
                    SELECT
                        typeof({self.column_name}::variant) into :col_type 
                    FROM (SELECT {self.column_name} FROM {self.related_schema}.{self.related_table} LIMIT 1);
                    
                    IF (col_type in ('DECIMAL', 'INTEGER', 'DOUBLE')) THEN
                        res := (
                            {self.build_script_for_column_stat_collection(col_type=ColumnType.NUMERIC.value)}
                        );
                        RETURN table(res);
                    ELSEIF (col_type = 'TIMESTAMP_TZ') THEN
                        res := (
                            {self.build_script_for_column_stat_collection(col_type=ColumnType.TIMESTAMP.value)}
                        );
                        RETURN table(res);
                    ELSE
                        res := (
                            {self.build_script_for_column_stat_collection()}
                        );
                        RETURN table(res);
                    END IF;
                    END;
                    $$;"""
        df = await executor.execute_select(sql)
        if "error" in df.columns:
            return {
                "ERROR": df.head()["error"],
                "col_type": self.col_type,
                "uniq": 0,
                "uniq_upper": 0,
                "top_value": "NULL",
                "top_freq": 0,
                "top_share": 0,
            }
        return self.convert_df_stat_to_dict(df, df.head()["col_type"])

    def build_script_for_numeric_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""SELECT
                    *
                FROM (
                    SELECT
                        '{ColumnType.NUMERIC.value}' AS col_type,
                        AVG({self.column_name}) as mean,
                        MIN({self.column_name}) as min,
                        MAX({self.column_name}) as max,
                        PERCENTILE_CONT(0.25) WITHIN GROUP
                            (ORDER BY {self.column_name}) as perc25,
                        MEDIAN({self.column_name}) as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP
                            (ORDER BY {self.column_name}) as perc75
                    FROM {schema}{self.related_table}
                )
                JOIN (
                    SELECT
                        COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                        COALESCE({self.column_name}::varchar, 'NULL') as top_value,
                        COUNT(*) as top_freq,
                        COUNT(*) / (SELECT COUNT(*) FROM {schema}{self.related_table}) as top_share
                    FROM {schema}{self.related_table}
                    GROUP BY {self.column_name}
                    ORDER BY 3 DESC
                    LIMIT 1
                ) s ON 1=1"""

    def build_script_for_datetime_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""SELECT
                    *
                FROM (
                    SELECT
                        '{ColumnType.TIMESTAMP.value}' AS col_type,
                        AVG(DATE_PART(EPOCH, {self.column_name}))::timestamp as mean,
                        MIN(DATE_PART(EPOCH, {self.column_name}))::timestamp as min,
                        MAX(DATE_PART(EPOCH, {self.column_name}))::timestamp as max,
                        (PERCENTILE_CONT(0.25) WITHIN GROUP
                            (ORDER BY DATE_PART(EPOCH, {self.column_name})))::timestamp as perc25,
                        MEDIAN(DATE_PART(EPOCH, {self.column_name}))::timestamp as median,
                        (PERCENTILE_CONT(0.75) WITHIN GROUP
                            (ORDER BY DATE_PART(EPOCH, {self.column_name})))::timestamp as perc75
                    FROM {schema}{self.related_table}
                )
                JOIN (
                    SELECT
                        COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                        COALESCE({self.column_name}::varchar, 'NULL') as top_value,
                        COUNT(*) as top_freq,
                        COUNT(*) / (SELECT COUNT(*) FROM {schema}{self.related_table}) as top_share
                    FROM {schema}{self.related_table}
                    GROUP BY {self.column_name}
                    ORDER BY 3 DESC
                    LIMIT 1
                ) s ON 1=1"""

    def build_script_for_text_column_stat_collection(self) -> str:
        schema = "" if not self.related_schema else self.related_schema + "."
        return f"""SELECT
                    '{ColumnType.TEXT.value}' as col_type,
                    COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                    COUNT(DISTINCT UPPER({self.column_name})) OVER() as uniq_upper,
                    COALESCE({self.column_name}::varchar, 'NULL') as top_value,
                    COUNT(*) as top_freq,
                    COUNT(*) / (SELECT COUNT(*) FROM {schema}{self.related_table}) as top_share
                FROM
                    {schema}{self.related_table}
                GROUP BY {self.column_name}
                ORDER BY 5 DESC
                LIMIT 1"""
