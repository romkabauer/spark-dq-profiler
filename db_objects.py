from executor import SnowflakeExecutor


class Table:
    def __init__(self, schema: str, name: str, columns: list[str] = None):
        self.schema = schema
        self.name = name
        self.columns = columns

    async def get_count(self, executor: SnowflakeExecutor) -> dict | None:
        sql = f"""SELECT
                    COUNT(*) as cnt
                FROM {self.schema}.{self.name}"""
        df = await executor.execute_select(sql)
        if df.get("cnt") is None:
            return {
                "ERROR": str(df[0][0]),
                "STATEMENT_WAS_TIRED_TO_EXECUTE": str(df[0][1]),
            }
        return {
            "TABLE_COUNT": int(df["cnt"][0]),
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
        return df["table_columns"][0].split(",")


class TableColumn(Table):
    def __init__(self, schema: str, table_name: str, column_name: str):
        super().__init__(schema, table_name)
        self.related_schema = self.schema
        self.related_table = self.name
        self.column_name = column_name

    async def get_count(self, executor: SnowflakeExecutor) -> dict:
        sql = f"""SELECT
                    COUNT({self.column_name}) as cnt,
                    COUNT({self.column_name})/COUNT(*) as share
                FROM {self.related_schema}.{self.related_table}"""
        df = await executor.execute_select(sql)
        return {
            "count": int(df["cnt"][0]),
            "share": float(df["share"][0]),
        }

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
                            {self.__build_script_for_numeric_column_stat_collection()}
                        );
                        RETURN table(res);
                    ELSEIF (col_type = 'TIMESTAMP_TZ') THEN
                        res := (
                            {self.__build_script_for_datetime_column_stat_collection()}
                        );
                        RETURN table(res);
                    ELSE
                        res := (
                            {self.__build_script_for_text_column_stat_collection()}
                        );
                        RETURN table(res);
                    END IF;
                    END;
                    $$;"""
        df = await executor.execute_select(sql)
        match df["col_type"][0]:
            case "NUMERIC":
                return self.__convert_df_with_numeric_stat_to_dict(df)
            case "TIMESTAMP":
                return self.__convert_df_with_datetime_stat_to_dict(df)
            case _:
                return self.__convert_df_with_text_stat_to_dict(df)

    def __build_script_for_numeric_column_stat_collection(self) -> str:
        return f"""SELECT
                    *
                FROM (
                    SELECT
                        'NUMERIC' AS col_type,
                        AVG({self.column_name}) as mean,
                        MIN({self.column_name}) as min,
                        MAX({self.column_name}) as max,
                        PERCENTILE_CONT(0.25) WITHIN GROUP
                            (ORDER BY {self.column_name}) as perc25,
                        MEDIAN({self.column_name}) as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP
                            (ORDER BY {self.column_name}) as perc75
                    FROM {self.related_schema}.{self.related_table}
                )
                JOIN (
                    SELECT
                        COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                        COALESCE({self.column_name}::varchar, 'NULL') as top_value,
                        COUNT(*) as top_freq,
                        COUNT(*) / (SELECT COUNT(*) FROM {self.related_schema}.{self.related_table}) as top_share
                    FROM {self.related_schema}.{self.related_table}
                    GROUP BY {self.column_name}
                    ORDER BY 3 DESC
                    LIMIT 1
                ) s ON 1=1"""

    def __build_script_for_datetime_column_stat_collection(self) -> str:
        return f"""SELECT
                    *
                FROM (
                    SELECT
                        'TIMESTAMP' AS col_type,
                        AVG(DATE_PART(EPOCH, {self.column_name}))::timestamp as mean,
                        MIN(DATE_PART(EPOCH, {self.column_name}))::timestamp as min,
                        MAX(DATE_PART(EPOCH, {self.column_name}))::timestamp as max,
                        (PERCENTILE_CONT(0.25) WITHIN GROUP
                            (ORDER BY DATE_PART(EPOCH, {self.column_name})))::timestamp as perc25,
                        MEDIAN(DATE_PART(EPOCH, {self.column_name}))::timestamp as median,
                        (PERCENTILE_CONT(0.75) WITHIN GROUP
                            (ORDER BY DATE_PART(EPOCH, {self.column_name})))::timestamp as perc75
                    FROM {self.related_schema}.{self.related_table}
                )
                JOIN (
                    SELECT
                        COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                        COALESCE({self.column_name}::varchar, 'NULL') as top_value,
                        COUNT(*) as top_freq,
                        COUNT(*) / (SELECT COUNT(*) FROM {self.related_schema}.{self.related_table}) as top_share
                    FROM {self.related_schema}.{self.related_table}
                    GROUP BY {self.column_name}
                    ORDER BY 3 DESC
                    LIMIT 1
                ) s ON 1=1"""

    def __build_script_for_text_column_stat_collection(self) -> str:
        return f"""SELECT
                    'TEXT' as col_type,
                    COUNT(DISTINCT {self.column_name}) OVER() as uniq,
                    COUNT(DISTINCT UPPER({self.column_name})) OVER() as uniq_upper,
                    COALESCE({self.column_name}, 'NULL') as top_value,
                    COUNT(*) as top_freq,
                    COUNT(*) / (SELECT COUNT(*) FROM {self.related_schema}.{self.related_table}) as top_share
                FROM
                    {self.related_schema}.{self.related_table}
                GROUP BY {self.column_name}
                ORDER BY 5 DESC
                LIMIT 1"""

    @staticmethod
    def __convert_df_with_numeric_stat_to_dict(df) -> dict:
        return {
            "col_type": str(df["col_type"][0]),
            "uniq": int(df["uniq"][0]),
            "top_value": str(df["top_value"][0]),
            "top_freq": int(df["top_freq"][0]),
            "top_share": float(df["top_share"][0]),
            "mean": float(df["mean"][0]),
            "min": float(df["min"][0]),
            "perc25": float(df["perc25"][0]),
            "median": float(df["median"][0]),
            "perc75": float(df["perc75"][0]),
            "max": float(df["max"][0]),
        }

    @staticmethod
    def __convert_df_with_datetime_stat_to_dict(df) -> dict:
        return {
            "col_type": str(df["col_type"][0]),
            "uniq": int(df["uniq"][0]),
            "top_value": str(df["top_value"][0]),
            "top_freq": int(df["top_freq"][0]),
            "top_share": float(df["top_share"][0]),
            "mean": str(df["mean"][0]),
            "min": str(df["min"][0]),
            "perc25": str(df["perc25"][0]),
            "median": str(df["median"][0]),
            "perc75": str(df["perc75"][0]),
            "max": str(df["max"][0]),
        }

    @staticmethod
    def __convert_df_with_text_stat_to_dict(df) -> dict:
        return {
            "col_type": str(df["col_type"][0]),
            "uniq": int(df["uniq"][0]),
            "uniq_upper": int(df["uniq_upper"][0]),
            "top_value": str(df["top_value"][0]),
            "top_freq": int(df["top_freq"][0]),
            "top_share": float(df["top_share"][0]),
        }
