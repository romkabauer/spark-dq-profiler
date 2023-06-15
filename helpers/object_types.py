from enum import Enum


class TableType(Enum):
    SNF = "SNF"
    SPARK = "SPARK"

    @staticmethod
    def list_possible_types():
        return [TableType.SNF.value, TableType.SPARK.value]


class ColumnType(Enum):
    NUMERIC = "NUMERIC"
    TEXT = "TEXT"
    TIMESTAMP = "TIMESTAMP"

    @staticmethod
    def list_possible_types():
        return [ColumnType.NUMERIC.value, ColumnType.TIMESTAMP.value, ColumnType.TEXT.value]
