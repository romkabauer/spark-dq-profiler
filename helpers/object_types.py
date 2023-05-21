from enum import Enum


class TableType(Enum):
    SNF = "SNF"
    CSV = "CSV"

    @staticmethod
    def list_possible_types():
        return [TableType.SNF.value, TableType.CSV.value]


class ColumnType(Enum):
    NUMERIC = "NUMERIC"
    TEXT = "TEXT"
    TIMESTAMP = "TIMESTAMP"

    @staticmethod
    def list_possible_types():
        return [ColumnType.NUMERIC.value, ColumnType.TIMESTAMP.value, ColumnType.TEXT.value]
