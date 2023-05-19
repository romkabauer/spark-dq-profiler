from enum import Enum


class TableType(Enum):
    SNF = "SNF"
    CSV = "CSV"

    @staticmethod
    def list_possible_types():
        return [TableType.SNF.value, TableType.CSV.value]
