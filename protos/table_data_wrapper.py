from .table_data_pb2 import TableData


class TableDataWrapper:
    def __init__(self, table_data: TableData):
        self.table_data = table_data
        self.schema = table_data.schema
        self.column_index = {name: idx for idx, name in enumerate(self.schema.columns)}

    def get(self, row_index: int, column: str) -> str:
        """Devuelve el valor de una celda (por fila y nombre de columna)."""
        if row_index < 0 or row_index >= len(self.table_data.rows):
            raise IndexError(f"Fila {row_index} fuera de rango")

        if column not in self.column_index:
            raise KeyError(f"Columna '{column}' no encontrada")

        idx = self.column_index[column]
        row = self.table_data.rows[row_index]

        if idx >= len(row.values):
            raise IndexError(f"Fila {row_index} no tiene valor para columna '{column}'")

        return row.values[idx]

    def as_dict(self, row_index: int) -> dict:
        """Devuelve una fila completa como diccionario columna: valor."""
        if row_index < 0 or row_index >= len(self.table_data.rows):
            raise IndexError(f"Fila {row_index} fuera de rango")

        row = self.table_data.rows[row_index]
        return {
            col: row.values[i] if i < len(row.values) else None
            for i, col in enumerate(self.schema.columns)
        }

    def column_names(self) -> list[str]:
        """Devuelve los nombres de columnas en orden."""
        return list(self.schema.columns)

    def num_rows(self) -> int:
        """Cantidad total de filas."""
        return len(self.table_data.rows)
