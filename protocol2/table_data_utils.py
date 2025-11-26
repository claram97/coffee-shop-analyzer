from typing import Iterator

from protocol2 import table_data_pb2


def build_table_data(
    table_name: table_data_pb2.TableName,
    columns: list[str],
    rows: list[list[str]],
    batch_number: int = 0,
    status: table_data_pb2.TableStatus = table_data_pb2.TableStatus.CONTINUE,
) -> table_data_pb2.TableData:
    schema = table_data_pb2.TableSchema(columns=columns)

    row_objs = []
    for row in rows:
        row_obj = table_data_pb2.Row(values=row)
        row_objs.append(row_obj)

    return table_data_pb2.TableData(
        name=table_name,
        schema=schema,
        rows=row_objs,
        batch_number=batch_number,
        status=status,
    )


def iterate_rows_as_dicts(
    table_data: table_data_pb2.TableData,
) -> Iterator[dict[str, str]]:
    columns = table_data.schema.columns
    for row in table_data.rows:
        yield {
            col: row.values[i] if i < len(row.values) else None
            for i, col in enumerate(columns)
        }
