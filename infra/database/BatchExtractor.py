from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

import pyarrow as pa
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, Result

from infra.database.ConnectionFactory import create_engine_sa

Params = Union[Mapping[str, Any], Sequence[Any], None]


@dataclass(frozen=True)
class BatchMeta:
    """Metadados do lote extraído, úteis para log e métricas."""

    batch_index: int
    rows: int
    columns: int


class DbContext:
    """Contexto simples de Engine + Connection reutilizável para extração."""

    def __init__(self, engine: Engine, conn: Connection):
        self.engine = engine
        self.conn = conn

    def close(self) -> None:
        try:
            self.conn.close()
        finally:
            self.engine.dispose()

    def __enter__(self) -> "DbContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def connect(**engine_kwargs: Any) -> DbContext:
    """
    Cria Engine + Connection com stream_results=True.

    A responsabilidade de abrir/fechar conexão fica centralizada aqui,
    em vez de espalhada nos services.
    """
    engine = create_engine_sa(**engine_kwargs)
    conn = engine.connect().execution_options(stream_results=True)
    return DbContext(engine, conn)


def load_sql(sql_path: Union[str, Path], encoding: str = "utf-8") -> str:
    return Path(sql_path).read_text(encoding=encoding)


def execute_sql(conn: Connection, sql: str, params: Params) -> Result:
    """Executa SQL preservando o estilo de parâmetros do script original."""
    if params is None:
        return conn.exec_driver_sql(sql)

    if isinstance(params, Mapping):
        try:
            return conn.execute(text(sql), dict(params))
        except Exception:
            return conn.exec_driver_sql(sql, dict(params))

    return conn.exec_driver_sql(sql, tuple(params))


def set_driver_arraysize(result: Result, arraysize: int) -> None:
    cursor = getattr(result, "cursor", None)
    if cursor is not None and hasattr(cursor, "arraysize"):
        try:
            cursor.arraysize = int(arraysize)
        except Exception:
            pass


def rows_to_record_batch(
    columns: Sequence[str],
    rows: Sequence[Tuple[Any, ...]],
    *,
    schema: Optional[pa.Schema] = None,
    coerce_decimal_to_str: bool = False,
) -> pa.RecordBatch:
    if not rows:
        if schema is None:
            return pa.RecordBatch.from_arrays([], [])
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=f.type) for f in schema],
            schema=schema,
        )

    transposed_columns = list(zip(*rows))
    arrays: List[pa.Array] = []

    # sem schema: inferência livre
    if schema is None:
        for column_values in transposed_columns:
            if coerce_decimal_to_str:
                coerced = [str(v) if hasattr(v, "as_tuple") else v for v in column_values]
                arrays.append(pa.array(coerced))
            else:
                arrays.append(pa.array(column_values))
        return pa.RecordBatch.from_arrays(arrays, list(columns))

    # reordena colunas conforme schema.names
    if list(columns) != schema.names:
        index_by_name = {name: idx for idx, name in enumerate(columns)}
        transposed_columns = [
            transposed_columns[index_by_name[name]] if name in index_by_name else tuple([None] * len(rows))
            for name in schema.names
        ]

    for idx, field in enumerate(schema):
        column_values = transposed_columns[idx]

        # caso decimal opcional
        if coerce_decimal_to_str and pa.types.is_decimal(field.type):
            coerced = [str(v) if v is not None else None for v in column_values]
            arrays.append(pa.array(coerced, type=pa.string()))
            continue

        # IMPORTANTE:
        # tentar primeiro no tipo do contrato;
        # se falhar, cair para array sem tipo explícito, preservando os dados
        try:
            arrays.append(pa.array(column_values, type=field.type))
        except (pa.ArrowTypeError, pa.ArrowInvalid, TypeError, ValueError):
            arrays.append(pa.array(column_values))

    # não usar schema=schema aqui, porque algumas colunas podem ter caído
    # para tipo inferido. O enforcement final ficará no Engine/Polars.
    return pa.RecordBatch.from_arrays(arrays, names=schema.names)


def iter_record_batches(
    cx: DbContext,
    sql: str,
    *,
    params: Params = None,
    chunk_rows: int = 200_000,
    driver_arraysize: int = 10_000,
    schema: Optional[pa.Schema] = None,
    coerce_decimal_to_str: bool = False,
) -> Iterator[Tuple[BatchMeta, pa.RecordBatch]]:
    result = execute_sql(cx.conn, sql, params)
    set_driver_arraysize(result, driver_arraysize)

    columns = list(result.keys())
    batch_index = 0

    while True:
        rows = result.fetchmany(chunk_rows)
        if not rows:
            break

        record_batch = rows_to_record_batch(
            columns,
            rows,
            schema=schema,
            coerce_decimal_to_str=coerce_decimal_to_str,
        )
        meta = BatchMeta(
            batch_index=batch_index,
            rows=len(rows),
            columns=len(columns),
        )
        yield meta, record_batch
        batch_index += 1


def iter_record_batches_from_file(
    cx: DbContext,
    sql_path: Union[str, Path],
    *,
    params: Params = None,
    chunk_rows: int = 200_000,
    driver_arraysize: int = 10_000,
    schema: Optional[pa.Schema] = None,
    coerce_decimal_to_str: bool = False,
    encoding: str = "utf-8",
) -> Iterator[Tuple[BatchMeta, pa.RecordBatch]]:
    sql = load_sql(sql_path, encoding=encoding)
    return iter_record_batches(
        cx,
        sql,
        params=params,
        chunk_rows=chunk_rows,
        driver_arraysize=driver_arraysize,
        schema=schema,
        coerce_decimal_to_str=coerce_decimal_to_str,
    )


def extract_record_batches_from_file(
    sql_path: Union[str, Path],
    *,
    params: Params = None,
    chunk_rows: int = 200_000,
    driver_arraysize: int = 10_000,
    schema: Optional[pa.Schema] = None,
    coerce_decimal_to_str: bool = False,
    encoding: str = "utf-8",
    **engine_kwargs: Any,
) -> Iterator[Tuple[BatchMeta, pa.RecordBatch]]:
    """
    Função de mais alto nível para extração.

    Ela:
    - abre conexão
    - executa o SQL
    - faz streaming dos batches
    - fecha conexão/engine automaticamente ao final
    """

    def _generator() -> Iterator[Tuple[BatchMeta, pa.RecordBatch]]:
        with connect(**engine_kwargs) as cx:
            yield from iter_record_batches_from_file(
                cx,
                sql_path,
                params=params,
                chunk_rows=chunk_rows,
                driver_arraysize=driver_arraysize,
                schema=schema,
                coerce_decimal_to_str=coerce_decimal_to_str,
                encoding=encoding,
            )

    return _generator()


def record_batches_to_table(
    batches: Iterable[pa.RecordBatch],
    *,
    schema: Optional[pa.Schema] = None,
) -> pa.Table:
    batch_list = list(batches)
    if schema is None:
        return pa.Table.from_batches(batch_list)
    return pa.Table.from_batches(batch_list, schema=schema)