from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union
import logging
import pyarrow as pa
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, Result
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from infra.database.ConnectionFactory import (
    create_engine_sa,
    create_impala_connection,
    database_dialect,
)


logger = logging.getLogger("database")


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



def _database_connection_hint(exc: Exception) -> str:
    msg = str(exc)
    msg_lower = msg.lower()

    if "im002" in msg_lower:
        return (
            "Driver ODBC não encontrado. Verifique se o driver informado em "
            "DB_ODBC_DRIVER está instalado na máquina e se o nome está exatamente igual."
        )

    if "08001" in msg or "server was not found" in msg_lower or "servidor não foi encontrado" in msg_lower:
        return (
            "Erro 08001: o SQL Server não foi encontrado ou não está acessível. "
            "Verifique DB_HOST, instância nomeada, DB_PORT, firewall, VPN/rede, "
            "DNS e se o SQL Server permite conexões remotas."
        )

    if "28000" in msg or "login failed" in msg_lower or "falha de logon" in msg_lower:
        return (
            "Erro de autenticação: a conexão chegou ao SQL Server, mas o login foi recusado. "
            "Verifique usuário/senha ou, em autenticação Windows, se o usuário logado "
            "tem permissão no banco."
        )

    if "hyt00" in msg_lower or "timeout" in msg_lower or "tempo limite" in msg_lower:
        return (
            "Timeout de conexão. Verifique rede, porta, nome da instância, firewall e "
            "conectividade com o servidor SQL Server."
        )

    if "trusted_connection" in msg_lower or "sspi" in msg_lower:
        return (
            "Falha relacionada à autenticação integrada do Windows. Verifique se "
            "DB_TRUSTED_CONNECTION=true está adequado ao ambiente e se o processo está "
            "rodando com o usuário de rede autorizado."
        )

    return (
        "Falha ao conectar ao banco. Verifique DB_DIALECT, DB_DRIVER, DB_HOST, "
        "DB_PORT, DB_NAME, DB_ODBC_DRIVER, DB_TRUSTED_CONNECTION e parâmetros de "
        "rede/autenticação."
    )

def connect(**engine_kwargs: Any) -> DbContext:
    """
    Cria Engine + Connection com stream_results=True.

    A responsabilidade de abrir/fechar conexão fica centralizada aqui,
    em vez de espalhada nos services.
    """
    engine = create_engine_sa(**engine_kwargs)

    try:
        logger.info("Abrindo conexão com o banco de dados")
        conn = engine.connect().execution_options(stream_results=True)
        logger.info("Conexão com o banco de dados aberta com sucesso")

        return DbContext(engine, conn)

    except OperationalError as exc:
        engine.dispose()
        hint = _database_connection_hint(exc)
        logger.exception(
            "Falha operacional ao conectar ao banco de dados | diagnóstico=%s",
            hint,
        )
        raise RuntimeError(hint) from exc

    except SQLAlchemyError as exc:
        engine.dispose()
        hint = _database_connection_hint(exc)
        logger.exception(
            "Falha SQLAlchemy ao conectar ao banco de dados | diagnóstico=%s",
            hint,
        )
        raise RuntimeError(hint) from exc

    except Exception as exc:
        engine.dispose()
        hint = _database_connection_hint(exc)
        logger.exception(
            "Falha inesperada ao conectar ao banco de dados | diagnóstico=%s",
            hint,
        )
        raise RuntimeError(hint) from exc


def load_sql(sql_path: Union[str, Path], encoding: str = "utf-8") -> str:
    return Path(sql_path).read_text(encoding=encoding)


def _param_ano(params: Params) -> Optional[int]:
    if params is None:
        return None

    if isinstance(params, Mapping):
        if "ano" not in params:
            return None

        return int(params["ano"])

    if (
        isinstance(params, Sequence)
        and not isinstance(params, (str, bytes))
        and len(params) >= 1
    ):
        return int(params[0])

    return None


def render_sql_template(sql: str, params: Params) -> tuple[str, bool]:
    """
    Renderização controlada de parâmetros do DataHub.

    Padrão novo recomendado nos arquivos SQL:

        where ano = {{ano}}

    Quando o SQL contém {{ano}}, o valor é substituído por inteiro e a query
    é executada sem parâmetros do driver.

    Isso evita diferença entre drivers:
    - SQL Server/pyodbc costuma usar ?
    - PostgreSQL/psycopg costuma usar %s
    - Impala pode variar conforme driver/configuração

    Compatibilidade:
    - Se o SQL não contém {{ano}}, nada é alterado.
    - Nesse caso, o código continua passando params ao driver como antes.
    """
    if "{{ano}}" not in sql:
        return sql, False

    ano = _param_ano(params)

    if ano is None:
        raise ValueError(
            "SQL contém '{{ano}}', mas o parâmetro ano não foi informado."
        )

    return sql.replace("{{ano}}", str(ano)), True


def execute_sql(conn: Connection, sql: str, params: Params) -> Result:
    """Executa SQL preservando o estilo de parâmetros do script original."""
    sql, rendered = render_sql_template(sql, params)

    if rendered or params is None:
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
                coerced = [
                    str(v) if hasattr(v, "as_tuple") else v
                    for v in column_values
                ]
                arrays.append(pa.array(coerced))
            else:
                arrays.append(pa.array(column_values))

        return pa.RecordBatch.from_arrays(arrays, list(columns))

    # reordena colunas conforme schema.names
    if list(columns) != schema.names:
        index_by_name = {name: idx for idx, name in enumerate(columns)}

        transposed_columns = [
            transposed_columns[index_by_name[name]]
            if name in index_by_name
            else tuple([None] * len(rows))
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


def iter_impala_record_batches_from_file(
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
    sql, rendered = render_sql_template(sql, params)

    conn = create_impala_connection()
    cursor = None

    try:
        cursor = conn.cursor()

        if hasattr(cursor, "arraysize"):
            try:
                cursor.arraysize = int(driver_arraysize)
            except Exception:
                pass

        if rendered or params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)

        columns = [str(col[0]) for col in cursor.description]
        batch_index = 0

        while True:
            rows = cursor.fetchmany(chunk_rows)

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

    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            conn.close()


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

    Quando DB_DIALECT=impala, usa conexão direta via impyla.
    Para os demais bancos, mantém o caminho atual via SQLAlchemy.

    Compatibilidade de parâmetros:
    - SQL com {{ano}} é renderizado internamente e executado sem params.
    - SQL sem {{ano}} continua usando params do driver:
        SQL Server/pyodbc: ?
        PostgreSQL/psycopg: %s
    """
    if database_dialect() == "impala":
        return iter_impala_record_batches_from_file(
            sql_path,
            params=params,
            chunk_rows=chunk_rows,
            driver_arraysize=driver_arraysize,
            schema=schema,
            coerce_decimal_to_str=coerce_decimal_to_str,
            encoding=encoding,
        )

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