from __future__ import annotations
import logging
from typing import Iterable, Iterator, Any
import pyarrow as pa
import polars as pl

logger = logging.getLogger("engine")

def _expected_column_names(expected_columns: list[str] | None) -> list[str] | None:
    if expected_columns is None:
        return None

    names = [str(x).strip() for x in expected_columns if str(x).strip()]
    return names or None

def _duplicate_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    dupes: list[str] = []

    for name in names:
        seen[name] = seen.get(name, 0) + 1
        if seen[name] == 2:
            dupes.append(name)

    return dupes

def _format_numbered_columns(title: str, names: list[str]) -> str:
    lines = [title]
    for i, name in enumerate(names, start=1):
        lines.append(f"  {i:02d}. {name}")
    return "\n".join(lines)

def _build_column_count_mismatch_message(
    actual_names: list[str],
    expected_names: list[str],
) -> str:
    return (
        "Quantidade de colunas retornadas pelo SQL difere do contrato. "
        f"sql_cols={len(actual_names)} | contrato_cols={len(expected_names)}\n\n"
        f"{_format_numbered_columns('Colunas retornadas pelo SQL:', actual_names)}\n\n"
        f"{_format_numbered_columns('Colunas esperadas pelo contrato:', expected_names)}"
    )


def _log_sql_name_differences(sql_names: list[str], expected_names: list[str]) -> None:
    changed: list[tuple[int, str, str]] = []

    for i, (sql_name, expected_name) in enumerate(zip(sql_names, expected_names), start=1):
        if sql_name != expected_name:
            changed.append((i, sql_name, expected_name))

    if not changed:
        return

    logger.warning(
        "  • O SQL retornou nomes de colunas diferentes do contrato; "
        "os nomes do contrato serão aplicados pela ordem | total_alteracoes=%s",
        len(changed),
    )

    for pos, sql_name, expected_name in changed:
        logger.warning(
            "    - COLUNA %s | sql=%s | contrato=%s | acao=renomear_para_contrato",
            pos,
            sql_name,
            expected_name,
        )


def _validate_and_resolve_names(
    actual_names: list[str],
    expected_names: list[str] | None,
) -> list[str]:
    if expected_names is None:
        dupes = _duplicate_names(actual_names)
        if dupes:
            dupes_txt = ", ".join(dupes)
            raise ValueError(
                "O lote retornado pelo SQL possui colunas duplicadas e não há "
                "lista de nomes esperados do contrato para renomeação por posição. "
                f"Colunas duplicadas: {dupes_txt}"
            )
        return actual_names

    actual_count = len(actual_names)
    expected_count = len(expected_names)

    if actual_count != expected_count:
        raise ValueError(
            _build_column_count_mismatch_message(
                actual_names=actual_names,
                expected_names=expected_names,
            )
        )

    dupes = _duplicate_names(actual_names)
    if dupes:
        dupes_txt = ", ".join(dupes)
        logger.warning(
            "  • O SQL retornou colunas com nomes duplicados; isso não impedirá o processamento "
            "porque os nomes do contrato serão aplicados pela ordem | colunas_duplicadas=%s",
            dupes_txt,
        )

    _log_sql_name_differences(actual_names, expected_names)
    return expected_names


def _rename_record_batch(batch: pa.RecordBatch, names: list[str]) -> pa.RecordBatch:
    arrays = [batch.column(i) for i in range(batch.num_columns)]
    return pa.RecordBatch.from_arrays(arrays, names=names)


def _rename_table(table: pa.Table, names: list[str]) -> pa.Table:
    arrays = [table.column(i) for i in range(table.num_columns)]
    return pa.Table.from_arrays(arrays, names=names)


def _to_polars(item: Any, *, expected_columns: list[str] | None = None) -> pl.DataFrame:
    expected_names = _expected_column_names(expected_columns)

    if isinstance(item, tuple) and len(item) == 2:
        _, item = item

    if isinstance(item, pl.DataFrame):
        actual_names = list(item.columns)
        final_names = _validate_and_resolve_names(actual_names, expected_names)

        if final_names != actual_names:
            item = item.clone()
            item.columns = final_names

        return item

    if isinstance(item, pa.RecordBatch):
        actual_names = list(item.schema.names)
        final_names = _validate_and_resolve_names(actual_names, expected_names)

        if final_names != actual_names:
            item = _rename_record_batch(item, final_names)

        return pl.from_arrow(pa.Table.from_batches([item]))

    if isinstance(item, pa.Table):
        actual_names = list(item.schema.names)
        final_names = _validate_and_resolve_names(actual_names, expected_names)

        if final_names != actual_names:
            item = _rename_table(item, final_names)

        return pl.from_arrow(item)

    raise TypeError(f"Tipo de chunk não suportado: {type(item)!r}")


def iter_polars_chunks(
    items: Iterable[Any],
    chunk_rows: int | None = None,
    *,
    expected_columns: list[str] | None = None,
) -> Iterator[pl.DataFrame]:
    """
    Converte a entrada da extração em DataFrames Polars.
    Se chunk_rows for informado, reagrupa os lotes até esse tamanho aproximado.

    Quando expected_columns for informado, os nomes vindos do SQL passam a ser
    apenas informativos: a validação é feita por quantidade/ordem e os nomes
    canônicos do contrato são aplicados ao lote antes da conversão final.
    """
    target = int(chunk_rows or 0)
    if target <= 0:
        for item in items:
            yield _to_polars(item, expected_columns=expected_columns)
        return

    buf: pl.DataFrame | None = None
    for item in items:
        df = _to_polars(item, expected_columns=expected_columns)
        if buf is None:
            buf = df
        else:
            buf = pl.concat([buf, df], how="vertical_relaxed")

        while buf is not None and buf.height >= target:
            yield buf.slice(0, target)
            rem = buf.slice(target)
            buf = rem if rem.height > 0 else None

    if buf is not None and buf.height > 0:
        yield buf