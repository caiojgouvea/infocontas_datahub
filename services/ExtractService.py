from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

import pyarrow as pa

from app.logger import format_int, log_error_last, log_item, log_last, log_section
from infra.database.BatchExtractor import extract_record_batches_from_file

logger = logging.getLogger("extract")


class ExtractService:
    def __init__(self, *, batch_rows: int = 200_000, driver_arraysize: int = 10_000):
        self.batch_rows = int(batch_rows)
        self.driver_arraysize = int(driver_arraysize)
        if self.batch_rows <= 0:
            raise ValueError(f"batch_rows deve ser > 0. Valor recebido: {batch_rows}")
        if self.driver_arraysize <= 0:
            raise ValueError(f"driver_arraysize deve ser > 0. Valor recebido: {driver_arraysize}")

    def extract_batches(self, *, sql_path: str | Path, ano: int, schema: pa.Schema | None = None) -> Iterator[pa.RecordBatch]:
        log_section(logger, "EXTRAÇÃO")
        log_item(logger, f"SQL: {sql_path}")
        log_item(logger, f"Exercício: {ano}")

        def _generator() -> Iterator[pa.RecordBatch]:
            batch_count = rows_total = 0

            try:
                iterator = extract_record_batches_from_file(
                    sql_path,
                    params=[ano],
                    chunk_rows=self.batch_rows,
                    driver_arraysize=self.driver_arraysize,
                    schema=schema,
                )

                for _, record_batch in iterator:
                    batch_count += 1
                    rows_total += record_batch.num_rows
                    yield record_batch

                log_last(logger, f"Linhas extraídas: {format_int(rows_total)} | lotes={format_int(batch_count)}")

            except Exception as exc:
                log_error_last(logger, f"Falha na extração | lotes={format_int(batch_count)} | linhas={format_int(rows_total)} | erro={exc}")
                raise

        return _generator()