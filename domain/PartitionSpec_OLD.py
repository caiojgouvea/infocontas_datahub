from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class PartitionField:
    name: str
    source: str
    source_from: str = "dataframe"


@dataclass(frozen=True)
class PartitionSpec:
    fields: tuple[PartitionField, ...]

    @staticmethod
    def from_contract(contract) -> "PartitionSpec":
        if contract is None:
            raise ValueError("contract não informado")

        ingest_rules = getattr(contract, "ingest_rules", None) or {}
        part = ingest_rules.get("partitioning") or {}
        fields = part.get("fields")

        if not isinstance(fields, list) or not fields:
            raise ValueError("Contrato sem partitioning.fields válido")

        parsed: list[PartitionField] = []

        for item in fields:
            if not isinstance(item, dict):
                raise ValueError("partitioning.fields deve conter apenas objetos")

            name = str(item.get("name") or "").strip()
            source = str(item.get("source") or "").strip()
            source_from = str(item.get("from") or "dataframe").strip().lower()

            if not name:
                raise ValueError("partitioning.fields contém name vazio")
            if not source:
                raise ValueError(f"partitioning.fields.{name} contém source vazio")
            if source_from not in {"ctx", "dataframe"}:
                raise ValueError(
                    f"partitioning.fields.{name}.from inválido: {source_from!r}. "
                    "Use 'ctx' ou 'dataframe'."
                )

            parsed.append(PartitionField(name=name, source=source, source_from=source_from))

        return PartitionSpec(tuple(parsed))

    def get_fields(self, *, drop_last_n: int = 0) -> tuple[PartitionField, ...]:
        if drop_last_n < 0:
            raise ValueError("drop_last_n não pode ser negativo")
        if drop_last_n > len(self.fields):
            raise ValueError("drop_last_n maior que número de partições")
        return self.fields[:-drop_last_n] if drop_last_n else self.fields

    def values_from_df_row(self, row: dict[str, Any], ctx=None, *, drop_last_n: int = 0) -> dict[str, object]:
        values: dict[str, object] = {}

        for field in self.get_fields(drop_last_n=drop_last_n):
            if field.source_from == "ctx":
                values[field.name] = self._ctx_value(ctx, field)
            else:
                if field.source not in row:
                    raise ValueError(
                        f"DataFrame não possui coluna para partição '{field.name}' "
                        f"(source='{field.source}')"
                    )
                values[field.name] = row[field.source]

        self.validate_values(values, drop_last_n=drop_last_n)
        return values

    def unique_partition_values_from_df(self, df: pl.DataFrame, ctx, *, drop_last_n: int = 0) -> list[dict[str, object]]:
        fields = self.get_fields(drop_last_n=drop_last_n)
        df_sources = [f.source for f in fields if f.source_from == "dataframe"]

        missing = [c for c in df_sources if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas de partição ausentes no DataFrame: {missing}")

        rows = df.select(df_sources).unique().to_dicts() if df_sources else [{}]

        return [
            self.values_from_df_row(row, ctx=ctx, drop_last_n=drop_last_n)
            for row in rows
        ]

    def validate_values(self, values: dict, *, drop_last_n: int = 0) -> None:
        required = [f.name for f in self.get_fields(drop_last_n=drop_last_n)]
        missing = [name for name in required if name not in values]

        if missing:
            raise ValueError(f"Valores de partição ausentes: {missing}")

        for name in required:
            value = values[name]
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Valor inválido para partição '{name}': {value!r}")

    def render_parts(self, values: dict, *, drop_last_n: int = 0) -> list[str]:
        self.validate_values(values, drop_last_n=drop_last_n)
        return [
            f"{field.name}={values[field.name]}"
            for field in self.get_fields(drop_last_n=drop_last_n)
        ]

    def build_prefix(self, *, base_prefix: str, values: dict, drop_last_n: int = 0) -> str:
        parts = self.render_parts(values, drop_last_n=drop_last_n)
        return "/".join([base_prefix.rstrip("/")] + parts) + "/"

    def build_path(self, *, base_dir: Path, values: dict, drop_last_n: int = 0) -> Path:
        parts = self.render_parts(values, drop_last_n=drop_last_n)
        return Path(base_dir).joinpath(*parts)

    @staticmethod
    def _ctx_value(ctx, field: PartitionField):
        if ctx is None or not hasattr(ctx, field.source):
            raise ValueError(
                f"Contexto não possui valor para partição '{field.name}' "
                f"(source='{field.source}')"
            )
        return getattr(ctx, field.source)