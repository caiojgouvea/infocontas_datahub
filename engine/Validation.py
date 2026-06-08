from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import polars as pl

from domain.Rules import CompiledValidation


@dataclass(frozen=True)
class ValidationResult:
    valid_df: pl.DataFrame
    invalid_df: pl.DataFrame
    invalid_count: int
    rule_counts: Dict[str, int]
    invalid_flags_df: pl.DataFrame
    warning_df: pl.DataFrame
    warning_count: int
    warning_flags_df: pl.DataFrame


def _false_series(name: str, height: int) -> pl.Series:
    return pl.Series(name, [False] * height)


def _empty_result(df: pl.DataFrame, *, invalid_df=None, warning_df=None) -> ValidationResult:
    empty_flags = pl.DataFrame()
    return ValidationResult(
        valid_df=df,
        invalid_df=invalid_df if invalid_df is not None else df,
        invalid_count=0,
        rule_counts={},
        invalid_flags_df=empty_flags,
        warning_df=warning_df if warning_df is not None else df,
        warning_count=0,
        warning_flags_df=empty_flags,
    )


def validate_chunk(df: pl.DataFrame, compiled: List[CompiledValidation]) -> ValidationResult:
    if df.height == 0:
        return _empty_result(df)

    if not compiled:
        return _empty_result(df, invalid_df=df.head(0), warning_df=df.head(0))

    flag_exprs: List[pl.Expr] = []
    alias_to_key: Dict[str, str] = {}
    error_aliases: List[str] = []
    warning_aliases: List[str] = []

    for item in compiled:
        expr = getattr(item, "invalid_expr", None)
        if expr is None:
            continue

        alias = item.alias
        flag_exprs.append(expr.alias(alias))
        alias_to_key[alias] = item.key
        error_aliases.append(alias) if item.is_error else warning_aliases.append(alias)

    if not flag_exprs:
        return _empty_result(df, invalid_df=df.head(0), warning_df=df.head(0))

    flags_df = df.select(flag_exprs)
    all_flag_cols = [c for c in flags_df.columns if c in alias_to_key]
    error_cols = [c for c in error_aliases if c in flags_df.columns]
    warning_cols = [c for c in warning_aliases if c in flags_df.columns]

    row_invalid = (
        flags_df.select(pl.any_horizontal([pl.col(c) for c in error_cols]).alias("__row_invalid__")).get_column("__row_invalid__")
        if error_cols else _false_series("__row_invalid__", df.height)
    )
    row_warning = (
        flags_df.select(pl.any_horizontal([pl.col(c) for c in warning_cols]).alias("__row_warning__")).get_column("__row_warning__")
        if warning_cols else _false_series("__row_warning__", df.height)
    )

    row_warning_only = row_warning & (~row_invalid)

    invalid_count = int(row_invalid.cast(pl.Int64).sum())
    warning_count = int(row_warning_only.cast(pl.Int64).sum())

    valid_df = df.filter(~row_invalid)
    invalid_df = df.filter(row_invalid)
    warning_df = df.filter(row_warning_only)

    invalid_flags_df = flags_df.filter(row_invalid)
    warning_flags_df = flags_df.filter(row_warning_only)

    if invalid_flags_df.height > 0:
        invalid_flags_df = invalid_flags_df.with_columns(pl.lit(True).alias("__row_invalid__"))

    if warning_flags_df.height > 0:
        warning_flags_df = warning_flags_df.with_columns(pl.lit(True).alias("__row_warning__"))

    if all_flag_cols:
        counts_row = flags_df.select([pl.col(alias).cast(pl.Int64).sum().alias(alias) for alias in all_flag_cols]).row(0, named=True)
        rule_counts = {alias_to_key[alias]: int(counts_row.get(alias) or 0) for alias in all_flag_cols}
    else:
        rule_counts = {}

    return ValidationResult(valid_df=valid_df,invalid_df=invalid_df,invalid_count=invalid_count,rule_counts=rule_counts,invalid_flags_df=invalid_flags_df,warning_df=warning_df,warning_count=warning_count,
                            warning_flags_df=warning_flags_df,)