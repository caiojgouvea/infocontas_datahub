from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable
import polars as pl
from domain.Rules import build_norm_expr

""" def _as_str(value) -> str | None:
    if value is None: return None
    text = str(value).strip()
    return text if text else None
 """
def apply_normalizations(df: pl.DataFrame, compiled_norms: Iterable[Any]) -> tuple[pl.DataFrame, list[dict]]:
    by_field: dict[str, list[Any]] = defaultdict(list)

    for item in compiled_norms:
        if item.field in df.columns: by_field[item.field].append(item)

    updates: list[pl.Expr] = []
    unsupported: list[dict] = []

    for field, rules in by_field.items():
        expr: pl.Expr = pl.col(field)

        for item in rules:
            next_expr = build_norm_expr(expr, item.rule)

            if next_expr is None:
                unsupported.append({
                    "kind": "norm",
                    "op": item.rule.rule,
                    "field": item.field,
                    "when": item.rule.when,
                    "args": item.rule.args,
                })
                continue

            expr = next_expr if item.when_expr is None else pl.when(item.when_expr).then(next_expr).otherwise(expr)

        updates.append(expr.alias(field))

    return (df.with_columns(updates) if updates else df), unsupported