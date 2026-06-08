from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple
import polars as pl

_DEC128_RE = re.compile(r"^decimal128\((\d+),(\d+)\)$", re.IGNORECASE)
_INLINE_ARG_KEYS = {"value", "pattern", "format", "scale", "values", "len", "char", "old", "new", "unit"}

SUPPORTED_NORM_OPS = { "trim",    "upper",    "lower",    "to_date",    "to_datetime",    "coalesce",    "replace",    "digits_only",    "lpad",}
SUPPORTED_VAL_RULES = { "required", "notnull", "min", "max", "gt", "ge", "max_length", "len_max", "regex","in","date_gte","date_lte", "date_format", "decimal_scale", "cpf_cnpj_by_tipo_credor",}
SUPPORTED_LEVELS = {"error", "warning"}

@dataclass
class Rule:
    kind: str
    rule: str
    field: Optional[str]
    args: Dict[str, Any]
    when: Optional[Dict[str, Any]] = None
    enabled: bool = True
    comment: Optional[str] = None
    level: str = "error"


@dataclass(frozen=True)
class CompiledNormalization:
    rule: Rule
    field: str
    when_expr: Optional[pl.Expr]


@dataclass(frozen=True)
class CompiledValidation:
    rule: Rule
    field: Optional[str]
    when_expr: Optional[pl.Expr]
    invalid_expr: Optional[pl.Expr]
    key: str
    alias: str
    level: str

    @property
    def is_error(self) -> bool:
        return self.level == "error"


def _as_str(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return s if s else None

def _normalize_rule_name(rule_name: Optional[str]) -> Optional[str]:
    name = _as_str(rule_name)
    if not name: return None
    name = name.lower()
    return "max_length" if name == "len_max" else name

def _normalize_level(level: Any) -> str:
    raw = str(level or "error").strip().lower()
    return raw if raw in {"error", "warning"} else "error"

def _normalized_field(field: Optional[str]) -> Optional[str]:
    return _as_str(field)

def _merge_rule_args(obj: Dict[str, Any]) -> Dict[str, Any]:
    args = dict(obj.get("args") or {}) if isinstance(obj.get("args"), dict) else {}
    for key in _INLINE_ARG_KEYS:
        if key in obj and key not in args:
            args[key] = obj.get(key)
    return args

def _safe_alias(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in text)

def _build_validation_key(rule: Rule, *, field: Optional[str], level: str) -> str:
    return f"{level}:{rule.rule}:{field or 'global'}"

def _build_validation_alias(idx: int, rule: Rule, *, field: Optional[str], level: str) -> str:
    return _safe_alias(f"r{idx:03}_{level}_{rule.rule}_{field or 'row'}")

def _compile_when_expr_polars(when: Optional[Dict[str, Any]]) -> Optional[pl.Expr]:
    if not when: return None

    field, op, value = when.get("field"), (when.get("op") or "").lower(), when.get("value")
    if not field or not op: return None

    col = pl.col(field)

    def _cast_col_for_value(c: pl.Expr, v: Any) -> pl.Expr:
        if v is None: return c
        if isinstance(v, bool): return c.cast(pl.Boolean, strict=False)
        if isinstance(v, int): return c.cast(pl.Int64, strict=False)
        if isinstance(v, float): return c.cast(pl.Float64, strict=False)
        return c.cast(pl.Utf8, strict=False)

    if op in {"eq", "ne"}:
        c = _cast_col_for_value(col, value)
        return (c == value) if op == "eq" else (c != value)

    if op in {"in", "nin"}:
        vals = value if isinstance(value, list) else [value]
        probe = next((x for x in vals if x is not None), None)
        c = _cast_col_for_value(col, probe)
        expr = c.is_in(vals)
        return expr if op == "in" else ~expr

    if op == "isnull": return col.is_null()
    if op == "notnull": return col.is_not_null()
    return None

def _make_rule(*, kind: str, field: Optional[str], obj: Dict[str, Any], default_key: str) -> Optional[Rule]:
    if not isinstance(obj, dict): return None
    enabled = bool(obj.get("enabled", True))
    if not enabled: return None

    rule_name = _normalize_rule_name(_as_str(obj.get(default_key)) or _as_str(obj.get("rule")) or _as_str(obj.get("op")))
    if not rule_name: return None

    return Rule(
        kind=kind,
        rule=rule_name,
        field=_as_str(field) if field is not None else _as_str(obj.get("field")),
        args=_merge_rule_args(obj),
        when=obj.get("when") if isinstance(obj.get("when"), dict) else None,
        enabled=enabled,
        comment=_as_str(obj.get("comment")) or _as_str(obj.get("_comment")),
        level=_normalize_level(obj.get("level")),
    )

def _rules_from_ingest(ingest_rules: Dict[str, Any]) -> Tuple[List[Rule], List[Rule]]:
    norms: List[Rule] = []
    vals: List[Rule] = []

    if not isinstance(ingest_rules, dict): return norms, vals

    fields = ingest_rules.get("fields")
    if not isinstance(fields, dict):
        raise ValueError("ingest_rules.json inválido: objeto 'fields' é obrigatório.")

    for field_name, block in fields.items():
        if not isinstance(block, dict): continue

        normalize_items = block.get("normalize", []) or []
        if not isinstance(normalize_items, list):
            raise ValueError(f"Campo '{field_name}': 'normalize' deve ser uma lista.")
        for r in normalize_items:
            obj = _make_rule(kind="norm", field=str(field_name), obj=r, default_key="op")
            if obj is not None: norms.append(obj)

        validate_items = block.get("validate", []) or []
        if not isinstance(validate_items, list):
            raise ValueError(f"Campo '{field_name}': 'validate' deve ser uma lista.")
        for r in validate_items:
            obj = _make_rule(kind="val", field=str(field_name), obj=r, default_key="rule")
            if obj is not None: vals.append(obj)

    top_validations = ingest_rules.get("validations", []) or []
    if not isinstance(top_validations, list):
        raise ValueError("'validations' deve ser uma lista.")

    for r in top_validations:
        obj = _make_rule(kind="val", field=None, obj=r, default_key="rule")
        if obj is not None: vals.append(obj)

    return norms, vals

def _is_supported_cross_rule(rule: Rule) -> bool:
    return (rule.rule or "").strip().lower() == "cpf_cnpj_by_tipo_credor"

def collect_unsupported_rules(norms: List[Rule], vals: List[Rule]) -> List[str]:
    warnings: List[str] = []

    for r in norms:
        op = (r.rule or "").strip().lower()
        if op not in SUPPORTED_NORM_OPS:
            warnings.append(f"Normalização não suportada e ignorada: op='{r.rule}' field='{r.field}'")

    for r in vals:
        op = (r.rule or "").strip().lower()

        if op not in SUPPORTED_VAL_RULES:
            warnings.append(f"Validação não suportada e ignorada: rule='{r.rule}' field='{r.field}'")
            continue

        if not r.field:
            if _is_supported_cross_rule(r): continue
            warnings.append(f"Validação declarada sem field e não executada pelo engine atual: rule='{r.rule}'")
            continue

        if r.field == "*":
            warnings.append(f"Validação com field='*' declarada, mas não executada pelo engine atual: rule='{r.rule}'")
            continue

        if r.when and _compile_when_expr_polars(r.when) is None:
            warnings.append(f"Cláusula when não suportada e ignorada: rule='{r.rule}' field='{r.field}' when={r.when}")

    return warnings

def _parse_arrow_type(t: Any) -> pl.DataType:
    if t is None: return pl.Utf8
    if isinstance(t, dict) and "type" in t: t = t.get("type")

    if isinstance(t, str):
        s = t.strip().lower()
        if s in {"string", "utf8"}: return pl.Utf8
        if s in {"int8"}: return pl.Int8
        if s in {"int16"}: return pl.Int16
        if s in {"int32"}: return pl.Int32
        if s in {"int64"}: return pl.Int64
        if s in {"uint8"}: return pl.UInt8
        if s in {"uint16"}: return pl.UInt16
        if s in {"uint32"}: return pl.UInt32
        if s in {"uint64"}: return pl.UInt64
        if s in {"float32"}: return pl.Float32
        if s in {"float64"}: return pl.Float64
        if s in {"bool", "boolean"}: return pl.Boolean
        if s.startswith("timestamp"): return pl.Datetime("ms")
        if s in {"date32", "date"}: return pl.Date

        m = _DEC128_RE.match(s)
        if m:
            precision, scale = int(m.group(1)), int(m.group(2))
            return pl.Decimal(precision=precision, scale=scale)

    return pl.Utf8

def _enforce_schema_arrow_polars(df: pl.DataFrame, *, schema_arrow: Dict[str, Any]) -> pl.DataFrame:
    fields = schema_arrow.get("fields") if isinstance(schema_arrow, dict) else None
    if not isinstance(fields, list) or not fields: return df

    ordered_names: List[str] = []
    casts: List[pl.Expr] = []

    for f in fields:
        if not isinstance(f, dict) or not f.get("name"): continue

        name = str(f["name"])
        ordered_names.append(name)
        if name not in df.columns: continue

        dtype, col = _parse_arrow_type(f.get("type")), pl.col(name)
        casts.append(col.cast(pl.Date if dtype == pl.Date else dtype, strict=False).alias(name))

    if casts: df = df.with_columns(casts)

    present = [c for c in ordered_names if c in df.columns]
    extras = [c for c in df.columns if c not in present]
    return df.select(present + extras)

""" def _polars_norm_expr_on(expr: pl.Expr, rule: Rule) -> Optional[pl.Expr]:
    return build_norm_expr(expr, rule) """

""" def _polars_norm_expr(rule: Rule) -> Optional[pl.Expr]:
    if not rule.field: return None
    return _polars_norm_expr_on(pl.col(rule.field), rule) """

def _polars_cross_val_expr(rule: Rule) -> Optional[pl.Expr]:
    op, args = (rule.rule or "").strip().lower(), rule.args or {}

    if op == "cpf_cnpj_by_tipo_credor":
        tipo_field = _as_str(args.get("tipo_field")) or "tipo_credor"
        doc_field = _as_str(args.get("doc_field")) or "cpf_cnpj_credor"

        tipo = pl.col(tipo_field).cast(pl.Int64, strict=False)
        doc = pl.col(doc_field).cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
        invalido_cpf = ((tipo == 1) & (doc.str.len_chars() != 11)).fill_null(False)
        invalido_cnpj = ((tipo == 2) & (doc.str.len_chars() != 14)).fill_null(False)
        return (invalido_cpf | invalido_cnpj).fill_null(False)

    return None

def _polars_val_expr(rule: Rule) -> Optional[pl.Expr]:
    op, args, f = (rule.rule or "").strip().lower(), rule.args or {}, rule.field
    if not f: return _polars_cross_val_expr(rule)
    if f == "*": return None

    col = pl.col(f)

    if op in {"required", "notnull"}:
        return (col.is_null() | (col.cast(pl.Utf8, strict=False).str.strip_chars() == "")).fill_null(False)
    if op == "min":
        v = args.get("value")
        return None if v is None else (col.cast(pl.Float64, strict=False) < float(v)).fill_null(False)
    if op == "max":
        v = args.get("value")
        return None if v is None else (col.cast(pl.Float64, strict=False) > float(v)).fill_null(False)
    if op == "gt":
        v = args.get("value")
        return None if v is None else (col.cast(pl.Float64, strict=False) <= float(v)).fill_null(False)
    if op == "ge":
        v = args.get("value")
        return None if v is None else (col.cast(pl.Float64, strict=False) < float(v)).fill_null(False)
    if op in {"max_length", "len_max"}:
        v = args.get("value")
        return None if v is None else (col.cast(pl.Utf8, strict=False).str.len_chars() > int(v)).fill_null(False)
    if op == "regex":
        pat = _as_str(args.get("pattern"))
        if not pat: return None
        s = col.cast(pl.Utf8, strict=False)
        return (s.is_not_null() & (~s.str.contains(pat))).fill_null(False)
    if op == "in":
        vals = args.get("values")
        if not isinstance(vals, list) or not vals: return None
        s = col.cast(pl.Utf8, strict=False)
        return (s.is_not_null() & (~s.is_in([str(x) for x in vals]))).fill_null(False)
    if op == "date_gte":
        v = _as_str(args.get("value"))
        if not v: return None
        dt, dcol = datetime.fromisoformat(v).date(), col.cast(pl.Date, strict=False)
        return (dcol.is_not_null() & (dcol < pl.lit(dt))).fill_null(False)
    if op == "date_lte":
        v = _as_str(args.get("value"))
        if not v: return None
        dt, dcol = datetime.fromisoformat(v).date(), col.cast(pl.Date, strict=False)
        return (dcol.is_not_null() & (dcol > pl.lit(dt))).fill_null(False)
    if op == "date_format":
        fmt = _as_str(args.get("format")) or "yyyy-mm-dd"
        parse_fmt = {"yyyy-mm-dd": "%Y-%m-%d", "yyyy/mm/dd": "%Y/%m/%d", "dd/mm/yyyy": "%d/%m/%Y"}.get(fmt.lower(), fmt)
        s = col.cast(pl.Utf8, strict=False).str.strip_chars()
        parsed = s.str.strptime(pl.Date, format=parse_fmt, strict=False)
        return ((s != "") & s.is_not_null() & parsed.is_null()).fill_null(False)
    if op == "decimal_scale":
        scale = args.get("scale")
        if scale is None: return None
        s = col.cast(pl.Utf8, strict=False).str.strip_chars().str.replace_all(",", ".")
        pat = rf"^-?\d+(?:\.\d{{1,{int(scale)}}})?$"
        return ((s != "") & s.is_not_null() & (~s.str.contains(pat))).fill_null(False)

    return None

def _compile_rule_base(rule: Rule) -> tuple[Optional[str], str, Optional[pl.Expr]]:
    field = _normalized_field(rule.field)
    level = _normalize_level(getattr(rule, "level", "error"))
    when_expr = _compile_when_expr_polars(getattr(rule, "when", None))
    return field, level, when_expr

def compile_normalizations(rules: Sequence[Rule]) -> list[CompiledNormalization]:
    compiled: list[CompiledNormalization] = []

    for rule in rules:
        field, _, when_expr = _compile_rule_base(rule)
        if not field or field == "*": continue
        compiled.append(CompiledNormalization(rule=rule, field=field, when_expr=when_expr))

    return compiled

def compile_validations(rules: Sequence[Rule]) -> list[CompiledValidation]:
    compiled: list[CompiledValidation] = []

    for idx, rule in enumerate(rules, start=1):
        field, level, when_expr = _compile_rule_base(rule)
        invalid_expr = _polars_val_expr(rule)

        if invalid_expr is not None and when_expr is not None:
            invalid_expr = (
                pl.when(when_expr)
                .then(invalid_expr)
                .otherwise(pl.lit(False))
            )

        compiled.append(
            CompiledValidation(
                rule=rule,
                field=field,
                when_expr=when_expr,
                invalid_expr=invalid_expr,
                key=_build_validation_key(rule, field=field, level=level),
                alias=_build_validation_alias(idx, rule, field=field, level=level),
                level=level,
            )
        )

    return compiled

def build_norm_expr(expr: pl.Expr, rule) -> pl.Expr | None:
    op = (rule.rule or "").strip().lower()
    args = rule.args or {}

    if op == "trim": return expr.cast(pl.Utf8, strict=False).str.strip_chars()
    if op == "upper": return expr.cast(pl.Utf8, strict=False).str.to_uppercase()
    if op == "lower": return expr.cast(pl.Utf8, strict=False).str.to_lowercase()

    if op == "to_date":
        return expr.cast(pl.Utf8, strict=False).str.strptime(
            pl.Date, format=_as_str(args.get("format")) or "%Y-%m-%d", strict=False,
        )

    if op == "to_datetime":
        return expr.cast(pl.Utf8, strict=False).str.strptime(
            pl.Datetime(_as_str(args.get("unit")) or "ms"),
            format=_as_str(args.get("format")) or "%Y-%m-%d %H:%M:%S",
            strict=False,
        )

    if op == "coalesce": return expr.fill_null(args.get("value"))

    if op == "replace":
        old = _as_str(args.get("old"))
        if old is None: return None
        return expr.cast(pl.Utf8, strict=False).str.replace_all(old, _as_str(args.get("new")) or "")

    if op == "digits_only": return expr.cast(pl.Utf8, strict=False).str.replace_all(r"[^0-9]", "")

    if op == "lpad":
        width = args.get("len")
        if width is None: return None
        return expr.cast(pl.Utf8, strict=False).str.pad_start(
            int(width), fill_char=(_as_str(args.get("char")) or "0")[:1],
        )

    return None