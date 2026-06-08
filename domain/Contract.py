from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from domain.Rules import SUPPORTED_LEVELS, SUPPORTED_NORM_OPS, SUPPORTED_VAL_RULES


@dataclass(frozen=True)
class Contract:
    dataset: str
    version: str
    contract_dir: Path | None
    schema_arrow: dict[str, Any]
    ingest_rules: dict[str, Any]

    @classmethod
    def from_dicts(cls, *, dataset: str, version: str, schema_arrow: dict[str, Any],
                   ingest_rules: dict[str, Any], contract_dir: str | Path | None = None) -> "Contract":
        dataset, version = (dataset or "").strip(), (version or "").strip()
        if not dataset: raise ValueError("dataset vazio")
        if not version: raise ValueError("version vazio")
        if not isinstance(schema_arrow, dict): raise ValueError("schema.arrow deve ser dict")
        if not isinstance(ingest_rules, dict): raise ValueError("ingest_rules.json deve ser dict")
        return cls(dataset=dataset, version=version, contract_dir=Path(contract_dir).resolve() if contract_dir else None,
                   schema_arrow=schema_arrow, ingest_rules=ingest_rules)

    @classmethod
    def from_local_dir(cls, *, dataset: str, version: str, contract_dir: str | Path) -> "Contract":
        contract_dir = Path(contract_dir).resolve()
        schema_path, rules_path = contract_dir / "schema.arrow", contract_dir / "ingest_rules.json"
        if not schema_path.exists(): raise FileNotFoundError(f"schema.arrow ausente em {schema_path}")
        if not rules_path.exists(): raise FileNotFoundError(f"ingest_rules.json ausente em {rules_path}")
        return cls.from_dicts(dataset=dataset, version=version, contract_dir=contract_dir,
                              schema_arrow=json.loads(schema_path.read_text(encoding="utf-8")),
                              ingest_rules=json.loads(rules_path.read_text(encoding="utf-8")))

    def validate(self, *, fail_fast: bool = False):
        return validate_contract(self, fail_fast=fail_fast)

    @staticmethod
    def summarize_issues(issues):
        return summarize_issues(issues)

    def validate_input_layout_strict(self, input_columns: list[str], *, allow_extra: bool = False,
                                     require_exact_order: bool = True):
        return validate_input_layout_strict(self, input_columns, allow_extra=allow_extra,
                                            require_exact_order=require_exact_order)

    @property
    def input_column_names(self) -> list[str]:
        fields = self.schema_arrow.get("fields")
        return [] if not isinstance(fields, list) else [str(f["name"]) for f in fields if isinstance(f, dict) and f.get("name")]


@dataclass(frozen=True)
class ContractIssue:
    level: str
    where: str
    message: str


def _issue(issues: list[ContractIssue], level: str, where: str, message: str, *, fail_fast: bool) -> bool:
    issues.append(ContractIssue(level, where, message)); return bool(fail_fast)


def _rule_name(obj: Any, default_key: str) -> str:
    if not isinstance(obj, dict): return ""
    return str(obj.get(default_key, "") or obj.get("rule", "") or obj.get("op", "") or "").strip().lower()


def _rule_level(obj: Any) -> str:
    return str((obj if isinstance(obj, dict) else {}).get("level", "error") or "error").strip().lower()


def _schema_field_names(fields: Any) -> list[str]:
    return [] if not isinstance(fields, list) else [str(f["name"]) for f in fields if isinstance(f, dict) and f.get("name")]


def _has_required_rule(block: Any) -> bool:
    if not isinstance(block, dict) or not isinstance(block.get("validate", []), list): return False
    return any(isinstance(item, dict) and item.get("enabled", True) and _rule_name(item, "rule") in {"required", "notnull"}
               for item in block.get("validate", []))


def _validate_identity(issues: list[ContractIssue], *, contract: Contract, sa: dict, ir: dict, fail_fast: bool) -> bool:
    pairs = (
        ("schema.arrow.dataset", sa.get("dataset"), contract.dataset, "schema_arrow"),
        ("schema.arrow.version", sa.get("version"), contract.version, "schema_arrow"),
        ("ingest_rules.json.dataset", ir.get("dataset"), contract.dataset, "ingest_rules"),
        ("ingest_rules.json.version", ir.get("version"), contract.version, "ingest_rules"),
    )
    for where, found, expected, src in pairs:
        found = str(found or "").strip()
        if found and found != expected:
            label = "dataset" if where.endswith(".dataset") else "version"
            if _issue(issues, "ERROR", where, f"{label} divergente: contrato='{expected}' {src}='{found}'", fail_fast=fail_fast): return True
    return False


def _validate_rule_items(issues: list[ContractIssue], *, items: Any, where: str, supported: set[str],
                         default_key: str, fail_fast: bool, warning_prefix: str) -> bool:
    if items is not None and not isinstance(items, list):
        return _issue(issues, "ERROR", where, f"{where.split('.')[-1]} deve ser list", fail_fast=fail_fast)
    if not isinstance(items, list): return False

    for idx, item in enumerate(items):
        item_where = f"{where}[{idx}]"
        if not isinstance(item, dict):
            if _issue(issues, "ERROR", item_where, f"cada item de {where.split('.')[-1]} deve ser dict", fail_fast=fail_fast): return True
            continue

        name, level = _rule_name(item, default_key), _rule_level(item)
        if name and name not in supported:
            if _issue(issues, "WARNING", item_where, f"{warning_prefix}: '{name}'", fail_fast=fail_fast): return True
        if level not in SUPPORTED_LEVELS:
            if _issue(issues, "ERROR", f"{item_where}.level",
                      f"level inválido: '{level}'. Valores aceitos: {sorted(SUPPORTED_LEVELS)}", fail_fast=fail_fast): return True
    return False


def _validate_fields(issues: list[ContractIssue], *, fields: Any, sa_set: set[str], fail_fast: bool) -> bool:
    if not isinstance(fields, dict) or not fields:
        return _issue(issues, "ERROR", "ingest_rules.json.fields", "fields obrigatório e deve ser dict", fail_fast=fail_fast)

    for field_name, block in fields.items():
        where = f"ingest_rules.json.fields.{field_name}"
        if field_name not in sa_set:
            if _issue(issues, "ERROR", where, f"Campo '{field_name}' não existe em schema.arrow", fail_fast=fail_fast): return True
        if not isinstance(block, dict):
            if _issue(issues, "ERROR", where, "bloco do campo deve ser dict", fail_fast=fail_fast): return True
            continue
        if _validate_rule_items(issues, items=block.get("normalize", []), where=f"{where}.normalize",
                                supported=SUPPORTED_NORM_OPS, default_key="op", fail_fast=fail_fast,
                                warning_prefix="Operação de normalização não suportada pelo engine"): return True
        if _validate_rule_items(issues, items=block.get("validate", []), where=f"{where}.validate",
                                supported=SUPPORTED_VAL_RULES, default_key="rule", fail_fast=fail_fast,
                                warning_prefix="Regra de validação não suportada pelo engine"): return True
    return False


def _validate_nullable_consistency(issues: list[ContractIssue], *, sa_fields: Any, fields: Any, fail_fast: bool) -> bool:
    if not isinstance(sa_fields, list) or not isinstance(fields, dict): return False

    for f in sa_fields:
        if not isinstance(f, dict): continue
        name = str(f.get("name", "") or "").strip()
        if not name or f.get("nullable", True) is not False or _has_required_rule(fields.get(name)): continue
        msg = (f"Campo '{name}' está com nullable=false no schema.arrow, mas não possui regra required/notnull "
               f"no ingest_rules.json. Verifique se o NOT NULL já é garantido na origem (SQL) "
               f"ou se uma regra required deveria ser declarada.")
        if _issue(issues, "WARNING", f"schema.arrow.fields.{name}.nullable", msg, fail_fast=fail_fast): return True
    return False


def _validate_top_validations(issues: list[ContractIssue], *, validations: Any, sa_set: set[str], fail_fast: bool) -> bool:
    if validations is not None and not isinstance(validations, list):
        return _issue(issues, "ERROR", "ingest_rules.json.validations", "validations deve ser list", fail_fast=fail_fast)
    if not isinstance(validations, list): return False

    for idx, rule in enumerate(validations):
        where = f"ingest_rules.json.validations[{idx}]"
        if not isinstance(rule, dict):
            if _issue(issues, "ERROR", where, "cada item de validations deve ser dict", fail_fast=fail_fast): return True
            continue

        field, name, level = str(rule.get("field", "") or "").strip(), _rule_name(rule, "rule"), _rule_level(rule)
        if field and field != "*" and field not in sa_set:
            if _issue(issues, "ERROR", f"{where}.field", f"Campo '{field}' não existe em schema.arrow", fail_fast=fail_fast): return True
        if name and name not in SUPPORTED_VAL_RULES:
            if _issue(issues, "WARNING", where, f"Regra top-level não suportada pelo engine: '{name}'", fail_fast=fail_fast): return True
        if level not in SUPPORTED_LEVELS:
            if _issue(issues, "ERROR", f"{where}.level",
                      f"level inválido: '{level}'. Valores aceitos: {sorted(SUPPORTED_LEVELS)}", fail_fast=fail_fast): return True
    return False


def _validate_partitioning(issues: list[ContractIssue], *, part: Any, fail_fast: bool) -> bool:
    if not isinstance(part, dict):
        return _issue(issues, "ERROR", "ingest_rules.json.partitioning",
                      "partitioning obrigatório e deve ser dict", fail_fast=fail_fast)

    style = str(part.get("style", "") or "").strip()
    if not style:
        if _issue(issues, "ERROR", "ingest_rules.json.partitioning.style", "style obrigatório", fail_fast=fail_fast): return True

    fields = part.get("fields")
    if not isinstance(fields, list) or not fields:
        return _issue(issues, "ERROR", "ingest_rules.json.partitioning.fields",
                      "fields obrigatório e deve ser list não vazia", fail_fast=fail_fast)

    for idx, item in enumerate(fields):
        where = f"ingest_rules.json.partitioning.fields[{idx}]"
        if not isinstance(item, dict):
            if _issue(issues, "ERROR", where, "cada item de fields deve ser dict", fail_fast=fail_fast): return True
            continue

        name = str(item.get("name", "") or "").strip()
        source = str(item.get("source", "") or "").strip()
        source_from = str(item.get("from", "dataframe") or "dataframe").strip().lower()

        if not name and _issue(issues, "ERROR", f"{where}.name", "name obrigatório", fail_fast=fail_fast): return True
        if not source and _issue(issues, "ERROR", f"{where}.source", "source obrigatório", fail_fast=fail_fast): return True
        if source_from not in {"ctx", "dataframe"}:
            if _issue(issues, "ERROR", f"{where}.from",
                      "from inválido. Valores aceitos: ctx, dataframe", fail_fast=fail_fast): return True
    return False


def validate_contract(contract: Contract, *, fail_fast: bool = False):
    issues: list[ContractIssue] = []
    sa, ir = contract.schema_arrow or {}, contract.ingest_rules or {}

    if not isinstance(sa, dict): return [ContractIssue("ERROR", "schema.arrow", "schema.arrow deve ser um dict")]
    if not isinstance(ir, dict):
        issues.append(ContractIssue("ERROR", "ingest_rules.json", "ingest_rules.json deve ser um dict"))
        if fail_fast: return issues

    sa_fields = sa.get("fields")
    if not isinstance(sa_fields, list) or not sa_fields:
        issues.append(ContractIssue("ERROR", "schema.arrow.fields", "fields ausente/vazio"))
        if fail_fast: return issues

    sa_names, fields = _schema_field_names(sa_fields), ir.get("fields")
    sa_set = set(sa_names)

    if _validate_identity(issues, contract=contract, sa=sa, ir=ir, fail_fast=fail_fast): return issues
    if _validate_fields(issues, fields=fields, sa_set=sa_set, fail_fast=fail_fast): return issues
    if _validate_nullable_consistency(issues, sa_fields=sa_fields, fields=fields, fail_fast=fail_fast): return issues
    if _validate_top_validations(issues, validations=ir.get("validations", []), sa_set=sa_set, fail_fast=fail_fast): return issues
    if _validate_partitioning(issues, part=ir.get("partitioning"), fail_fast=fail_fast): return issues
    return issues


def summarize_issues(issues):
    if not issues: return "Contrato OK (sem problemas)."
    errs, warns = [i for i in issues if i.level == "ERROR"], [i for i in issues if i.level != "ERROR"]

    def _fmt(label: str, items: list[ContractIssue]) -> str:
        head = f"{label}({len(items)}): " + "; ".join(f"{x.where}: {x.message}" for x in items[:5])
        return head if len(items) <= 5 else head + f" | (+{len(items) - 5} {label.lower()})"

    return " | ".join(part for part in (_fmt("ERROS", errs) if errs else "", _fmt("WARN", warns) if warns else "") if part)


def validate_input_layout_strict(contract: Contract, input_columns: list[str], *,
                                 allow_extra: bool = False, require_exact_order: bool = True):
    issues: list[ContractIssue] = []
    input_columns, expected = list(input_columns or []), list(contract.input_column_names)
    exp_set, in_set = set(expected), set(input_columns)
    missing, extra = sorted(exp_set - in_set), sorted(in_set - exp_set)

    if missing: return [ContractIssue("ERROR", "input.columns", f"Colunas ausentes: {missing}")]
    if extra and not allow_extra: return [ContractIssue("ERROR", "input.columns", f"Colunas extras: {extra}")]

    if require_exact_order and not allow_extra and input_columns != expected:
        n = min(len(input_columns), len(expected))
        idx = next((i for i in range(n) if input_columns[i] != expected[i]), n)
        issues.append(ContractIssue(
            "ERROR", "input.order",
            f"Ordem de colunas divergente. Primeiro mismatch idx={idx}: "
            f"esperado={expected[idx] if idx < len(expected) else None!r}, "
            f"recebido={input_columns[idx] if idx < len(input_columns) else None!r}",
        ))

    return issues