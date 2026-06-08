from __future__ import annotations

import json, logging, time, shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
import polars as pl

from app.logger import ( format_int, format_seconds, log_detail, log_item, log_last, log_section, log_warn_detail, log_warn_item, log_warn_last,)
from domain.Rules import ( _enforce_schema_arrow_polars, _rules_from_ingest, collect_unsupported_rules, compile_normalizations, compile_validations,)
from domain.PartitionSpec import PartitionSpec

from .Chunking import iter_polars_chunks
from .Normalization import apply_normalizations
from .Validation import validate_chunk

logger = logging.getLogger("engine")

@dataclass
class EngineResult:
    ok: bool; rows_total: int; rows_valid: int; rows_invalid: int; parquet_files: int
    invalid_samples_file: str | None = None
    invalid_samples_count: int = 0
    invalid_samples_truncated: bool = False
    stopped_on_invalid_sample_limit: bool = False
    warning_samples_file: str | None = None
    warning_samples_count: int = 0
    warning_samples_truncated: bool = False
    rows_warning: int = 0
    timings: dict[str, float] = field(default_factory=dict)
    validation_stats: dict[str, int] = field(default_factory=dict)

@dataclass
class EngineState:
    rows_total: int = 0; rows_valid: int = 0; rows_invalid: int = 0; rows_warning: int = 0; parquet_files: int = 0
    validation_stats: dict[str, int] = field(default_factory=dict)
    invalid_sample_rows: list[dict] = field(default_factory=list)
    warning_sample_rows: list[dict] = field(default_factory=list)
    invalid_samples_truncated: bool = False
    warning_samples_truncated: bool = False
    stopped_on_invalid_sample_limit: bool = False

@dataclass
class EngineTimers:
    started_at: float = 0.0
    normalization: float = 0.0
    schema: float = 0.0
    validation: float = 0.0
    parquet_write: float = 0.0

def _resolve_max_invalid_samples(ctx) -> int:
    raw = getattr(ctx, "max_invalid_samples", None)
    if raw is None: return 100
    try: value = int(raw)
    except Exception: return 100
    return 0 if value <= 0 else value

def _errors_dir(ctx) -> Path:
    path = Path(getattr(ctx, "output_dir")) / "errors"
    path.mkdir(parents=True, exist_ok=True)
    return path

def _samples_file_path(ctx, filename: str) -> Path:
    return _errors_dir(ctx) / filename

def _friendly_error_message(compiled_item) -> str:
    rule_name = str(getattr(compiled_item.rule, "rule", "") or "").strip().lower()
    args = getattr(compiled_item.rule, "args", {}) or {}
    messages = {
        "required": "Campo obrigatório ausente ou vazio",
        "notnull": "Campo obrigatório ausente ou vazio",
        "min": f"Valor abaixo do mínimo permitido ({args.get('value')})",
        "max": f"Valor acima do máximo permitido ({args.get('value')})",
        "gt": f"Valor deve ser maior que ({args.get('value')})",
        "ge": f"Valor deve ser maior ou igual a ({args.get('value')})",
        "max_length": f"Tamanho acima do máximo permitido ({args.get('value')})",
        "len_max": f"Tamanho acima do máximo permitido ({args.get('value')})",
        "regex": "Valor fora do formato esperado",
        "in": "Valor fora do conjunto permitido",
        "date_gte": f"Data anterior ao mínimo permitido ({args.get('value')})",
        "date_lte": f"Data posterior ao máximo permitido ({args.get('value')})",
        "date_format": f"Data fora do formato esperado ({args.get('format')})",
        "decimal_scale": f"Quantidade de casas decimais acima do permitido ({args.get('scale')})",
        "cpf_cnpj_by_tipo_credor": "CPF/CNPJ incompatível com o tipo de credor",
    }
    return messages.get(rule_name, f"Regra violada: {rule_name or 'desconhecida'}")

def _schema_arrow_field_names(schema_arrow) -> list[str] | None:
    if not schema_arrow: return None
    names = [name for field in schema_arrow.get("fields") or [] if (name := str((field or {}).get("name", "")).strip())]
    return names or None

def _build_rules_summary(validation_stats: dict[str, int], *, level_prefix: str | None = None) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": int(count)}
        for key, count in sorted(validation_stats.items(), key=lambda x: x[1], reverse=True)
        if count and (not level_prefix or key.startswith(level_prefix))
    ]

def _append_samples(*, sample_df, flags_df, compiled_vals, sample_rows: list[dict], max_samples: int,
                    chunk_number: int, wanted_level: str, marker_column: str) -> int:
    if max_samples <= 0 or sample_df is None or sample_df.height == 0: return 0
    remaining = max_samples - len(sample_rows)
    if remaining <= 0: return 0

    take_n = min(remaining, sample_df.height)
    sample_base = sample_df.head(take_n).to_dicts()
    sample_flags = flags_df.head(take_n).to_dicts() if flags_df is not None else [{}] * take_n
    compiled_by_alias = {
        item.alias: item for item in compiled_vals
        if getattr(item, "invalid_expr", None) is not None and str(getattr(item, "level", "") or "").lower() == wanted_level
    }

    for i, (base_row, flags_row) in enumerate(zip(sample_base, sample_flags), start=1):
        issues = []
        for alias, fired in flags_row.items():
            if alias == marker_column or not fired: continue
            item = compiled_by_alias.get(alias)
            if item is None: continue
            issues.append({
                "level": str(getattr(item, "level", "") or wanted_level),
                "rule": str(getattr(item.rule, "rule", "") or ""),
                "field": getattr(item, "field", None),
                "key": item.key,
                "message": _friendly_error_message(item),
            })

        row_out = dict(base_row)
        for col in ("__invalid_rules__", "__row_invalid__", "__row_warning__"):
            row_out.pop(col, None)
        row_out["__issues__"] = issues
        row_out["__chunk_number__"] = chunk_number
        row_out["__sample_seq__"] = len(sample_rows) + i
        sample_rows.append(row_out)

    return take_n


def _write_samples_json(*, ctx, path: Path, sample_rows: list[dict], rows_total: int, rows_count: int,
                        max_samples: int, truncated: bool, validation_stats: dict[str, int],
                        kind: str, level_prefix: str) -> str | None:
    if not sample_rows: return None
    payload = {
        "dataset": getattr(ctx, "dataset", None), "version": getattr(ctx, "version", None),
        "ano": getattr(ctx, "ano", None), "tc": getattr(ctx, "tc", None),
        "carga_id": getattr(ctx, "carga_id", None), "kind": kind,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rows_total_processed": rows_total, "rows_matched_processed": rows_count,
        "samples_count": len(sample_rows), "max_samples": max_samples, "truncated": truncated,
        "rules_summary": _build_rules_summary(validation_stats, level_prefix=level_prefix),
        "samples": sample_rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return str(path)

def _log_unsupported_rules(norms, vals) -> None:
    unsupported = collect_unsupported_rules(norms, vals)
    if not unsupported: return
    log_warn_item(logger, f"Contrato possui regras declaradas que não serão executadas | total={len(unsupported)}")
    for msg in unsupported: log_warn_detail(logger, msg)

def _log_compiled_rule_summary(compiled_norms, compiled_vals) -> int:
    executable_vals = len([item for item in compiled_vals if getattr(item, "invalid_expr", None) is not None])
    logger.debug(
        "Regras compiladas | normalização=%s | validação_total=%s | validação_executável=%s",
        len(compiled_norms),
        len(compiled_vals),
        executable_vals,
    )
    return executable_vals

def _merge_rule_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, count in source.items():
        if count: target[key] = target.get(key, 0) + int(count)

def _discard_local_parquet_data(ctx) -> None:
    data_dir = Path(getattr(ctx, "output_dir")) / "data"
    if data_dir.exists():
        shutil.rmtree(data_dir)
        log_warn_detail(logger, f"Arquivos Parquet locais descartados porque foram encontrados erros | dir={data_dir}")           
            
def _process_chunk(*, df, chunk_number: int, ctx, schema_arrow, compiled_norms, compiled_vals,
                   executable_val_count: int, state: EngineState, timers: EngineTimers,
                   max_invalid_samples: int) -> None:
    state.rows_total += df.height
    logger.debug("Chunk %s | linhas=%s | acumulado=%s | validações=%s", chunk_number, format_int(df.height), format_int(state.rows_total), executable_val_count)

    df = _prepare_input_df(df, ctx)

    t0 = time.perf_counter()
    df, unsupported_norms = apply_normalizations(df, compiled_norms)
    timers.normalization += time.perf_counter() - t0
    if unsupported_norms:
        raise RuntimeError(f"Regra de normalização não suportada por Polars. unsupported_norms={unsupported_norms}")

    t0 = time.perf_counter()
    if schema_arrow:
        df = _enforce_schema_arrow_polars(df, schema_arrow=schema_arrow)
    timers.schema += time.perf_counter() - t0

    t0 = time.perf_counter()
    result = validate_chunk(df, compiled_vals)
    timers.validation += time.perf_counter() - t0

    valid_df, invalid_df, warning_df = result.valid_df, result.invalid_df, result.warning_df

    had_errors_before_chunk = state.rows_invalid > 0

    state.rows_valid += valid_df.height
    state.rows_invalid += result.invalid_count
    state.rows_warning += result.warning_count
    _merge_rule_counts(state.validation_stats, result.rule_counts)

    fired_rules_count = len([k for k, v in result.rule_counts.items() if v])
    log_detail(logger, f"validação | válidas={format_int(valid_df.height)} | inválidas={format_int(result.invalid_count)} | warnings={format_int(result.warning_count)} | regras_disparadas={fired_rules_count}")

    if invalid_df is not None and invalid_df.height > 0:
        first_error_detected = not had_errors_before_chunk

        _append_samples(
            sample_df=invalid_df,
            flags_df=result.invalid_flags_df,
            compiled_vals=compiled_vals,
            sample_rows=state.invalid_sample_rows,
            max_samples=max_invalid_samples,
            chunk_number=chunk_number,
            wanted_level="error",
            marker_column="__row_invalid__",
        )

        logger.debug(
            logger,
            f"inválidos  | chunk={format_int(invalid_df.height)} | acumulado={format_int(state.rows_invalid)} | amostra={format_int(len(state.invalid_sample_rows))}/{format_int(max_invalid_samples)}"
        )

        if first_error_detected:
            _discard_local_parquet_data(ctx)
            state.parquet_files = 0
            log_warn_detail(logger, "Publicação bloqueada: foram encontrados erros de validação")

        if max_invalid_samples > 0 and len(state.invalid_sample_rows) >= max_invalid_samples:
            state.invalid_samples_truncated = True
            state.stopped_on_invalid_sample_limit = True
            log_warn_detail(logger, f"Limite de amostra de inválidos atingido | limite={format_int(max_invalid_samples)} | processamento interrompido")
            return

    if warning_df is not None and warning_df.height > 0:
        _append_samples(
            sample_df=warning_df,
            flags_df=result.warning_flags_df,
            compiled_vals=compiled_vals,
            sample_rows=state.warning_sample_rows,
            max_samples=max_invalid_samples,
            chunk_number=chunk_number,
            wanted_level="warning",
            marker_column="__row_warning__",
        )

        log_warn_detail(
            logger,
            f"warnings   | chunk={format_int(warning_df.height)} | acumulado={format_int(state.rows_warning)} | amostra={format_int(len(state.warning_sample_rows))}/{format_int(max_invalid_samples)}"
        )

        if max_invalid_samples > 0 and len(state.warning_sample_rows) >= max_invalid_samples:
            state.warning_samples_truncated = True
            log_warn_detail(logger, f"Limite de amostra de warnings atingido | limite={format_int(max_invalid_samples)} | validação continuará sem novas amostras")

    if state.rows_invalid == 0:
        t0 = time.perf_counter()
        written = _write_partitioned_parquet(valid_df, ctx, chunk_number)
        timers.parquet_write += time.perf_counter() - t0
        state.parquet_files += written

        if written:
            log_detail(logger, f"gravação  | parquet_files={format_int(state.parquet_files)}")
    else:
        log_warn_detail(logger, "gravação  | ignorada porque já existem erros na carga")            
            

def _finalize_result(*, state: EngineState, timers: EngineTimers,
                     invalid_samples_file: str | None, warning_samples_file: str | None) -> EngineResult:
    total = time.perf_counter() - timers.started_at

    log_item(logger, f"Linhas processadas: {format_int(state.rows_total)}")
    log_item(logger, f"Linhas válidas: {format_int(state.rows_valid)}")
    log_item(logger, f"Linhas inválidas: {format_int(state.rows_invalid)}")
    log_item(logger, f"Linhas com warning: {format_int(state.rows_warning)}")

    if invalid_samples_file:
        log_warn_item(logger, f"Amostras de erro: {invalid_samples_file}")
    if warning_samples_file:
        log_warn_item(logger, f"Amostras de warning: {warning_samples_file}")

    if state.rows_invalid > 0:
        log_warn_last(logger, "Resultado: dados não publicados por erros de validação")
    elif state.rows_warning > 0:
        log_warn_last(logger, "Resultado: dados válidos com warnings")
    else:
        log_last(logger, "Resultado: dados válidos")

    return EngineResult(
        ok=True,        rows_total=state.rows_total,        rows_valid=state.rows_valid,        rows_invalid=state.rows_invalid,        rows_warning=state.rows_warning,        parquet_files=state.parquet_files,
        invalid_samples_file=invalid_samples_file, invalid_samples_count=len(state.invalid_sample_rows), invalid_samples_truncated=state.invalid_samples_truncated, stopped_on_invalid_sample_limit=state.stopped_on_invalid_sample_limit,
        warning_samples_file=warning_samples_file, warning_samples_count=len(state.warning_sample_rows), warning_samples_truncated=state.warning_samples_truncated, timings={
            "total": total,
            "normalizacao": timers.normalization,
            "schema": timers.schema,
            "validacao": timers.validation,
            "escrita_parquet": timers.parquet_write,
        },
        validation_stats=dict(sorted(state.validation_stats.items(), key=lambda x: x[1], reverse=True)),
    )

def validate_and_write(df_chunks: Any, ingest_rules, schema_arrow, ctx) -> EngineResult:
    timers, state = EngineTimers(started_at=time.perf_counter()), EngineState()

    log_section(logger, "VALIDAÇÃO")
    log_item(logger, "Normalizando e validando os dados")

    norms, vals = _rules_from_ingest(ingest_rules)
    compiled_norms, compiled_vals = compile_normalizations(norms), compile_validations(vals)
    executable_val_count = _log_compiled_rule_summary(compiled_norms, compiled_vals)
    _log_unsupported_rules(norms, vals)

    max_invalid_samples = _resolve_max_invalid_samples(ctx)
    expected_columns = _schema_arrow_field_names(schema_arrow)
    chunk_rows = getattr(ctx, "engine_chunk_rows", None)

    for chunk_number, df in enumerate(iter_polars_chunks(df_chunks, chunk_rows=chunk_rows, expected_columns=expected_columns), start=1):
        chunk_started = time.perf_counter()
        before_valid, before_invalid, before_warning = state.rows_valid, state.rows_invalid, state.rows_warning
        before_parquet_files = state.parquet_files
        log_item(logger, f"Lote {format_int(chunk_number)} iniciado | linhas={format_int(df.height)} | acumulado_previsto={format_int(state.rows_total + df.height)}")

        _process_chunk(df=df, chunk_number=chunk_number, ctx=ctx, schema_arrow=schema_arrow,
                       compiled_norms=compiled_norms, compiled_vals=compiled_vals,
                       executable_val_count=executable_val_count, state=state, timers=timers,
                       max_invalid_samples=max_invalid_samples)

        elapsed = time.perf_counter() - chunk_started
        log_item(
            logger,
            "Lote %s concluído | válidas=%s | inválidas=%s | warnings=%s | parquet_files=%s | acumulado=%s | tempo=%s" % (
                format_int(chunk_number),
                format_int(state.rows_valid - before_valid),
                format_int(state.rows_invalid - before_invalid),
                format_int(state.rows_warning - before_warning),
                format_int(state.parquet_files - before_parquet_files),
                format_int(state.rows_total),
                format_seconds(elapsed),
            ),
        )
        if state.stopped_on_invalid_sample_limit:
            break

    invalid_samples_file = _write_samples_json(
        ctx=ctx, path=_samples_file_path(ctx, "invalid_samples.json"), sample_rows=state.invalid_sample_rows,
        rows_total=state.rows_total, rows_count=state.rows_invalid, max_samples=max_invalid_samples,
        truncated=state.invalid_samples_truncated, validation_stats=state.validation_stats,
        kind="invalid", level_prefix="error:",
    )
    warning_samples_file = _write_samples_json(
        ctx=ctx, path=_samples_file_path(ctx, "warning_samples.json"), sample_rows=state.warning_sample_rows,
        rows_total=state.rows_total, rows_count=state.rows_warning, max_samples=max_invalid_samples,
        truncated=state.warning_samples_truncated, validation_stats=state.validation_stats,
        kind="warning", level_prefix="warning:",
    )
    return _finalize_result(state=state, timers=timers, invalid_samples_file=invalid_samples_file,
                            warning_samples_file=warning_samples_file)
    
def _write_partitioned_parquet(df: pl.DataFrame, ctx, chunk_number: int) -> int:
    if df.height == 0: return 0

    spec = PartitionSpec.from_contract(getattr(ctx, "contract", None))
    base_dir = Path(ctx.output_dir) / "data"
    written = 0

    for seq, values in enumerate(spec.unique_partition_values_from_df(df, ctx), start=1):
        part_df = df

        for field in spec.fields:
            if field.source_from == "ctx": continue
            part_df = part_df.filter(pl.col(field.source) == values[field.name])

        if part_df.height == 0: continue

        part_dir = spec.build_path(base_dir=base_dir, values=values)
        part_dir.mkdir(parents=True, exist_ok=True)

        part_df.write_parquet(part_dir / f"part-{chunk_number:06}-{seq:03}.parquet", compression="zstd")
        written += 1

    return written


def _prepare_input_df(df: pl.DataFrame, ctx) -> pl.DataFrame:
    spec = PartitionSpec.from_contract(getattr(ctx, "contract", None))

    updates = []

    for field in spec.fields:
        if field.source_from != "ctx": continue
        if field.source in df.columns: continue

        if not hasattr(ctx, field.source):
            raise ValueError(
                f"Contexto não possui valor para partição '{field.name}' "
                f"(source='{field.source}')"
            )

        updates.append(pl.lit(getattr(ctx, field.source)).alias(field.source))

    return df.with_columns(updates) if updates else df