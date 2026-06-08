from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from project_config.Config import init_env, resolve_app_path


def _env_str(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Variável de ambiente obrigatória não informada: {name}")
    return "" if value is None else str(value).strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError as exc:
        raise ValueError(
            f"Variável de ambiente {name} deve ser inteira. Valor recebido: {raw!r}"
        ) from exc


@dataclass(frozen=True)
class AppConfig:
    producer_tc: str
    extract_batch_rows: int
    engine_chunk_rows: int
    max_invalid_samples: int
    output_dir: Path
    log_dir: Path
    contract_dir: Path
    download_dir: Path
    download_max_workers: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        init_env(require_dotenv=False, override=True)

        return cls(
            producer_tc=_env_str("MINIO_ACCESS_KEY", required=True).lower(),
            extract_batch_rows=_env_int("EXTRACT_BATCH_ROWS", 250_000),
            engine_chunk_rows=_env_int("ENGINE_CHUNK_ROWS", 250_000),
            max_invalid_samples=_env_int("MAX_INVALID_SAMPLES", 100),
            output_dir=resolve_app_path(_env_str("OUTPUT_DIR", "out")),
            log_dir=resolve_app_path(_env_str("LOG_DIR", "logs")),
            contract_dir=resolve_app_path(_env_str("CONTRACT_DIR", "contratos")),
            download_dir=resolve_app_path(_env_str("DOWNLOAD_DIR", "downloads")),
            download_max_workers=_env_int("DOWNLOAD_MAX_WORKERS", 6),
        )