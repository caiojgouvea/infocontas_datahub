from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from dotenv import load_dotenv  # type: ignore

if TYPE_CHECKING:
    from infra.storage.MinioClient import MinioClient


REQUIRED_ENV_VARS = (
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
)

OPTIONAL_ENV_DEFAULTS = {
    "PARQUET_CHUNK_ROWS": "250000",
    "MAX_INVALID_SAMPLES": "100",
}


def app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


DEFAULT_DOTENV = app_base_dir() / ".env"


def resolve_app_path(value: str | Path) -> Path:
    p = Path(value)
    if p.is_absolute():
        return p
    return (app_base_dir() / p).resolve()


def init_env(
    *,
    dotenv_path: Optional[str | Path] = None,
    require_dotenv: bool = False,
    override: bool = True,
) -> Optional[Path]:
    candidates: list[Path] = []

    if dotenv_path is not None:
        candidates.append(Path(dotenv_path).resolve())
    else:
        base_dir = app_base_dir()
        candidates.append((base_dir / ".env").resolve())
        candidates.append((Path.cwd() / ".env").resolve())

    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=p, override=override)
            return p

    if require_dotenv:
        raise RuntimeError(
            ".env não encontrado. Locais verificados: "
            + ", ".join(str(p) for p in candidates)
        )

    return None


def env_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return str(v).strip()


def env_optional(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip()


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(str(v).strip())
    except Exception as exc:
        raise RuntimeError(f"Variável {name} deve ser int. Valor recebido: {v!r}") from exc


def env_int_optional(name: str) -> Optional[int]:
    v = env_optional(name)
    if v is None:
        return None
    try:
        return int(v)
    except Exception as exc:
        raise RuntimeError(f"Variável {name} deve ser int. Valor recebido: {v!r}") from exc


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)

    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "sim", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "nao", "não", "off"}:
        return False

    raise RuntimeError(f"Variável {name} deve ser bool. Valor recebido: {v!r}")


def validate_env(*, apply_optional_defaults: bool = True) -> None:
    missing = [k for k in REQUIRED_ENV_VARS if not (os.getenv(k) or "").strip()]
    if missing:
        raise RuntimeError(
            "Variáveis de ambiente obrigatórias ausentes: " + ", ".join(missing)
        )

    if apply_optional_defaults:
        for k, v in OPTIONAL_ENV_DEFAULTS.items():
            if not (os.getenv(k) or "").strip():
                os.environ[k] = str(v)


def producer_tc() -> str:
    return env_required("MINIO_ACCESS_KEY")


def build_minio_client_from_env(*, base_prefix: str = "") -> "MinioClient":
    from infra.storage.MinioClient import MinioClient, MinioConfig

    init_env(require_dotenv=False, override=True)
    validate_env(apply_optional_defaults=True)

    endpoint = env_required("MINIO_ENDPOINT")
    bucket = env_required("MINIO_BUCKET")
    access_key = env_required("MINIO_ACCESS_KEY")
    secret_key = env_required("MINIO_SECRET_KEY")

    cfg = MinioConfig(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        base_prefix=base_prefix,
    )
    return MinioClient(cfg)