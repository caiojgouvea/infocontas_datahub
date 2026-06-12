from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

from project_config.Config import (
    env_int_optional,
    env_optional,
    env_required,
    init_env,
)


@dataclass(frozen=True)
class DatabaseConfig:
    dialect: str
    driver: str
    host: Optional[str]
    port: Optional[int]
    database: Optional[str]
    username: Optional[str]
    password: Optional[str]
    query: Dict[str, str]
    schema: Optional[str]


@dataclass(frozen=True)
class ImpalaConfig:
    host: str
    port: int
    database: str
    username: Optional[str]
    password: Optional[str]
    auth_mechanism: str
    use_ssl: bool
    kerberos_service_name: Optional[str]
    timeout: Optional[int]


_DEFAULT_DRIVER = {
    "mssql": "pyodbc",
    "postgresql": "psycopg",
    "mysql": "pymysql",
    "oracle": "oracledb",
    "sqlite": "",
    "impala": "impyla",
}

_DEFAULT_PORT = {
    "mssql": 1433,
    "postgresql": 5432,
    "mysql": 3306,
    "oracle": 1521,
    "impala": 21050,
}


def parse_query_kv(raw: Optional[str]) -> Dict[str, str]:
    """Converte DB_QUERY no formato 'a=1&b=2&flag' para dict."""
    query: Dict[str, str] = {}

    if not raw:
        return query

    for part in str(raw).split("&"):
        part = part.strip()

        if not part:
            continue

        if "=" in part:
            key, value = part.split("=", 1)
            query[key.strip()] = value.strip()
        else:
            query[part] = ""

    return query


def _env_bool_optional(name: str, default: bool = False) -> bool:
    raw = env_optional(name)

    if raw is None or str(raw).strip() == "":
        return default

    return str(raw).strip().lower() in {"1", "true", "yes", "y", "sim", "s"}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "y", "sim", "s"}


def database_dialect() -> str:
    init_env(require_dotenv=False, override=True)

    return env_required("DB_DIALECT").lower().strip()


def database_config_from_env() -> DatabaseConfig:
    init_env(require_dotenv=False, override=True)

    dialect = env_required("DB_DIALECT").lower().strip()
    schema = env_optional("DB_SCHEMA")

    if dialect == "sqlite":
        path = env_optional("DB_SQLITE_PATH") or env_optional("DB_NAME") or "./db.sqlite"

        return DatabaseConfig(
            dialect="sqlite",
            driver="",
            host=None,
            port=None,
            database=path,
            username=None,
            password=None,
            query={},
            schema=schema,
        )

    host = env_required("DB_HOST")
    database = env_required("DB_NAME")

    trusted_connection = _env_bool_optional("DB_TRUSTED_CONNECTION", False)

    if trusted_connection:
        username = None
        password = None
    else:
        username = env_required("DB_USER")
        password = env_required("DB_PASSWORD")

    driver = env_optional("DB_DRIVER", _DEFAULT_DRIVER.get(dialect))

    if not driver:
        raise RuntimeError(
            f"DB_DRIVER não definido e não há default para DB_DIALECT={dialect!r}"
        )

    port = env_int_optional("DB_PORT")

    if port is None:
        port = _DEFAULT_PORT.get(dialect)

    query = parse_query_kv(env_optional("DB_QUERY"))

    if dialect == "mssql" and driver == "pyodbc":
        query.setdefault(
            "driver",
            env_optional("DB_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
        )
        query.setdefault("Encrypt", env_optional("DB_ENCRYPT", "yes"))
        query.setdefault(
            "TrustServerCertificate",
            env_optional("DB_TRUST_SERVER_CERTIFICATE", "yes"),
        )

        if trusted_connection:
            query.setdefault("Trusted_Connection", "yes")

    if dialect == "postgresql":
        sslmode = env_optional("DB_SSLMODE")

        if sslmode:
            query.setdefault("sslmode", sslmode)

    if dialect == "oracle":
        service_name = env_optional("DB_SERVICE_NAME")

        if service_name:
            query.setdefault("service_name", service_name)

    if dialect == "impala":
        query.setdefault(
            "auth_mechanism",
            env_optional("DB_AUTH_MECHANISM", "NOSASL"),
        )

        use_ssl = env_optional("DB_USE_SSL")

        if use_ssl:
            query.setdefault("use_ssl", use_ssl)

    return DatabaseConfig(
        dialect=dialect,
        driver=driver,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        query=query,
        schema=schema,
    )


def impala_config_from_env() -> ImpalaConfig:
    init_env(require_dotenv=False, override=True)

    query = parse_query_kv(env_optional("DB_QUERY"))

    host = env_required("DB_HOST")
    database = env_optional("DB_NAME", "default") or "default"

    port = env_int_optional("DB_PORT")

    if port is None:
        port = 21050

    username = env_optional("DB_USER")
    password = env_optional("DB_PASSWORD")

    auth_mechanism = (
        env_optional("DB_AUTH_MECHANISM")
        or query.get("auth_mechanism")
        or "NOSASL"
    )

    use_ssl_raw = env_optional("DB_USE_SSL")

    if use_ssl_raw is None:
        use_ssl = _as_bool(query.get("use_ssl"), False)
    else:
        use_ssl = _env_bool_optional("DB_USE_SSL", False)

    kerberos_service_name = (
        env_optional("DB_KERBEROS_SERVICE_NAME")
        or query.get("kerberos_service_name")
    )

    timeout = env_int_optional("DB_TIMEOUT")

    return ImpalaConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        auth_mechanism=str(auth_mechanism).strip().upper(),
        use_ssl=use_ssl,
        kerberos_service_name=kerberos_service_name,
        timeout=timeout,
    )


def create_impala_connection():
    """
    Cria conexão direta com Impala via impyla.

    Mantemos Impala fora do create_engine_sa porque, sem ambiente real para teste,
    é mais seguro usar a API direta do driver do que depender do dialeto SQLAlchemy.
    """
    try:
        from impala.dbapi import connect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Driver do Impala não encontrado. Instale a dependência 'impyla'."
        ) from exc

    cfg = impala_config_from_env()

    kwargs: dict[str, Any] = {
        "host": cfg.host,
        "port": cfg.port,
        "database": cfg.database,
        "auth_mechanism": cfg.auth_mechanism,
        "use_ssl": cfg.use_ssl,
    }

    if cfg.username:
        kwargs["user"] = cfg.username

    if cfg.password:
        kwargs["password"] = cfg.password

    if cfg.kerberos_service_name:
        kwargs["kerberos_service_name"] = cfg.kerberos_service_name

    if cfg.timeout is not None:
        kwargs["timeout"] = cfg.timeout

    return connect(**kwargs)


def sqlalchemy_url_from_config(cfg: Optional[DatabaseConfig] = None) -> URL:
    cfg = cfg or database_config_from_env()

    if cfg.dialect == "sqlite":
        return URL.create("sqlite", database=cfg.database or "./db.sqlite")

    dialect_driver = f"{cfg.dialect}+{cfg.driver}"

    database = cfg.database

    if cfg.dialect == "oracle" and cfg.query.get("service_name"):
        database = None

    return URL.create(
        dialect_driver,
        username=cfg.username,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        database=database,
        query=cfg.query or None,
    )


def create_engine_sa(
    *,
    pool_pre_ping: bool = True,
    pool_recycle: int = 3600,
    echo: bool = False,
    connect_args: Optional[Dict[str, Any]] = None,
) -> Engine:
    url = sqlalchemy_url_from_config()

    return create_engine(
        url,
        pool_pre_ping=pool_pre_ping,
        pool_recycle=pool_recycle,
        echo=echo,
        connect_args=connect_args or {},
    )


def database_schema(default_by_dialect: bool = True) -> str:
    init_env(require_dotenv=False, override=True)

    schema = env_optional("DB_SCHEMA")

    if schema:
        return schema

    if not default_by_dialect:
        return "dbo"

    dialect = env_required("DB_DIALECT").lower().strip()

    if dialect == "postgresql":
        return "public"

    if dialect == "impala":
        return "default"

    return "dbo"