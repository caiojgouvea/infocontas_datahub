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


_DEFAULT_DRIVER = {
    "mssql": "pyodbc",
    "postgresql": "psycopg",
    "mysql": "pymysql",
    "oracle": "oracledb",
    "sqlite": "",
}

_DEFAULT_PORT = {
    "mssql": 1433,
    "postgresql": 5432,
    "mysql": 3306,
    "oracle": 1521,
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
    username = env_required("DB_USER")
    password = env_required("DB_PASSWORD")

    driver = env_optional("DB_DRIVER", _DEFAULT_DRIVER.get(dialect))
    if not driver:
        raise RuntimeError(f"DB_DRIVER não definido e não há default para DB_DIALECT={dialect!r}")

    port = env_int_optional("DB_PORT")
    if port is None:
        port = _DEFAULT_PORT.get(dialect)

    query = parse_query_kv(env_optional("DB_QUERY"))

    if dialect == "mssql" and driver == "pyodbc":
        query.setdefault("driver", env_optional("DB_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"))
        query.setdefault("Encrypt", env_optional("DB_ENCRYPT", "yes"))
        query.setdefault(
            "TrustServerCertificate",
            env_optional("DB_TRUST_SERVER_CERTIFICATE", "yes"),
        )

    if dialect == "postgresql":
        sslmode = env_optional("DB_SSLMODE")
        if sslmode:
            query.setdefault("sslmode", sslmode)

    if dialect == "oracle":
        service_name = env_optional("DB_SERVICE_NAME")
        if service_name:
            query.setdefault("service_name", service_name)

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
    return "public" if dialect == "postgresql" else "dbo"