from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DatasetSpec:
    dataset: str
    version: str
    sql_path: Path
    storage_prefix: str

    @property
    def data_prefix(self) -> str:
        return f"{self.storage_prefix}/data"

    def validate(self, *, validate_sql: bool = True) -> None:
        ds, ver = self.dataset.strip().lower(), self.version.strip().lower()
        if not ds: raise ValueError("dataset não pode ser vazio")
        if not ver: raise ValueError("version não pode ser vazia")
        if not isinstance(self.sql_path, Path): raise TypeError("sql_path deve ser pathlib.Path")
        if validate_sql and not self.sql_path.exists(): raise FileNotFoundError(f"SQL não encontrado: {self.sql_path}")
        if self.storage_prefix.strip("/") != f"{ds}/{ver}":
            raise ValueError(f"storage_prefix inválido. Esperado '{ds}/{ver}', recebido: {self.storage_prefix!r}")


class Datasets:
    def __init__(self, base_sql_dir: str | Path = "consultas") -> None:
        self.base_sql_dir = Path(base_sql_dir)

    def get(self, dataset: str, version: str, *, validate_sql: bool = True) -> DatasetSpec:
        ds, ver = (dataset or "").strip().lower(), (version or "").strip().lower()
        if not ds: raise ValueError("dataset não informado")
        if not ver: raise ValueError("version não informada")

        spec = DatasetSpec(
            dataset=ds,
            version=ver,
            sql_path=self.base_sql_dir / ds / f"{ver}.sql",
            storage_prefix=f"{ds}/{ver}",
        )
        spec.validate(validate_sql=validate_sql)
        return spec