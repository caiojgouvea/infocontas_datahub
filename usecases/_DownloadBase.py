from pathlib import Path
from app.AppConfig import AppConfig
from domain.Datasets import Datasets
from domain.PartitionSpec import PartitionSpec
from infra.storage.Storage import Storage
from services.ContractService import ContractService

class DownloadBase:
    def __init__(self, *, registry: Datasets, storage: Storage, app_config: AppConfig, contract_service: ContractService):
        self.registry = registry
        self.storage = storage
        self.app_config = app_config
        self.contract_service = contract_service

    def _norm_text(self, value, field: str) -> str:
        text = (value or "").strip().lower()
        if not text:
            raise ValueError(f"{field} não informado")
        return text

    def _norm_int(self, value, field: str) -> int:
        try:
            return int(value)
        except Exception as e:
            raise ValueError(f"{field} inválido: {value!r}") from e

    def _resolve_download_target(self, *, dataset: str, version: str, values: dict, drop_last_n: int | None = None):
        registry_spec = self.registry.get(dataset, version, validate_sql=False)
        contract = self.contract_service.load_and_validate(dataset=dataset, version=version)
        spec = PartitionSpec.from_contract(contract)

        if drop_last_n is None:
            drop_last_n = spec.ctx_scope_drop_last_n()

        remote_prefix = spec.build_prefix(base_prefix=f"{registry_spec.storage_prefix}/data",values=values,drop_last_n=drop_last_n,)

        local_dir = spec.build_path(base_dir=Path(self.app_config.download_dir) / dataset / version / "data",values=values,drop_last_n=drop_last_n,)

        return remote_prefix, local_dir

    def _download(self, *, remote_prefix: str, local_dir: Path):
        objs = list(self.storage.list_objects(remote_prefix=remote_prefix) or [])
        files = [o for o in objs if self._is_real_file_object(o)]

        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado no MinIO para remote_prefix='{remote_prefix}'")

        local_dir.mkdir(parents=True, exist_ok=True)

        workers = int(self.app_config.download_max_workers or 6)
        self.storage.download_prefix(remote_prefix=remote_prefix, local_dir=local_dir, max_workers=workers)

        return files, workers

    @staticmethod
    def _is_real_file_object(obj) -> bool:
        if obj is None:
            return False

        if getattr(obj, "is_dir", False):
            return False

        if isinstance(obj, str):
            return not obj.endswith("/")

        name = getattr(obj, "object_name", None)
        return not (isinstance(name, str) and name.endswith("/"))