from app.AppConfig import AppConfig
from domain.Datasets import Datasets
from infra.storage.MinioStorage import MinioStorage
from project_config.Config import build_dest_minio_client_from_env
from services.ContractService import ContractService
from services.ExtractService import ExtractService
from services.PublishService import PublishService
from usecases.DownloadDataset import DownloadDataset
from usecases.ExportAndPublishDataset import ExportAndPublishDataset

class Hub:
    def __init__(self, *, export_uc, download_uc):
        self.export_uc = export_uc
        self.download_uc = download_uc

    @classmethod
    def from_app_config(cls, *, app_config: AppConfig):
        registry = Datasets()
        storage = MinioStorage()
        contract = ContractService(cache_root=app_config.contract_dir)
        extract = ExtractService(batch_rows=app_config.extract_batch_rows, driver_arraysize=10_000)
        publish = PublishService(storage=storage)

        dest_client = build_dest_minio_client_from_env()
        dest_storage = MinioStorage(client=dest_client) if dest_client else None

        return cls(
            export_uc=ExportAndPublishDataset(registry=registry, extract_service=extract, publish_service=publish,contract_service=contract, app_config=app_config,),
            download_uc=DownloadDataset(registry=registry, storage=storage, app_config=app_config,contract_service=contract, dest_storage=dest_storage,),)

    def execute_command(self, *, command: str, **k):
        if command == "export":
            return self.export_uc.execute(dataset=k["dataset"], version=k["version"], ano=k["ano"])
        if command == "download":
            return self.download_uc.execute(dataset=k["dataset"], version=k["version"], ano=k["ano"], tc=k["tc"], dest=k.get("dest", "local"))
        if command == "download-all-years":
            return self.download_uc.execute_all_years(dataset=k["dataset"], version=k["version"], tc=k["tc"], dest=k.get("dest", "local"))
        raise ValueError(f"Comando inválido: {command}")

    def get_success_message(self, command: str) -> str:
        messages = {
            "export": "Pipeline executado com sucesso",
            "download": "Download executado com sucesso",
            "download-all-years": "Download de todos os exercícios executado com sucesso",
        }
        if command not in messages:
            raise ValueError(f"Comando inválido: {command}")
        return messages[command]