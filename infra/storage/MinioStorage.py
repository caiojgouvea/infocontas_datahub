from __future__ import annotations

from pathlib import Path

from project_config.Config import build_minio_client_from_env
from infra.storage.Storage import Storage


class MinioStorage(Storage):
    """
    Adapter da interface Storage para o cliente concreto de MinIO.
    """

    def __init__(self, *, client=None):
        self.client = client or build_minio_client_from_env(base_prefix="")

    def remove_prefix(self, *, remote_prefix: str) -> None:
        self.client.delete_prefix(remote_prefix)

    def upload_dir(self, *, local_dir: Path, remote_prefix: str) -> None:
        local_dir = Path(local_dir)

        if not local_dir.exists():
            raise FileNotFoundError(f"Diretório local não encontrado: {local_dir}")

        self.client.upload_tree(
            local_dir=local_dir,
            remote_prefix=remote_prefix,
        )

    def list_objects(self, *, remote_prefix: str) -> list[str]:
        return self.client.list_keys(prefix=remote_prefix, recursive=True)

    def download_prefix(
        self,
        *,
        remote_prefix: str,
        local_dir: Path,
        max_workers: int = 6,
    ) -> None:
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        self.client.download_prefix(
            remote_prefix=remote_prefix,
            local_dir=local_dir,
            recursive=True,
            max_workers=max_workers,
        )

    def download_file(self, *, remote_path: str, local_path: Path) -> Path:
        return self.client.download_file(
            object_name=remote_path,
            local_path=local_path,
        )