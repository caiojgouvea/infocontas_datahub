from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from minio.deleteobjects import DeleteObject
from pathlib import Path
from typing import List

import urllib3
from minio import Minio


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    base_prefix: str = ""


class MinioClient:
    """
    Cliente concreto de MinIO para o projeto.

    Regras fixas do ambiente:
    - sempre HTTPS
    - certificado autoassinado / CA não reconhecida
    - não validar certificado nem hostname
    """

    def __init__(self, cfg: MinioConfig):
        self.cfg = cfg

        http_client = urllib3.PoolManager(
            cert_reqs="CERT_NONE",
            assert_hostname=False,
            maxsize=10,
            num_pools=10,
        )

        self.client = Minio(
            endpoint=cfg.endpoint,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            secure=True,
            http_client=http_client,
        )

    # ------------------------------------------------------------------
    # Helpers de caminho remoto
    # ------------------------------------------------------------------
    def join(self, *parts: str) -> str:
        clean: List[str] = []

        base = (self.cfg.base_prefix or "").strip("/")
        if base:
            clean.append(base)

        for part in parts:
            p = (part or "").strip().strip("/")
            if p:
                clean.append(p)

        return "/".join(clean)

    def dataset_prefix(self, *, dataset: str, version: str) -> str:
        return self.join(dataset, version)

    def data_prefix(self, *, dataset: str, version: str, tc: str, ano: int | str) -> str:
        return self.join(dataset, version, "data", f"tc={tc}", f"ano={ano}")

    def contract_prefix(self, *, dataset: str, version: str) -> str:
        return self.join(dataset, version, "contract")

    # ------------------------------------------------------------------
    # Listagem
    # ------------------------------------------------------------------
    def list_keys(
        self,
        prefix: str = "",
        *,
        recursive: bool = True,
    ) -> List[str]:
        full_prefix = self.join(prefix)

        objs = self.client.list_objects(
            self.cfg.bucket,
            prefix=full_prefix,
            recursive=recursive,
        )
        return [obj.object_name for obj in objs]

    def list_relative_keys(
        self,
        prefix: str = "",
        *,
        recursive: bool = True,
    ) -> List[str]:
        full_prefix = self.join(prefix).strip("/")
        keys = self.list_keys(prefix=prefix, recursive=recursive)

        if not full_prefix:
            return keys

        out: List[str] = []
        for key in keys:
            rel = key[len(full_prefix):].lstrip("/") if key.startswith(full_prefix) else key
            out.append(rel)
        return out

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------
    def download_file(
        self,
        object_name: str,
        local_path: str | Path,
    ) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        remote_name = self.join(object_name)

        self.client.fget_object(
            self.cfg.bucket,
            remote_name,
            str(local_path),
        )
        return local_path

    def _download_one(
        self,
        *,
        object_name: str,
        dest: Path,
    ) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)

        self.client.fget_object(
            self.cfg.bucket,
            object_name,
            str(dest),
        )
        return dest

    def download_prefix(
        self,
        remote_prefix: str,
        local_dir: str | Path,
        *,
        recursive: bool = True,
        max_workers: int = 6,
    ) -> List[Path]:
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        full_prefix = self.join(remote_prefix).strip("/")
        downloaded: List[Path] = []

        objs = self.client.list_objects(
            self.cfg.bucket,
            prefix=full_prefix,
            recursive=recursive,
        )

        tasks: List[tuple[str, Path]] = []

        for obj in objs:
            name = obj.object_name

            if not name or name.endswith("/"):
                continue

            rel = name[len(full_prefix):].lstrip("/") if full_prefix and name.startswith(full_prefix) else name
            if not rel:
                continue

            dest = local_dir / rel
            tasks.append((name, dest))

        if not tasks:
            return downloaded

        workers = max(1, int(max_workers))

        if workers == 1 or len(tasks) == 1:
            for name, dest in tasks:
                downloaded.append(self._download_one(object_name=name, dest=dest))
            return downloaded

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(self._download_one, object_name=name, dest=dest): (name, dest)
                for name, dest in tasks
            }

            for future in as_completed(future_map):
                downloaded.append(future.result())

        downloaded.sort(key=lambda p: str(p).lower())
        return downloaded

    def download_contract(
        self,
        *,
        dataset: str,
        version: str,
        local_dir: str | Path,
        max_workers: int = 6,
    ) -> List[Path]:
        return self.download_prefix(
            remote_prefix=self.contract_prefix(dataset=dataset, version=version),
            local_dir=local_dir,
            recursive=True,
            max_workers=max_workers,
        )

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------
    def upload_file(
        self,
        local_path: str | Path,
        object_name: str,
    ) -> None:
        local_path = Path(local_path)
        remote_name = self.join(object_name)

        self.client.fput_object(
            self.cfg.bucket,
            remote_name,
            str(local_path),
        )

    def upload_tree(
        self,
        local_dir: str | Path,
        *,
        remote_prefix: str = "",
    ) -> List[str]:
        local_dir = Path(local_dir)
        uploaded: List[str] = []

        prefix = self.join(remote_prefix).strip("/")

        for p in local_dir.rglob("*"):
            if p.is_dir():
                continue

            rel = p.relative_to(local_dir).as_posix()
            object_name = f"{prefix}/{rel}" if prefix else rel

            self.client.fput_object(
                self.cfg.bucket,
                object_name,
                str(p),
            )
            uploaded.append(object_name)

        return uploaded

    # ------------------------------------------------------------------
    # Remoção
    # ------------------------------------------------------------------
    def delete_prefix(self, remote_prefix: str) -> int:
        keys = self.list_keys(prefix=remote_prefix, recursive=True)
        if not keys:
            return 0

        objects = [DeleteObject(key) for key in keys]

        errors = []
        for err in self.client.remove_objects(self.cfg.bucket, objects):
            errors.append(err)

        if errors:
            first = errors[0]
            raise RuntimeError(
                f"Falha ao remover objetos no prefixo {remote_prefix!r}: {first}"
            )

        return len(keys)