from __future__ import annotations

import logging
from pathlib import Path

from app.logger import log_item, log_last, log_section
from domain.PartitionSpec import PartitionSpec
from infra.storage.MinioStorage import MinioStorage

logger = logging.getLogger("publish")


class PublishService:
    def __init__(self, *, storage: MinioStorage):
        self.storage = storage

    def publish(self, *, local_dir: str | Path, dataset: str, version: str, contract, **partition_values):
        local_dir = Path(local_dir)
        data_dir = local_dir / "data"

        if not data_dir.exists():
            raise ValueError(f"Diretório de dados não existe: {data_dir}")

        files = list(data_dir.rglob("*.parquet"))
        if not files:
            raise ValueError(f"Nenhum arquivo parquet encontrado em: {data_dir}")

        spec = PartitionSpec.from_contract(contract)
        drop_last_n = spec.ctx_scope_drop_last_n()

        clean_prefix = spec.build_prefix(
            base_prefix=f"{dataset}/{version}/data",
            values=partition_values,
            drop_last_n=drop_last_n,
        )

        upload_prefix = f"{dataset}/{version}/data"

        log_section(logger, "PUBLISH")
        log_item(logger, f"Removendo dados anteriores | prefix={clean_prefix}")
        self.storage.remove_prefix(remote_prefix=clean_prefix)

        log_last(logger, f"Enviando diretório | local={data_dir} | remoto={upload_prefix} | files={len(files)}")
        self.storage.upload_dir(local_dir=data_dir, remote_prefix=upload_prefix)

        return {
            "files": len(files),
            "clean_prefix": clean_prefix,
            "upload_prefix": upload_prefix,
        }