from __future__ import annotations

import logging
from pathlib import Path

from app.logger import log_item, log_last, log_section, log_warn_item
from domain.Contract import Contract
from project_config.Config import build_minio_client_from_env

logger = logging.getLogger('contract')


class ContractService:
    REQUIRED_FILES = ('schema.arrow', 'ingest_rules.json')

    def __init__(self, cache_root: str | Path = '_contracts_cache', client=None):
        self.cache_root = Path(cache_root).resolve()
        self.client = client or build_minio_client_from_env(base_prefix='')

    def load_and_validate(self, *, dataset: str, version: str) -> Contract:
        self.cache_root.mkdir(parents=True, exist_ok=True)

        log_section(logger, 'CONTRACT')
        log_item(logger, f'Atualizando cache local | dataset={dataset} | version={version}')
        contract_dir = self._refresh_contract_locally(dataset=dataset, version=version)

        log_item(logger, f'Carregando contrato | dir={contract_dir}')
        contract = Contract.from_local_dir(dataset=dataset, version=version, contract_dir=contract_dir)

        issues = contract.validate(fail_fast=False)
        if any(i.level == 'ERROR' for i in issues):
            resumo = Contract.summarize_issues(issues)
            raise ValueError(f"Validação do contrato falhou para dataset='{dataset}' version='{version}': {resumo}")

        log_last(logger, 'Contrato carregado e validado')
        return contract

    def _refresh_contract_locally(self, *, dataset: str, version: str) -> Path:
        if not dataset:
            raise ValueError('dataset vazio')
        if not version:
            raise ValueError('version vazia')

        contract_dir = (self.cache_root / dataset / version).resolve()
        remote_prefix = f'{dataset}/{version}/contract'

        if contract_dir.exists():
            for p in contract_dir.rglob('*'):
                if not p.is_file():
                    continue
                try:
                    p.unlink()
                except Exception as exc:
                    log_warn_item(logger, f'Falha ao remover arquivo antigo do cache | path={p} | erro={exc}')

        contract_dir.mkdir(parents=True, exist_ok=True)

        for i, filename in enumerate(self.REQUIRED_FILES, start=1):
            remote_key = f'{remote_prefix}/{filename}'.replace('//', '/')
            local_path = contract_dir / filename
            log_fn = log_last if i == len(self.REQUIRED_FILES) else log_item
            log_fn(logger, f'Baixando arquivo do contrato | remote={remote_key} | local={local_path}')
            self.client.download_file(remote_key, str(local_path))

        return contract_dir