from dataclasses import asdict, dataclass
from usecases._DownloadBase import DownloadBase

@dataclass(frozen=True)
class DownloadResult:
    dataset: str
    version: str
    tc: str
    remote_prefix: str
    local_dir: str
    objects_found: int
    download_max_workers: int
    status: str
    ano: int | None = None

    def to_dict(self):
        return asdict(self)

class DownloadDataset(DownloadBase):
    def execute(self, *, dataset: str, version: str, ano: int, tc: str):
        ds = self._norm_text(dataset, "dataset")
        ver = self._norm_text(version, "version")
        tc = self._norm_text(tc, "tc")
        ano = self._norm_int(ano, "ano")

        return self._execute( dataset=ds, version=ver, tc=tc, ano=ano, values={"tc": tc, "ano": ano}, drop_last_n=None, status="downloaded",
            not_found_msg=f"Nenhum dado publicado encontrado para dataset='{ds}', version='{ver}', tc='{tc}', ano={ano}.",)

    def execute_all_years(self, *, dataset: str, version: str, tc: str):
        ds = self._norm_text(dataset, "dataset")
        ver = self._norm_text(version, "version")
        tc = self._norm_text(tc, "tc")

        return self._execute(dataset=ds,version=ver,tc=tc,ano=None,values={"tc": tc},
            drop_last_n=self._drop_all_partitions_after_tc(dataset=ds, version=ver),
            status="downloaded_all_years", not_found_msg=f"Nenhum dado publicado encontrado para dataset='{ds}', version='{ver}', tc='{tc}' em qualquer ano.",)

    def _drop_all_partitions_after_tc(self, *, dataset: str, version: str) -> int:
        contract = self.contract_service.load_and_validate(dataset=dataset, version=version)
        spec = self._partition_spec(contract)

        tc_index = next((i for i, f in enumerate(spec.fields) if f.name == "tc"), None)
        if tc_index is None:
            raise ValueError("Contrato não possui partição 'tc'")

        keep_count = tc_index + 1
        return len(spec.fields) - keep_count

    @staticmethod
    def _partition_spec(contract):
        from domain.PartitionSpec import PartitionSpec
        return PartitionSpec.from_contract(contract)

    def _execute(self,*,dataset: str,version: str,tc: str,ano: int | None,values: dict,drop_last_n: int | None,status: str,not_found_msg: str,):
        remote_prefix, local_dir = self._resolve_download_target(dataset=dataset,version=version,values=values,drop_last_n=drop_last_n,)

        try:
            objs, workers = self._download(remote_prefix=remote_prefix, local_dir=local_dir)
        except FileNotFoundError as e:
            raise FileNotFoundError(not_found_msg) from e

        return DownloadResult(dataset=dataset,version=version,tc=tc,ano=ano,remote_prefix=remote_prefix,local_dir=str(local_dir),objects_found=len(objs),download_max_workers=workers,status=status,).to_dict()