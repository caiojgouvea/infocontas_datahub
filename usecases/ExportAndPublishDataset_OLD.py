import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from app.AppConfig import AppConfig
from domain.Datasets import Datasets
from engine.Engine import validate_and_write
from services.ContractService import ContractService
from services.ExtractService import ExtractService
from services.PublishService import PublishService

@dataclass(frozen=True)
class RunContext:
    dataset: str; version: str; ano: int; tc: str
    carga_id: str; output_dir: Path
    engine_chunk_rows: int | None; max_invalid_samples: int | None
    contract: object | None = None

@dataclass(frozen=True)
class ExportResult:
    dataset: str; version: str; ano: int; tc: str
    carga_id: str; output_dir: str
    rows_total: int; rows_valid: int; rows_invalid: int
    parquet_files: int; timings: dict
    status: str = "exported_and_published"
    invalid_samples_file: str | None = None
    invalid_samples_count: int = 0
    invalid_samples_truncated: bool = False
    stopped_on_invalid_sample_limit: bool = False

    def to_dict(self): return asdict(self)

class ExportAndPublishDataset:

    def __init__(self, *, registry: Datasets, extract_service: ExtractService, publish_service: PublishService, contract_service: ContractService, app_config: AppConfig):
        self.registry = registry
        self.extract = extract_service
        self.publish = publish_service
        self.contract_service = contract_service
        self.cfg = app_config

    def _norm(self, dataset, version, ano):
        ds, ver = (dataset or "").strip().lower(), (version or "").strip().lower()
        if not ds: raise ValueError("dataset não informado")
        if not ver: raise ValueError("version não informada")
        try:
            ano = int(ano)
        except Exception as e:
            raise ValueError(f"ano inválido: {ano!r}") from e
        return ds, ver, ano

    def _make_ctx(self, ds, ver, ano, contract):
        carga_id = uuid.uuid4().hex[:8]
        output_dir = Path(self.cfg.output_dir) / ds / ver / carga_id
        output_dir.mkdir(parents=True, exist_ok=True)

        return RunContext(dataset=ds, version=ver, ano=ano, tc=str(self.cfg.producer_tc).strip().lower(), carga_id=carga_id, output_dir=output_dir, engine_chunk_rows=self.cfg.engine_chunk_rows,
                          max_invalid_samples=self.cfg.max_invalid_samples, contract=contract,)

    def _status(self, rows_total: int, rows_valid: int, rows_invalid: int) -> str:
        if rows_total == 0: return "completed_with_no_rows"
        if rows_valid == 0: return "completed_with_all_rows_invalid"
        if rows_invalid > 0: return "completed_with_invalid_rows"
        return "exported_and_published"

    def execute(self, *, dataset: str, version: str, ano: int):
        ds, ver, ano = self._norm(dataset, version, ano)

        spec = self.registry.get(ds, ver)
        contract = self.contract_service.load_and_validate(dataset=ds, version=ver)
        ctx = self._make_ctx(ds, ver, ano, contract)

        batches = self.extract.extract_batches(sql_path=spec.sql_path, ano=ctx.ano, schema=None)
        res = validate_and_write(df_chunks=batches,ingest_rules=contract.ingest_rules,schema_arrow=contract.schema_arrow,ctx=ctx,)

        if not bool(getattr(res, "ok", True)):
            raise RuntimeError("Falha técnica no processamento do engine")

        parquet_files = int(getattr(res, "parquet_files", 0) or 0)        
        rows_total = int(getattr(res, "rows_total", 0) or 0)
        rows_valid = int(getattr(res, "rows_valid", 0) or 0)
        rows_invalid = int(getattr(res, "rows_invalid", 0) or 0)

        if rows_invalid > 0:
            return ExportResult(dataset=ctx.dataset,version=ctx.version,ano=ctx.ano,tc=ctx.tc,carga_id=ctx.carga_id,output_dir=str(ctx.output_dir),rows_total=rows_total,rows_valid=rows_valid,rows_invalid=rows_invalid,
                parquet_files=parquet_files,timings=dict(getattr(res, "timings", {}) or {}),status="completed_with_invalid_rows_not_published",invalid_samples_file=getattr(res, "invalid_samples_file", None),
                invalid_samples_count=int(getattr(res, "invalid_samples_count", 0) or 0),invalid_samples_truncated=bool(getattr(res, "invalid_samples_truncated", False)),
                stopped_on_invalid_sample_limit=bool(getattr(res, "stopped_on_invalid_sample_limit", False)),).to_dict()

        if parquet_files > 0:
            self.publish.publish(local_dir=ctx.output_dir,dataset=ctx.dataset,version=ctx.version,contract=ctx.contract,tc=ctx.tc,ano=ctx.ano,)

        return ExportResult(dataset=ctx.dataset,version=ctx.version,ano=ctx.ano,tc=ctx.tc,carga_id=ctx.carga_id,output_dir=str(ctx.output_dir),rows_total=rows_total,rows_valid=rows_valid,rows_invalid=rows_invalid,
            parquet_files=parquet_files,timings=dict(getattr(res, "timings", {}) or {}), status=self._status(rows_total, rows_valid, rows_invalid), invalid_samples_file=getattr(res, "invalid_samples_file", None), 
            invalid_samples_count=int(getattr(res, "invalid_samples_count", 0) or 0), invalid_samples_truncated=bool(getattr(res, "invalid_samples_truncated", False)),
            stopped_on_invalid_sample_limit=bool(getattr(res, "stopped_on_invalid_sample_limit", False)),).to_dict()