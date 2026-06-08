import argparse, json, time, os
from pathlib import Path
from app.AppConfig import AppConfig
from app.Hub import Hub
from app.logger import build_log_file_path, configure_logging

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "sim", "s"}

def _result_title(command: str, result: dict) -> str:
    status = result.get("status") if isinstance(result, dict) else None

    if command == "export":
        if status == "exported_and_published":
            return "Pipeline executado com sucesso; dados publicados"
        if status == "completed_with_invalid_rows_not_published":
            return "Pipeline concluído com erros de validação; dados não publicados"
        if status == "completed_with_all_rows_invalid":
            return "Pipeline concluído com todas as linhas inválidas; dados não publicados"
        if status == "completed_with_no_rows":
            return "Pipeline concluído sem linhas retornadas; nada foi publicado"
        return "Pipeline concluído"

    if command == "download":
        return "Download executado com sucesso"
    if command == "download-all-years":
        return "Download de todos os exercícios executado com sucesso"

    return "Comando concluído"

def _print_export_summary(result: dict) -> None:
    print("Resumo:")
    print(f"  Dataset        : {result.get('dataset')}/{result.get('version')}")
    print(f"  TC             : {result.get('tc')}")
    print(f"  Ano            : {result.get('ano')}")
    print(f"  Linhas totais  : {result.get('rows_total')}")
    print(f"  Linhas válidas : {result.get('rows_valid')}")
    print(f"  Linhas inválidas: {result.get('rows_invalid')}")
    if result.get("invalid_samples_file"):
        print(f"  Erros          : {result.get('invalid_samples_file')}")
    if result.get("warning_samples_file"):
        print(f"  Warnings       : {result.get('warning_samples_file')}")
    print(f"  Status         : {result.get('status')}")

def _print_success(title, result, elapsed, log, *, command: str):
    print(f"\n{title}\n")

    if command == "export" and isinstance(result, dict) and not _env_bool("PRINT_RESULT_JSON", False):
        _print_export_summary(result)
    else:
        _print_result(result)

    print(f"\nTempo total: {elapsed:.2f}s")
    if log:
        print(f"Log salvo em: {log}")

def build_parser():
    p = argparse.ArgumentParser(description="InfoContas DataHub Pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export")
    exp.add_argument("--dataset", required=True)
    exp.add_argument("--version", required=True)
    exp.add_argument("--ano", required=True, type=int)

    dl = sub.add_parser("download")
    dl.add_argument("--dataset", required=True)
    dl.add_argument("--version", required=True)
    dl.add_argument("--ano", required=True, type=int)
    dl.add_argument("--tc", required=True)

    dla = sub.add_parser("download-all-years")
    dla.add_argument("--dataset", required=True)
    dla.add_argument("--version", required=True)
    dla.add_argument("--tc", required=True)

    return p

def print_banner(a, log: Path | None):
    print("\n===========================================")
    print(" InfoContas DataHub Pipeline")
    print("===========================================")
    print(f"Command : {a.command}")
    print(f"Dataset : {a.dataset}")
    print(f"Version : {a.version}")
    if getattr(a, "ano", None) is not None: print(f"Ano     : {a.ano}")
    if getattr(a, "tc", None): print(f"TC      : {a.tc}")
    if log: print(f"Log     : {log}")
    print()

def _print_result(r): print(json.dumps(r, ensure_ascii=False, indent=2, default=str))

def _print_error(exc, elapsed, log):
    print("\nERRO NA EXECUÇÃO\n-------------------------------------------")
    print(str(exc), "\n")
    print(f"Tempo até a falha: {elapsed:.2f}s")
    if log: print(f"Veja detalhes em: {log}\n")

def _build_log_path(a, cfg):
    return build_log_file_path(
        command=a.command,
        dataset=getattr(a, "dataset", None),
        version=getattr(a, "version", None),
        ano=getattr(a, "ano", None),
        tc=getattr(a, "tc", None),
        base_dir=cfg.log_dir,
    )

def _args_to_kwargs(a):
    k = {"dataset": a.dataset, "version": a.version}
    if getattr(a, "ano", None) is not None: k["ano"] = a.ano
    if getattr(a, "tc", None): k["tc"] = a.tc
    return k

def main():
    args = build_parser().parse_args()
    cfg = AppConfig.from_env()
    log = _build_log_path(args, cfg)
    configure_logging(log_file_path=log)

    print_banner(args, log)
    t0 = time.perf_counter()

    try:
        hub = Hub.from_app_config(app_config=cfg)
        result = hub.execute_command(command=args.command, **_args_to_kwargs(args))
        _print_success(_result_title(args.command, result), result, time.perf_counter() - t0, log, command=args.command)
        return 0
    except Exception as e:
        _print_error(e, time.perf_counter() - t0, log)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())