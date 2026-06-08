import copy, logging, os, sys, time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

_INDENT_UNIT = "  "
TREE_ITEM = "├─"
TREE_LAST = "└─"

class _ConsoleHandler(logging.StreamHandler):
    def emit(self, record: logging.LogRecord) -> None:
        safe = copy.copy(record)
        safe.exc_info = None
        safe.exc_text = None
        safe.stack_info = None
        super().emit(safe)

class _SafeFileHandler(logging.FileHandler):
    def emit(self, record: logging.LogRecord) -> None:
        Path(self.baseFilename).parent.mkdir(parents=True, exist_ok=True)
        super().emit(record)

def _resolve_level(default: str = "INFO") -> int:
    return getattr(logging, os.getenv("LOG_LEVEL", default).strip().upper(), logging.INFO)

def _sanitize_filename_part(value: str | None) -> str:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return "na"

    out = []
    for ch in raw.lower():
        out.append(ch if ch.isalnum() or ch in ("-", "_", "=") else "_")

    compact = "".join(out)
    while "__" in compact:
        compact = compact.replace("__", "_")
    return compact.strip("_") or "na"


def build_log_file_path(*, command: str, dataset: str | None = None, version: str | None = None,
                        ano: int | None = None, tc: str | None = None, base_dir: str | Path | None = None) -> Path:
    base_dir = Path(base_dir or os.getenv("LOG_DIR", "logs"))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    parts = [_sanitize_filename_part(command), _sanitize_filename_part(dataset), _sanitize_filename_part(version)]
    if ano is not None:
        parts.append(f"ano={ano}")
    if tc:
        parts.append(f"tc={_sanitize_filename_part(tc)}")

    return base_dir / ("_".join(parts + [timestamp]) + ".log")


def configure_logging(*, log_dir: Path | None = None, log_file_name: str | None = None,
                      log_file_path: str | Path | None = None, level: int | None = None) -> Path:
    resolved_level = level if level is not None else _resolve_level("INFO")

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(resolved_level)

    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(name)-8s | %(message)s")

    console = _ConsoleHandler(sys.stdout)
    console.setLevel(resolved_level)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file_path is not None:
        log_path = Path(log_file_path)
    else:
        log_dir = Path(log_dir or os.getenv("LOG_DIR", "logs"))
        log_file_name = log_file_name or f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        log_path = log_dir / log_file_name

    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = _SafeFileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    return log_path


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def format_seconds(sec: float) -> str:
    sec = float(sec)
    if sec < 60:
        return f"{sec:.2f}s"
    minutes = int(sec // 60)
    seconds = sec % 60
    return f"{minutes}m{seconds:04.1f}s"


def format_int(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def indent_text(message: str, level: int = 0, bullet: str | None = None) -> str:
    prefix = _INDENT_UNIT * max(0, int(level))
    return f"{prefix}{bullet} {message}" if bullet else f"{prefix}{message}"


def log_info(logger: logging.Logger, message: str, *, level: int = 0, bullet: str | None = None, **kwargs) -> None:
    logger.info(indent_text(message, level, bullet), **kwargs)


def log_warning(logger: logging.Logger, message: str, *, level: int = 0, bullet: str | None = None, **kwargs) -> None:
    logger.warning(indent_text(message, level, bullet), **kwargs)


def log_error(logger: logging.Logger, message: str, *, level: int = 0, bullet: str | None = None, **kwargs) -> None:
    logger.error(indent_text(message, level, bullet), **kwargs)


def log_section(logger: logging.Logger, name: str) -> None:
    logger.info(f"[{name}]")


def log_item(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_info(logger, message, level=level, bullet=TREE_ITEM, **kwargs)


def log_last(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_info(logger, message, level=level, bullet=TREE_LAST, **kwargs)


""" def log_detail(logger: logging.Logger, message: str, *, level: int = 2, **kwargs) -> None:
    log_info(logger, message, level=level, **kwargs) """
    
def log_detail(logger: logging.Logger, message: str, *, level: int = 2, **kwargs) -> None:
    logger.debug(indent_text(message, level), **kwargs)    


def log_warn_item(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_warning(logger, message, level=level, bullet=TREE_ITEM, **kwargs)


def log_warn_last(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_warning(logger, message, level=level, bullet=TREE_LAST, **kwargs)


def log_warn_detail(logger: logging.Logger, message: str, *, level: int = 2, **kwargs) -> None:
    log_warning(logger, message, level=level, **kwargs)


def log_error_item(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_error(logger, message, level=level, bullet=TREE_ITEM, **kwargs)


def log_error_last(logger: logging.Logger, message: str, *, level: int = 1, **kwargs) -> None:
    log_error(logger, message, level=level, bullet=TREE_LAST, **kwargs)


@contextmanager
def log_step(logger: logging.Logger, message: str, *, level: int = 0):
    logger.info(indent_text(message, level=level, bullet="▶"))
    start = time.perf_counter()
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - start
        logger.exception(indent_text(f"{message} failed after {format_seconds(elapsed)}", level=level, bullet="✖"))
        raise
    else:
        elapsed = time.perf_counter() - start
        logger.info(indent_text(f"{message} finished in {format_seconds(elapsed)}", level=level, bullet="✔"))