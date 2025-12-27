from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import asdict, is_dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.time(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            for k, v in extra.items():
                payload[k] = _to_jsonable(v)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _to_jsonable(v: Any) -> Any:
    if is_dataclass(v):
        return asdict(v)
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_to_jsonable(x) for x in v]
    return str(v)


class _Logger:
    def __init__(self, logger: logging.Logger):
        self._l = logger

    def info(self, msg: str, **fields: Any) -> None:
        self._l.info(msg, extra={"extra_fields": fields})

    def warning(self, msg: str, **fields: Any) -> None:
        self._l.warning(msg, extra={"extra_fields": fields})

    def error(self, msg: str, **fields: Any) -> None:
        self._l.error(msg, extra={"extra_fields": fields})

    def exception(self, msg: str, **fields: Any) -> None:
        self._l.exception(msg, extra={"extra_fields": fields})

    def debug(self, msg: str, **fields: Any) -> None:
        self._l.debug(msg, extra={"extra_fields": fields})


def get_logger(name: str) -> _Logger:
    return _Logger(logging.getLogger(name))


def configure_logging(settings: Any) -> None:
    level_name = getattr(settings, "log_level", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    fmt: logging.Formatter
    if getattr(settings, "json_logs", True):
        fmt = JsonFormatter()
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)
    root.addHandler(stdout)

    # Optional file logging (rotating). Useful for shipping tails to GitHub safely.
    log_file = getattr(settings, "log_file", None)
    if log_file:
        p = Path(str(log_file)).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        p.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            str(p),
            maxBytes=int(getattr(settings, "log_max_bytes", 10_000_000)),
            backupCount=int(getattr(settings, "log_backup_count", 5)),
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)
