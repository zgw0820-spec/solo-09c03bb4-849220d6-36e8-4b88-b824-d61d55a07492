"""Runtime configuration.

All values can be overridden with environment variables so that the same image
can be used by Docker Compose and by local tests.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Callable


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    """Collector settings.

    ``clock`` is injectable so tests can advance time deterministically.
    """

    db_path: str = "data/collector.db"
    window_seconds: float = 5.0
    max_active_traces: int = 8
    slow_root_ms: float = 500.0
    sweep_interval_seconds: float = 0.05
    clock: Callable[[], float] = field(default=time.time, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if self.max_active_traces < 1:
            raise ValueError("max_active_traces must be >= 1")
        if self.slow_root_ms <= 0:
            raise ValueError("slow_root_ms must be > 0")
        if self.sweep_interval_seconds <= 0:
            raise ValueError("sweep_interval_seconds must be > 0")

    @property
    def slow_root_ns(self) -> int:
        return int(round(self.slow_root_ms * 1_000_000))

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.environ.get("DB_PATH", "data/collector.db"),
            window_seconds=_env_float("COLLECT_WINDOW_SECONDS", 5.0),
            max_active_traces=_env_int("MAX_ACTIVE_TRACES", 8),
            slow_root_ms=_env_float("SLOW_ROOT_MS", 500.0),
            sweep_interval_seconds=_env_int("SWEEP_INTERVAL_MS", 50) / 1000.0,
        )
