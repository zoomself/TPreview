"""Load YAML config with sensible defaults and resolve `today` in Asia/Shanghai."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from zoneinfo import ZoneInfo

TZ_SH = ZoneInfo("Asia/Shanghai")
REPO_ROOT = Path(__file__).resolve().parents[1]


def today_shanghai() -> date:
    """Calendar 'today' in Asia/Shanghai (for end_date=today)."""
    from datetime import datetime

    return datetime.now(TZ_SH).date()


@dataclass
class AppConfig:
    start_date: str
    end_date: str
    adjustflag: str
    min_streak_days: int
    lookback_trading_days: int
    request_sleep_sec: float
    data_dir: str
    daily_subdir: str
    meta_subdir: str
    docs_dir: str
    template_path: str

    def resolved_end_date(self) -> str:
        if (self.end_date or "").strip().lower() == "today":
            return today_shanghai().strftime("%Y-%m-%d")
        return self.end_date.strip()

    def data_root(self) -> Path:
        return REPO_ROOT / self.data_dir

    def daily_dir(self) -> Path:
        return self.data_root() / self.daily_subdir

    def meta_dir(self) -> Path:
        return self.data_root() / self.meta_subdir

    def docs_path(self) -> Path:
        return REPO_ROOT / self.docs_dir

    def template_file(self) -> Path:
        return REPO_ROOT / self.template_path


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


DEFAULTS: dict[str, Any] = {
    "start_date": "2020-01-01",
    "end_date": "today",
    "adjustflag": "2",
    "min_streak_days": 5,
    "lookback_trading_days": 20,
    "request_sleep_sec": 0.05,
    "data_dir": "data",
    "daily_subdir": "daily",
    "meta_subdir": "meta",
    "docs_dir": "docs",
    "template_path": "templates/report.html.j2",
}


def config_file_path() -> Path:
    env = os.environ.get("CONFIG_PATH", "").strip()
    if env:
        return Path(env)
    p = REPO_ROOT / "config.yaml"
    if p.is_file():
        return p
    return REPO_ROOT / "config.example.yaml"


def load_config() -> AppConfig:
    path = config_file_path()
    raw: dict[str, Any] = {}
    if path.is_file():
        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    merged = _deep_merge(DEFAULTS, raw)
    return AppConfig(
        start_date=str(merged["start_date"]),
        end_date=str(merged["end_date"]),
        adjustflag=str(merged["adjustflag"]),
        min_streak_days=int(merged["min_streak_days"]),
        lookback_trading_days=int(merged["lookback_trading_days"]),
        request_sleep_sec=float(merged["request_sleep_sec"]),
        data_dir=str(merged["data_dir"]),
        daily_subdir=str(merged["daily_subdir"]),
        meta_subdir=str(merged["meta_subdir"]),
        docs_dir=str(merged["docs_dir"]),
        template_path=str(merged["template_path"]),
    )
