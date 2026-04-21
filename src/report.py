"""Build static HTML report from local daily CSVs (GitHub Pages)."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from src.config import TZ_SH, AppConfig, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("report")


@dataclass
class DayDetail:
    date: str
    close: float
    day_pct: float


@dataclass
class ReportRow:
    stock_name: str
    code: str
    latest_close: float
    streak_days: int
    streak_total_pct: float
    lookback_15_pct: float | None
    month_20_pct: float | None
    details: list[DayDetail]


def _trailing_down_streak(df: pd.DataFrame) -> tuple[int, int, list[DayDetail]]:
    """
    Streak ending at last row: count of strict down steps (close[t] < close[t-1]).
    Returns (streak_days, anchor_index, details) where anchor_index is `cur`
    from sync narrative (reference close for total move).
    """
    if df.empty or len(df) < 2:
        return 0, 0, []
    closes = pd.to_numeric(df["close"], errors="coerce")
    dates = df["date"].astype(str).tolist()
    n = len(df)
    cur = n - 1
    while cur > 0 and closes.iloc[cur] < closes.iloc[cur - 1]:
        cur -= 1
    streak_days = (n - 1) - cur
    details: list[DayDetail] = []
    for idx in range(cur + 1, n):
        prev = float(closes.iloc[idx - 1])
        c = float(closes.iloc[idx])
        day_pct = (c / prev - 1.0) * 100.0 if prev else 0.0
        details.append(DayDetail(date=dates[idx], close=c, day_pct=day_pct))
    return streak_days, cur, details


def _trailing_up_streak(df: pd.DataFrame) -> tuple[int, int, list[DayDetail]]:
    """
    Streak ending at last row: count of strict up steps (close[t] > close[t-1]).
    Returns (streak_days, anchor_index, details).
    """
    if df.empty or len(df) < 2:
        return 0, 0, []
    closes = pd.to_numeric(df["close"], errors="coerce")
    dates = df["date"].astype(str).tolist()
    n = len(df)
    cur = n - 1
    while cur > 0 and closes.iloc[cur] > closes.iloc[cur - 1]:
        cur -= 1
    streak_days = (n - 1) - cur
    details: list[DayDetail] = []
    for idx in range(cur + 1, n):
        prev = float(closes.iloc[idx - 1])
        c = float(closes.iloc[idx])
        day_pct = (c / prev - 1.0) * 100.0 if prev else 0.0
        details.append(DayDetail(date=dates[idx], close=c, day_pct=day_pct))
    return streak_days, cur, details


def _month_return(closes: pd.Series, lookback: int) -> float | None:
    if len(closes) <= lookback:
        return None
    a = float(closes.iloc[-(lookback + 1)])
    b = float(closes.iloc[-1])
    if a == 0:
        return None
    return (b / a - 1.0) * 100.0


def _row_to_ctx(r: ReportRow, *, streak_bg: str) -> dict[str, Any]:
    d = asdict(r)
    d["details"] = [asdict(x) for x in r.details]
    d["streak_bg"] = streak_bg
    d["stock_title"] = f"{r.stock_name}（{r.code}）"
    return d


# Light tints for dark theme (same streak_days → same entry via mapping below).
_STREAK_TINT_BASE: list[tuple[int, int, int]] = [
    (110, 168, 255),
    (130, 210, 175),
    (230, 190, 120),
    (200, 150, 230),
    (120, 210, 220),
    (230, 160, 175),
    (180, 200, 130),
    (160, 175, 235),
    (220, 175, 130),
    (140, 195, 200),
    (210, 145, 200),
    (175, 205, 155),
]


def _streak_background_map(streak_days_list: list[int]) -> dict[int, str]:
    """Same streak_days → same soft tint; different天数在调色板中轮换。"""
    unique = sorted(set(streak_days_list), reverse=True)
    out: dict[int, str] = {}
    for i, d in enumerate(unique):
        r, g, b = _STREAK_TINT_BASE[i % len(_STREAK_TINT_BASE)]
        out[d] = f"rgba({r},{g},{b},0.14)"
    return out


ReportMode = Literal["bear", "bull"]


def build_rows(cfg: AppConfig, mode: ReportMode) -> tuple[list[ReportRow], str]:
    daily_dir = cfg.daily_dir()
    meta_dir = cfg.meta_dir()
    stocks_path = meta_dir / "stocks.csv"
    if not stocks_path.is_file():
        LOG.warning("Missing %s — run sync first.", stocks_path)
        return [], ""
    meta = pd.read_csv(stocks_path, dtype=str)
    meta = meta.fillna("")
    name_by_code = dict(zip(meta["code"].astype(str), meta["code_name"].astype(str)))
    st_by_code = dict(zip(meta["code"].astype(str), meta.get("is_st", "0").astype(str)))

    streak_fn = _trailing_down_streak if mode == "bear" else _trailing_up_streak

    rows: list[ReportRow] = []
    data_as_of = ""
    for path in sorted(daily_dir.glob("*.csv")):
        code = path.stem  # sh.600000
        try:
            df = pd.read_csv(path)
        except Exception as e:
            LOG.debug("Skip %s: %s", path, e)
            continue
        if df.empty or "close" not in df.columns:
            continue
        df = df.sort_values("date")
        last_d = str(df["date"].iloc[-1])
        if last_d > data_as_of:
            data_as_of = last_d
        closes = pd.to_numeric(df["close"], errors="coerce")
        if closes.isna().all():
            continue
        streak_days, anchor_idx, details = streak_fn(df)
        if streak_days < cfg.min_streak_days:
            continue
        if str(st_by_code.get(code, "0")).strip() in {"1", "1.0"}:
            continue
        c_anchor = float(closes.iloc[anchor_idx])
        c_last = float(closes.iloc[-1])
        streak_total = (c_last / c_anchor - 1.0) * 100.0 if c_anchor else 0.0
        lb15 = _month_return(closes, 15)
        m20 = _month_return(closes, cfg.lookback_trading_days)
        disp_name = name_by_code.get(code, code)
        rows.append(
            ReportRow(
                stock_name=str(disp_name),
                code=code,
                latest_close=c_last,
                streak_days=streak_days,
                streak_total_pct=streak_total,
                lookback_15_pct=lb15,
                month_20_pct=m20,
                details=details,
            )
        )

    if mode == "bear":
        # Primary: streak_days desc. Secondary: more negative total move first.
        rows.sort(key=lambda r: r.streak_total_pct)
        rows.sort(key=lambda r: r.streak_days, reverse=True)
    else:
        # Primary: streak_days desc. Secondary: more positive total move first.
        rows.sort(key=lambda r: r.streak_total_pct, reverse=True)
        rows.sort(key=lambda r: r.streak_days, reverse=True)
    return rows, data_as_of


def render_report(
    cfg: AppConfig,
    rows_bear: list[ReportRow],
    rows_bull: list[ReportRow],
    out_path: Path,
    data_as_of: str,
) -> None:
    tpl_dir = cfg.template_file().parent
    env = Environment(
        loader=FileSystemLoader(str(tpl_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template(cfg.template_file().name)
    now_sh = datetime.now(TZ_SH).strftime("%Y-%m-%d %H:%M:%S %Z")
    bg_bear = _streak_background_map([r.streak_days for r in rows_bear])
    bg_bull = _streak_background_map([r.streak_days for r in rows_bull])
    html = tpl.render(
        rows_bear=[_row_to_ctx(r, streak_bg=bg_bear[r.streak_days]) for r in rows_bear],
        rows_bull=[_row_to_ctx(r, streak_bg=bg_bull[r.streak_days]) for r in rows_bull],
        generated_at=now_sh,
        data_end_date=data_as_of or cfg.resolved_end_date(),
        min_streak_days=cfg.min_streak_days,
        lookback_trading_days=cfg.lookback_trading_days,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    LOG.info(
        "Wrote %s (bear=%d bull=%d rows)",
        out_path,
        len(rows_bear),
        len(rows_bull),
    )


def run_report() -> int:
    cfg = load_config()
    rows_bear, as_bear = build_rows(cfg, "bear")
    rows_bull, as_bull = build_rows(cfg, "bull")
    data_as_of = as_bear if as_bear >= as_bull else as_bull
    out_html = cfg.docs_path() / "index.html"
    render_report(cfg, rows_bear, rows_bull, out_html, data_as_of)
    return 0


def main() -> None:
    raise SystemExit(run_report())


if __name__ == "__main__":
    main()
