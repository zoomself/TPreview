"""Incremental daily K sync from Baostock into local CSV files."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import baostock as bs
import pandas as pd

from src.config import TZ_SH, AppConfig, load_config
from src.st_utils import is_special_treatment_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("sync")

K_FIELDS = (
    "date,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
)


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _fmt(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def trading_dates_between(cfg_start: str, cfg_end: str) -> list[str]:
    """Return sorted YYYY-MM-DD list of trading days (is_trading_day==1)."""
    rs = bs.query_trade_dates(cfg_start, cfg_end)
    if rs.error_code != "0":
        raise RuntimeError(f"query_trade_dates failed: {rs.error_msg}")
    idx_date = rs.fields.index("calendar_date")
    idx_flag = rs.fields.index("is_trading_day")
    out: list[str] = []
    while rs.error_code == "0" and rs.next():
        row = rs.get_row_data()
        if row[idx_flag] == "1":
            out.append(row[idx_date])
    return out


def next_trading_day_after(last_inclusive: str, end_inclusive: str) -> str | None:
    """First trading day strictly after last_inclusive, not after end_inclusive."""
    d0 = _parse_yyyymmdd(last_inclusive) + timedelta(days=1)
    d1 = _parse_yyyymmdd(end_inclusive)
    if d0 > d1:
        return None
    days = trading_dates_between(_fmt(d0), _fmt(d1))
    return days[0] if days else None


def load_stock_universe() -> pd.DataFrame:
    """All A-share basics from Baostock."""
    rs = bs.query_stock_basic()
    if rs.error_code != "0":
        raise RuntimeError(f"query_stock_basic failed: {rs.error_msg}")
    rows: list[list[str]] = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=rs.fields)
    df = df[df["code"].astype(str).str.match(r"^(sh|sz|bj)\.", na=False)]
    df = df[df["type"].astype(str) == "1"]
    df["is_st"] = df["code_name"].map(lambda x: 1 if is_special_treatment_name(str(x)) else 0)
    return df.reset_index(drop=True)


def daily_csv_path(daily_dir: Path, code: str) -> Path:
    safe = code.replace("/", "_")
    return daily_dir / f"{safe}.csv"


def read_last_date(csv_path: Path) -> str | None:
    if not csv_path.is_file():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["date"])
        if df.empty:
            return None
        return str(df["date"].iloc[-1])
    except Exception:
        return None


def fetch_k_range(code: str, start_date: str, end_date: str, adjustflag: str) -> pd.DataFrame:
    rs = bs.query_history_k_data_plus(
        code,
        K_FIELDS,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag=adjustflag,
    )
    if rs.error_code != "0":
        raise RuntimeError(f"{code}: query_history_k_data_plus: {rs.error_msg}")
    rows: list[list[str]] = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame(columns=rs.fields)
    return pd.DataFrame(rows, columns=rs.fields)


def trim_daily_dataframe(df: pd.DataFrame, retention_days: int) -> pd.DataFrame:
    """
    Keep rows with date >= (max date in df) - retention_days (calendar days).
    Dates are YYYY-MM-DD strings; anchor is the last row's date, not wall-clock today.
    """
    if retention_days <= 0 or df.empty or "date" not in df.columns:
        return df
    work = df.sort_values("date").copy()
    work["date"] = work["date"].astype(str).str[:10]
    last_s = str(work["date"].iloc[-1])
    try:
        last_d = datetime.strptime(last_s, "%Y-%m-%d").date()
    except ValueError:
        return df
    cutoff = last_d - timedelta(days=int(retention_days))
    cutoff_s = cutoff.strftime("%Y-%m-%d")
    trimmed = work[work["date"] >= cutoff_s].reset_index(drop=True)
    return trimmed


def merge_daily_csv(existing: Path | None, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        if existing and existing.is_file():
            return pd.read_csv(existing)
        return pd.DataFrame()
    new_df = new_df.copy()
    new_df["date"] = new_df["date"].astype(str)
    if existing and existing.is_file():
        old = pd.read_csv(existing)
        old["date"] = old["date"].astype(str)
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date"], keep="last")
        merged = merged.sort_values("date")
        return merged
    return new_df.sort_values("date")


def _apply_retention_to_universe(daily_dir: Path, stocks: pd.DataFrame, retention_days: int) -> None:
    """Trim every listed stock's CSV; also shrinks files that were skipped in the fetch loop."""
    if retention_days <= 0:
        return
    n_trimmed = 0
    for _, row in stocks.iterrows():
        code = str(row["code"])
        path = daily_csv_path(daily_dir, code)
        if not path.is_file():
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            LOG.debug("Retention skip %s: %s", path, e)
            continue
        trimmed = trim_daily_dataframe(df, retention_days)
        if len(trimmed) < len(df):
            trimmed.to_csv(path, index=False, encoding="utf-8-sig")
            n_trimmed += 1
    if n_trimmed:
        LOG.info("Retention: rewrote %d daily CSV(s) (>%d days dropped before latest bar).", n_trimmed, retention_days)


def is_trading_day(d: str) -> bool:
    rs = bs.query_trade_dates(d, d)
    if rs.error_code != "0":
        return False
    if not rs.next():
        return False
    row = rs.get_row_data()
    flag_idx = rs.fields.index("is_trading_day")
    return row[flag_idx] == "1"


def _apply_env_overrides(cfg: AppConfig) -> None:
    """Optional: SYNC_START_DATE, SYNC_END_DATE override YAML (YYYY-MM-DD or 'today')."""
    s = os.environ.get("SYNC_START_DATE", "").strip()
    if s:
        cfg.start_date = s
    e = os.environ.get("SYNC_END_DATE", "").strip()
    if e:
        cfg.end_date = e


def run_sync() -> int:
    cfg = load_config()
    _apply_env_overrides(cfg)
    end_date = cfg.resolved_end_date()
    daily_dir = cfg.daily_dir()
    meta_dir = cfg.meta_dir()
    daily_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    lg = bs.login()
    if lg.error_code != "0":
        LOG.error("bs.login failed: %s", lg.error_msg)
        return 1
    try:
        # Optional: skip weekends/holidays quickly when Actions runs daily
        run_tz_today = datetime.now(TZ_SH).strftime("%Y-%m-%d")
        stocks = load_stock_universe()
        if stocks.empty:
            LOG.error("Empty stock universe.")
            return 1
        lim = os.environ.get("SYNC_MAX_STOCKS", "").strip()
        if lim.isdigit():
            stocks = stocks.head(int(lim))
            LOG.info("SYNC_MAX_STOCKS=%s — limiting universe for this run.", lim)
        stocks_path = meta_dir / "stocks.csv"
        stocks.to_csv(stocks_path, index=False, encoding="utf-8-sig")
        LOG.info("Wrote %s (%d rows)", stocks_path, len(stocks))

        trim_all_daily = cfg.data_retention_days > 0

        if run_tz_today == end_date and not is_trading_day(end_date):
            LOG.info(
                "End date %s is not a trading day in Baostock calendar; skipping K-line fetch.",
                end_date,
            )
            if trim_all_daily:
                _apply_retention_to_universe(daily_dir, stocks, cfg.data_retention_days)
            return 0

        n_ok = 0
        n_skip = 0
        n_err = 0
        for _, row in stocks.iterrows():
            code = str(row["code"])
            path = daily_csv_path(daily_dir, code)
            last = read_last_date(path)
            if last and last >= end_date:
                n_skip += 1
                continue
            if last is None:
                start = cfg.start_date
            else:
                nxt = next_trading_day_after(last, end_date)
                if nxt is None:
                    n_skip += 1
                    continue
                start = nxt

            try:
                chunk = fetch_k_range(code, start, end_date, cfg.adjustflag)
                if chunk.empty:
                    n_skip += 1
                    continue
                merged = merge_daily_csv(path if path.is_file() else None, chunk)
                if not merged.empty:
                    merged.to_csv(path, index=False, encoding="utf-8-sig")
                n_ok += 1
            except Exception as e:
                LOG.warning("%s: %s", code, e)
                n_err += 1

            if cfg.request_sleep_sec > 0:
                time.sleep(cfg.request_sleep_sec)

        if trim_all_daily:
            _apply_retention_to_universe(daily_dir, stocks, cfg.data_retention_days)

        LOG.info("Done. updated=%d skipped=%d errors=%d", n_ok, n_skip, n_err)
        return 0
    finally:
        bs.logout()


def main() -> None:
    raise SystemExit(run_sync())


if __name__ == "__main__":
    main()
