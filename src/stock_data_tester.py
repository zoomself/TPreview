"""
本地测试：对比单只股票「Baostock 接口」与「仓库内 data/daily CSV」是否一致。

用法示例：
  python -m src.stock_data_tester --code sh.600000 --start 2026-03-01 --end 2026-04-15
  python -m src.stock_data_tester --code sz.000001 --start 2026-01-01 --end today --adjustflag 2
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import baostock as bs
import pandas as pd

from src.config import REPO_ROOT, load_config
from src.sync import daily_csv_path

# 与 sync.py 中 query_history_k_data_plus 字段保持一致，便于逐列对比
K_FIELDS = "date,open,high,low,close,volume,amount,adjustflag,turn,pctChg"

NUMERIC_COLS = ("open", "high", "low", "close", "volume", "amount", "turn", "pctChg")

# 超过该阈值则认为「不一致」（可按需要改严/改松）
COL_TOL: dict[str, float] = {
    "open": 0.02,
    "high": 0.02,
    "low": 0.02,
    "close": 0.02,
    "volume": 1.0,
    "amount": 1.0,
    "turn": 0.0001,
    "pctChg": 0.02,
}


def _resolve_end_date(end_date: str) -> str:
    s = (end_date or "").strip()
    if s.lower() == "today":
        return load_config().resolved_end_date()
    return s


@dataclass
class StockDataTestResult:
    """单次对比的汇总结果。"""

    code: str
    start_date: str
    end_date: str
    adjustflag: str
    local_path: Path
    row_count_api: int
    row_count_local: int
    dates_only_in_api: list[str] = field(default_factory=list)
    dates_only_in_local: list[str] = field(default_factory=list)
    mismatch_detail: pd.DataFrame | None = None
    max_abs_close_diff: float | None = None
    max_abs_close_diff_date: str | None = None

    def ok(self) -> bool:
        if self.dates_only_in_api or self.dates_only_in_local:
            return False
        if self.mismatch_detail is not None and not self.mismatch_detail.empty:
            return False
        return True

    def summary_lines(self) -> list[str]:
        lines = [
            f"code={self.code} adjustflag={self.adjustflag} range={self.start_date}..{self.end_date}",
            f"local_csv={self.local_path}",
            f"rows api={self.row_count_api} local={self.row_count_local}",
        ]
        if self.dates_only_in_api:
            lines.append(f"only_in_api ({len(self.dates_only_in_api)}): {self.dates_only_in_api[:5]}{'...' if len(self.dates_only_in_api) > 5 else ''}")
        if self.dates_only_in_local:
            lines.append(f"only_in_local ({len(self.dates_only_in_local)}): {self.dates_only_in_local[:5]}{'...' if len(self.dates_only_in_local) > 5 else ''}")
        if self.max_abs_close_diff is not None:
            lines.append(
                f"max |Δclose| (common dates)={self.max_abs_close_diff:.6g} @ {self.max_abs_close_diff_date or '?'}"
            )
        lines.append("RESULT: " + ("MATCH" if self.ok() else "DIFF"))
        return lines

    def print_report(self) -> None:
        for line in self.summary_lines():
            print(line)
        if self.mismatch_detail is not None and not self.mismatch_detail.empty:
            pd.set_option("display.max_rows", 30)
            pd.set_option("display.width", 200)
            print("\n[mismatch_detail sample]")
            print(self.mismatch_detail.head(30).to_string())


class StockDataTester:
    """
    测试用：拉取 Baostock 日 K，与本地 data/daily/<code>.csv 对比。

    - ``adjustflag`` 不传则使用当前 ``config.yaml`` / ``config.example.yaml`` 中的值。
    - ``daily_dir`` 不传则使用配置里的日 K 目录。
    """

    def __init__(
        self,
        code: str,
        start_date: str,
        end_date: str,
        *,
        adjustflag: str | None = None,
        daily_dir: Path | None = None,
    ) -> None:
        self.code = code.strip()
        self.start_date = start_date.strip()
        self.end_date = _resolve_end_date(end_date.strip())
        cfg = load_config()
        self.adjustflag = (adjustflag or cfg.adjustflag).strip()
        self.daily_dir = daily_dir if daily_dir is not None else cfg.daily_dir()
        self.local_path = daily_csv_path(self.daily_dir, self.code)

    def fetch_baostock(self) -> pd.DataFrame:
        """需已 ``bs.login()``。"""
        rs = bs.query_history_k_data_plus(
            self.code,
            K_FIELDS,
            start_date=self.start_date,
            end_date=self.end_date,
            frequency="d",
            adjustflag=self.adjustflag,
        )
        if rs.error_code != "0":
            raise RuntimeError(f"query_history_k_data_plus: {rs.error_msg}")
        rows: list[list[str]] = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame(columns=rs.fields)
        return pd.DataFrame(rows, columns=rs.fields)

    def load_local(self) -> pd.DataFrame:
        if not self.local_path.is_file():
            return pd.DataFrame(columns=K_FIELDS.split(","))
        df = pd.read_csv(self.local_path)
        df = df[df["date"].astype(str).between(self.start_date, self.end_date, inclusive="both")]
        return df.reset_index(drop=True)

    def compare(self) -> StockDataTestResult:
        """登录 Baostock → 拉接口 → 登出，并与本地 CSV 合并对比。"""
        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"bs.login failed: {lg.error_msg}")
        try:
            api = self.fetch_baostock()
        finally:
            bs.logout()

        api = api.copy()
        local = self.load_local()
        api["date"] = api["date"].astype(str)
        if not local.empty:
            local = local.copy()
            local["date"] = local["date"].astype(str)

        api_dates = set(api["date"]) if not api.empty else set()
        local_dates = set(local["date"]) if not local.empty else set()
        only_api = sorted(api_dates - local_dates)
        only_local = sorted(local_dates - api_dates)

        mismatch: pd.DataFrame | None = None
        max_close_diff: float | None = None
        max_close_date: str | None = None

        if api.empty and local.empty:
            pass
        elif api.empty or local.empty:
            mismatch = pd.DataFrame({"note": ["one side empty"]})
        else:
            m = api.merge(local, on="date", how="inner", suffixes=("_api", "_disk"))
            if m.empty:
                mismatch = pd.DataFrame({"note": ["no overlapping dates"]})
            else:
                if "close_api" in m.columns and "close_disk" in m.columns:
                    ca = pd.to_numeric(m["close_api"], errors="coerce")
                    cb = pd.to_numeric(m["close_disk"], errors="coerce")
                    dclose = (ca - cb).abs()
                    ix = dclose.idxmax()
                    max_close_diff = float(dclose.max())
                    max_close_date = str(m.loc[ix, "date"])

                bad_rows: list[dict[str, Any]] = []
                for col in NUMERIC_COLS:
                    ca_n, cb_n = f"{col}_api", f"{col}_disk"
                    if ca_n not in m.columns or cb_n not in m.columns:
                        continue
                    a = pd.to_numeric(m[ca_n], errors="coerce")
                    b = pd.to_numeric(m[cb_n], errors="coerce")
                    delta = (a - b).abs()
                    tol = COL_TOL.get(col, 0.02)
                    bad = delta > tol
                    for i in m.index[bad]:
                        bad_rows.append(
                            {
                                "date": str(m.loc[i, "date"]),
                                "field": col,
                                "api": float(a.loc[i]) if pd.notna(a.loc[i]) else None,
                                "disk": float(b.loc[i]) if pd.notna(b.loc[i]) else None,
                                "abs_diff": float(delta.loc[i]),
                                "tol": tol,
                            }
                        )
                mismatch = pd.DataFrame(bad_rows) if bad_rows else None

        local_resolved = self.local_path
        if not local_resolved.is_absolute():
            local_resolved = (REPO_ROOT / local_resolved).resolve()
        else:
            local_resolved = local_resolved.resolve()

        return StockDataTestResult(
            code=self.code,
            start_date=self.start_date,
            end_date=self.end_date,
            adjustflag=self.adjustflag,
            local_path=local_resolved,
            row_count_api=len(api),
            row_count_local=len(local),
            dates_only_in_api=only_api,
            dates_only_in_local=only_local,
            mismatch_detail=mismatch,
            max_abs_close_diff=max_close_diff,
            max_abs_close_diff_date=max_close_date,
        )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare Baostock vs local CSV for one stock.")
    p.add_argument("--code", required=True, help="e.g. sh.600000, sz.000001, bj.430047")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD or today")
    p.add_argument("--adjustflag", default=None, help="Override config: 1 back 2 forward 3 none")
    p.add_argument("--daily-dir", type=Path, default=None, help="Override daily CSV directory")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    tester = StockDataTester(
        args.code,
        args.start,
        args.end,
        adjustflag=args.adjustflag,
        daily_dir=args.daily_dir,
    )
    result = tester.compare()
    result.print_report()
    return 0 if result.ok() else 1


if __name__ == "__main__":
    raise SystemExit(main())
