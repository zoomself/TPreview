"""Heuristic ST / risk name markers from display name (not exchange official flag)."""

from __future__ import annotations


def is_special_treatment_name(code_name: str) -> bool:
    """
    True if name suggests ST/*ST/S*ST or delisting-style markers.
    Baostock does not provide a dedicated boolean; this is best-effort.
    """
    if not code_name:
        return False
    s = code_name.strip()
    u = s.upper()
    if u.startswith("*ST") or u.startswith("S*ST") or u.startswith("ST"):
        return True
    if "退市" in s:
        return True
    return False


def st_label(code_name: str) -> str:
    return "是" if is_special_treatment_name(code_name) else "否"
