"""
fetch_market.py
行情数据采集脚本 — 企业财务估值系统数据层
职责：采集个股与指数行情、计算 β 和相对 PE 分位，输出 market_snapshot.json
"""

import json
import os
import ssl
import logging
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─── 代理处理（必须在 akshare 导入前执行）────────────────────────────────────────
# 系统 HTTP 代理（http://127.0.0.1:7890）对 push2his.eastmoney.com 不可用。
# 本脚本改用 QQ Finance（proxy.finance.qq.com）和 csindex.com.cn，均可直连。
# 通过将 Session.trust_env 设为 False 跳过系统代理查找。
import requests as _req_pre
import requests.sessions as _req_sessions

_orig_send = _req_sessions.Session.send


def _no_env_proxy_send(self, request, **kwargs):
    self.trust_env = False
    return _orig_send(self, request, **kwargs)


_req_sessions.Session.send = _no_env_proxy_send

# csindex.com.cn 证书链在 macOS 系统 Python 下无法自动验证
ssl._create_default_https_context = ssl._create_unverified_context

# ─── 第三方库 ─────────────────────────────────────────────────────────────────────
try:
    import akshare as ak
    _AK_OK = True
except ImportError:
    _AK_OK = False

try:
    import pandas as pd
    _PD_OK = True
except ImportError:
    _PD_OK = False

try:
    import numpy as np
    from scipy.stats import linregress
    _SCI_OK = True
except ImportError:
    _SCI_OK = False

# ============ 配置区 ============
STOCK_CODE = "001287"           # 目标股票代码
STOCK_NAME = "中电港"
INDEX_CODE = "000300"           # 基准指数（沪深300）

PE_HISTORY_YEARS = 3            # 相对 PE 分位历史窗口（年）
BETA_HISTORY_YEARS = 2          # β 回归历史窗口（年）

OUTPUT_PATH = Path(__file__).parent / "output" / "market_snapshot.json"

TIMEOUT = 15
RETRIES = 2
# ================================

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─── 通用工具 ─────────────────────────────────────────────────────────────────────

def _round(val: Any, n: int = 4) -> Optional[float]:
    try:
        return round(float(val), n)
    except (TypeError, ValueError):
        return None


def _mk(value: Any, status: str = "ok", source: str = "derived",
        as_of: str = "") -> Dict:
    return {"value": value, "status": status, "source": source, "as_of": as_of}


def _unavail(source: str = "akshare", as_of: str = "") -> Dict:
    return {"value": None, "status": "unavailable", "source": source, "as_of": as_of}


def _today() -> date:
    return date.today()


def _date_str(d: date) -> str:
    return d.isoformat()


def _fmt(d: date) -> str:
    """YYYYMMDD format for akshare date params."""
    return d.strftime("%Y%m%d")


# ─── 数据采集层 ───────────────────────────────────────────────────────────────────

def fetch_stock_price_history(errors: List) -> Optional["pd.DataFrame"]:
    """
    采集个股日线收盘价，最近 BETA_HISTORY_YEARS 年。
    接口：stock_zh_a_hist_tx（QQ Finance，可直连）
    symbol 格式：sz002049 / sh600000
    返回列：date, close
    """
    if not _AK_OK:
        errors.append("akshare not installed")
        return None
    try:
        end = _today()
        start = end - timedelta(days=365 * BETA_HISTORY_YEARS + 30)
        market = "sh" if STOCK_CODE.startswith("6") else "sz"
        symbol = f"{market}{STOCK_CODE}"
        log.info(f"Fetching stock price history: {symbol}")
        df = ak.stock_zh_a_hist_tx(
            symbol=symbol,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust="qfq",
            timeout=float(TIMEOUT),
        )
        if df is None or df.empty:
            errors.append(f"stock_zh_a_hist_tx returned empty for {symbol}")
            return None
        df = df[["date", "close"]].copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)
        log.info(f"Stock price history: {len(df)} rows, last={df.iloc[-1]['date']}")
        return df
    except Exception as e:
        errors.append(f"fetch_stock_price_history: {e}")
        log.exception("fetch_stock_price_history failed")
        return None


def fetch_index_hist(errors: List) -> Optional["pd.DataFrame"]:
    """
    采集沪深300日线收盘价 + 滚动市盈率，最近 PE_HISTORY_YEARS 年。
    接口：stock_zh_index_hist_csindex（csindex.com.cn，可直连）
    返回列：date, close, pe_ttm
    """
    if not _AK_OK:
        return None
    try:
        end = _today()
        start = end - timedelta(days=365 * PE_HISTORY_YEARS + 30)
        log.info(f"Fetching index hist (CSI300) from csindex...")
        df = ak.stock_zh_index_hist_csindex(
            symbol=INDEX_CODE,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if df is None or df.empty:
            errors.append("stock_zh_index_hist_csindex returned empty")
            return None
        df = df[["日期", "收盘", "滚动市盈率"]].copy()
        df.columns = ["date", "close", "pe_ttm"]
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["pe_ttm"] = pd.to_numeric(df["pe_ttm"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        log.info(f"Index hist: {len(df)} rows, last={df.iloc[-1]['date']}, PE={df.iloc[-1]['pe_ttm']}")
        return df
    except Exception as e:
        errors.append(f"fetch_index_hist: {e}")
        log.exception("fetch_index_hist failed")
        return None


def fetch_stock_eps_history(errors: List) -> Optional["pd.DataFrame"]:
    """
    采集个股季度 EPS（摊薄），用于计算个股 PE 历史。
    接口：stock_financial_analysis_indicator（QQ Finance，可直连）
    返回列：date（报告期）, eps_ttm（近四季滚动合计）
    """
    if not _AK_OK:
        return None
    try:
        start_year = str(_today().year - PE_HISTORY_YEARS - 2)
        log.info(f"Fetching stock EPS history from {start_year}...")
        df = ak.stock_financial_analysis_indicator(
            symbol=STOCK_CODE,
            start_year=start_year,
        )
        if df is None or df.empty:
            errors.append("stock_financial_analysis_indicator returned empty")
            return None

        df = df[["日期", "摊薄每股收益(元)"]].copy()
        df.columns = ["report_date", "eps_ytd"]
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.date
        df["eps_ytd"] = pd.to_numeric(df["eps_ytd"], errors="coerce")
        df = df.sort_values("report_date").reset_index(drop=True)

        # 计算 TTM EPS：四个季度滚动合计
        # eps_ytd 是 YTD 累计值（Q1=1季度, Q2=2季度, Q3=3季度, 年报=4季度）
        # TTM = 最新YTD + 上年年报 - 上年同期YTD
        ttm_rows = []
        for i, row in df.iterrows():
            rd = row["report_date"]
            ytd = row["eps_ytd"]
            if pd.isna(ytd):
                continue
            if rd.month == 12:
                # 年报，直接就是全年
                ttm_rows.append({"date": rd, "eps_ttm": ytd})
                continue
            # 找上年年报
            prior_annual = df[(df["report_date"].apply(lambda d: d.year) == rd.year - 1) &
                               (df["report_date"].apply(lambda d: d.month) == 12)]
            # 找上年同期
            prior_same = df[(df["report_date"].apply(lambda d: d.year) == rd.year - 1) &
                             (df["report_date"].apply(lambda d: d.month) == rd.month)]
            if prior_annual.empty or prior_same.empty:
                continue
            pa = prior_annual.iloc[-1]["eps_ytd"]
            ps = prior_same.iloc[-1]["eps_ytd"]
            if pd.isna(pa) or pd.isna(ps):
                continue
            ttm = ytd + pa - ps
            ttm_rows.append({"date": rd, "eps_ttm": ttm})

        if not ttm_rows:
            errors.append("EPS TTM calculation produced no rows")
            return None

        result = pd.DataFrame(ttm_rows).sort_values("date").reset_index(drop=True)
        log.info(f"EPS TTM: {len(result)} rows, latest={result.iloc[-1]['date']}, eps_ttm={result.iloc[-1]['eps_ttm']:.4f}")
        return result
    except Exception as e:
        errors.append(f"fetch_stock_eps_history: {e}")
        log.exception("fetch_stock_eps_history failed")
        return None


# ─── 衍生指标计算层 ───────────────────────────────────────────────────────────────

def calc_beta(stock_hist: "pd.DataFrame", index_hist: "pd.DataFrame",
              errors: List) -> Dict:
    """
    OLS 回归计算 β。
    使用最近 BETA_HISTORY_YEARS 年的日收益率数据。
    """
    if not _SCI_OK:
        errors.append("scipy not installed, cannot compute beta")
        return _unavail("derived")
    if stock_hist is None or index_hist is None:
        return _unavail("derived")
    try:
        cutoff = _today() - timedelta(days=365 * BETA_HISTORY_YEARS + 5)

        s = stock_hist[stock_hist["date"] >= cutoff][["date", "close"]].copy()
        idx = index_hist[index_hist["date"] >= cutoff][["date", "close"]].copy()

        s = s.set_index("date").sort_index()
        idx = idx.set_index("date").sort_index()

        rs = s["close"].pct_change().dropna()
        ri = idx["close"].pct_change().dropna()

        aligned = pd.concat([rs.rename("stock"), ri.rename("index")],
                            axis=1).dropna()
        if len(aligned) < 60:
            errors.append(f"Beta: insufficient data ({len(aligned)} rows after alignment)")
            return _unavail("derived")

        slope, intercept, r_value, p_value, std_err = linregress(
            aligned["index"], aligned["stock"]
        )
        period_start = str(aligned.index[0])
        period_end = str(aligned.index[-1])

        return {
            "value": _round(slope),
            "status": "ok",
            "source": "derived",
            "method": "OLS regression on daily returns",
            "period_start": period_start,
            "period_end": period_end,
            "r_squared": _round(r_value ** 2),
            "n_obs": len(aligned),
        }
    except Exception as e:
        errors.append(f"calc_beta: {e}")
        return _unavail("derived")


def calc_stock_pe_series(stock_hist: "pd.DataFrame",
                         eps_hist: "pd.DataFrame",
                         errors: List) -> Optional["pd.DataFrame"]:
    """
    构造个股 PE 历史序列：每日收盘价 ÷ 当期有效 TTM EPS。
    使用 merge_asof 将季度 EPS 对齐到每个交易日。
    返回列：date, pe_ttm
    """
    if stock_hist is None or eps_hist is None:
        return None
    try:
        s = stock_hist[["date", "close"]].copy()
        e = eps_hist[["date", "eps_ttm"]].copy()

        s["date"] = pd.to_datetime(s["date"])
        e["date"] = pd.to_datetime(e["date"])

        s = s.sort_values("date")
        e = e.sort_values("date")

        merged = pd.merge_asof(s, e, on="date", direction="backward")
        merged = merged.dropna(subset=["eps_ttm"])
        # 过滤 EPS <= 0（亏损期不计入 PE 历史）
        merged = merged[merged["eps_ttm"] > 0].copy()
        if merged.empty:
            errors.append("calc_stock_pe_series: no valid EPS > 0 after merge")
            return None

        merged["pe_ttm"] = merged["close"] / merged["eps_ttm"]
        merged["date"] = merged["date"].dt.date

        cutoff = _today() - timedelta(days=365 * PE_HISTORY_YEARS + 30)
        merged = merged[merged["date"] >= cutoff].copy()

        log.info(f"Stock PE series: {len(merged)} rows, latest PE={merged.iloc[-1]['pe_ttm']:.2f}")
        return merged[["date", "pe_ttm"]].reset_index(drop=True)
    except Exception as e:
        errors.append(f"calc_stock_pe_series: {e}")
        return None


def calc_relative_pe_metrics(stock_pe: "pd.DataFrame",
                              index_hist: "pd.DataFrame",
                              errors: List) -> Dict:
    """
    计算相对 PE 指标：
    - 当前相对 PE = 个股当前PE / 沪深300当前PE
    - 相对 PE 历史序列（每日）
    - 当前相对 PE 在过去3年的百分位（0-1）
    - P25 / P75
    """
    if stock_pe is None or index_hist is None:
        return {}
    try:
        # 对齐两个序列
        sp = stock_pe[["date", "pe_ttm"]].copy()
        ip = index_hist[["date", "pe_ttm"]].copy()

        sp["date"] = pd.to_datetime(sp["date"])
        ip["date"] = pd.to_datetime(ip["date"])

        sp = sp.sort_values("date")
        ip = ip.sort_values("date")

        merged = pd.merge_asof(sp.rename(columns={"pe_ttm": "stock_pe"}),
                               ip.rename(columns={"pe_ttm": "index_pe"}),
                               on="date", direction="backward")
        merged = merged.dropna(subset=["stock_pe", "index_pe"])
        merged = merged[merged["index_pe"] > 0].copy()

        if merged.empty:
            errors.append("calc_relative_pe_metrics: no aligned data")
            return {}

        merged["rel_pe"] = merged["stock_pe"] / merged["index_pe"]

        # 当前值（最新一行）
        last = merged.iloc[-1]
        current_rel_pe = float(last["rel_pe"])
        current_date = str(last["date"].date())

        rel_series = merged["rel_pe"].dropna()
        pct = float((rel_series < current_rel_pe).mean())
        p25 = float(rel_series.quantile(0.25))
        p75 = float(rel_series.quantile(0.75))

        log.info(f"Relative PE: current={current_rel_pe:.3f}, pct={pct:.2%}, P25={p25:.3f}, P75={p75:.3f}")
        return {
            "current_date": current_date,
            "current_rel_pe": _round(current_rel_pe),
            "percentile_3y": _round(pct, 4),
            "p25_3y": _round(p25),
            "p75_3y": _round(p75),
        }
    except Exception as e:
        errors.append(f"calc_relative_pe_metrics: {e}")
        return {}


# ─── 输出构建层 ───────────────────────────────────────────────────────────────────

def build_output(stock_hist, index_hist, eps_hist,
                 beta_result, stock_pe, rel_pe_metrics,
                 errors) -> Dict:
    today_str = _date_str(_today())
    snapshot = {
        "meta": {
            "generated_at": datetime.now().astimezone().isoformat(),
            "stock_code": STOCK_CODE,
            "stock_name": STOCK_NAME,
            "index_code": INDEX_CODE,
        },
        "price": {},
        "pe": {},
        "beta": {},
        "errors": errors,
    }

    # ── 当前价格 ──
    if stock_hist is not None and not stock_hist.empty:
        last_row = stock_hist.iloc[-1]
        snapshot["price"]["current"] = _mk(
            value=_round(last_row["close"]),
            status="ok",
            source="akshare/qq_finance",
            as_of=str(last_row["date"]),
        )
    else:
        snapshot["price"]["current"] = _unavail(as_of=today_str)

    # ── 个股 TTM PE ──
    if stock_pe is not None and not stock_pe.empty:
        last_pe_row = stock_pe.iloc[-1]
        snapshot["pe"]["stock_ttm_pe"] = _mk(
            value=_round(last_pe_row["pe_ttm"]),
            status="ok",
            source="derived",
            as_of=str(last_pe_row["date"]),
        )
    else:
        snapshot["pe"]["stock_ttm_pe"] = _unavail(as_of=today_str)

    # ── 沪深300 TTM PE ──
    if index_hist is not None and not index_hist.empty:
        last_idx = index_hist.iloc[-1]
        csi300_pe = _round(last_idx["pe_ttm"])
        snapshot["pe"]["csi300_ttm_pe"] = _mk(
            value=csi300_pe,
            status="ok",
            source="akshare/csindex",
            as_of=str(last_idx["date"]),
        )
    else:
        snapshot["pe"]["csi300_ttm_pe"] = _unavail(as_of=today_str)

    # ── 相对 PE 指标 ──
    if rel_pe_metrics:
        as_of = rel_pe_metrics.get("current_date", today_str)
        snapshot["pe"]["relative_pe"] = _mk(
            value=rel_pe_metrics.get("current_rel_pe"),
            source="derived", as_of=as_of,
        )
        snapshot["pe"]["relative_pe_percentile_3y"] = _mk(
            value=rel_pe_metrics.get("percentile_3y"),
            source="derived", as_of=as_of,
        )
        snapshot["pe"]["relative_pe_p25_3y"] = _mk(
            value=rel_pe_metrics.get("p25_3y"),
            source="derived", as_of=as_of,
        )
        snapshot["pe"]["relative_pe_p75_3y"] = _mk(
            value=rel_pe_metrics.get("p75_3y"),
            source="derived", as_of=as_of,
        )
    else:
        for key in ("relative_pe", "relative_pe_percentile_3y",
                    "relative_pe_p25_3y", "relative_pe_p75_3y"):
            snapshot["pe"][key] = _unavail(as_of=today_str)

    # ── β ──
    snapshot["beta"] = beta_result if beta_result else _unavail(as_of=today_str)

    return snapshot


# ─── 主入口 ───────────────────────────────────────────────────────────────────────

def run() -> Dict:
    errors: List[str] = []

    if not _AK_OK:
        errors.append("akshare not installed")
        log.error("akshare not installed")
    if not _PD_OK:
        errors.append("pandas not installed")
    if not _SCI_OK:
        errors.append("scipy/numpy not installed, beta unavailable")

    # ── 并行采集三组原始数据 ──
    stock_hist = None
    index_hist = None
    eps_hist = None

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_stock = pool.submit(fetch_stock_price_history, errors)
        f_index = pool.submit(fetch_index_hist, errors)
        f_eps   = pool.submit(fetch_stock_eps_history, errors)

        stock_hist = f_stock.result()
        index_hist = f_index.result()
        eps_hist   = f_eps.result()

    # ── 串行计算衍生指标 ──
    beta_result = calc_beta(stock_hist, index_hist, errors)

    # 个股 PE 历史序列（需要 stock_hist + eps_hist）
    stock_pe = calc_stock_pe_series(stock_hist, eps_hist, errors)

    # 相对 PE 指标（需要 stock_pe + index_hist）
    rel_pe_metrics = calc_relative_pe_metrics(stock_pe, index_hist, errors)

    # ── 构建输出 ──
    snapshot = build_output(
        stock_hist, index_hist, eps_hist,
        beta_result, stock_pe, rel_pe_metrics,
        errors,
    )

    # ── 写文件 ──
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Saved → {OUTPUT_PATH}")

    return snapshot


if __name__ == "__main__":
    import sys
    result = run()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if result.get("errors"):
        print(f"\n⚠ {len(result['errors'])} error(s):", file=sys.stderr)
        for e in result["errors"]:
            print(f"  - {e}", file=sys.stderr)
