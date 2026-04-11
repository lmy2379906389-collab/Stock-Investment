"""
fetch_finance.py
企业财务估值分析框架 — 数据采集脚本
职责：可靠地获取原始数据、计算衍生指标、输出干净的 JSON
输出：./output/finance_snapshot.json
"""

import json
import os
import ssl
import logging
import math
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ─── 代理与 SSL 处理（必须在 akshare 导入前执行）─────────────────────────────────
# 问题：macOS 系统代理 https_proxy=http://127.0.0.1:7890 被 requests 自动读取，
#       但该 HTTP 代理对 HTTPS 连接（TLS over CONNECT）返回 RemoteDisconnected。
# 解法：monkey-patch requests.api.request，每次请求都使用内置 SOCKS5 Session。
#       只有将 proxies 直接设置到 Session.proxies 上才能正确绕过系统代理。

import requests as _req_pre
import requests.sessions as _req_sessions

_SOCKS5_PROXIES = {"http": "socks5://127.0.0.1:7890", "https": "socks5://127.0.0.1:7890"}

_orig_api_request = _req_pre.api.request


def _noproxy_request(method, url, **kwargs):
    """Replace requests.api.request to force SOCKS5 proxy via Session.proxies."""
    if "proxies" not in kwargs:
        # Use a short-lived Session with proxies set directly (not via env lookup)
        with _req_sessions.Session() as _s:
            _s.proxies = _SOCKS5_PROXIES
            _s.verify = False
            return _s.request(method=method, url=url, **kwargs)
    return _orig_api_request(method, url, **kwargs)


_req_pre.api.request = _noproxy_request
# requests.get → requests.api.get → requests.api.request (patched above)

# csindex.com.cn 证书链在 macOS 系统 Python 下无法自动验证，统一关闭验证
ssl._create_default_https_context = ssl._create_unverified_context

# ─── 第三方库（软导入）────────────────────────────────────────────────────────────

try:
    import akshare as ak
    _AK_OK = True
except ImportError:
    _AK_OK = False

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    _REQ_OK = True
except ImportError:
    _REQ_OK = False

try:
    import pandas as pd
    _PD_OK = True
except ImportError:
    _PD_OK = False

try:
    import numpy as np
    from scipy.stats import linregress
    from scipy.optimize import brentq
    _SCI_OK = True
except ImportError:
    _SCI_OK = False

# ============ 配置区 ============
STOCK_CODE = "002049"           # 目标股票代码（A股6位数字）
STOCK_NAME = "紫光国微"          # 股票名称（用于日志和输出）
INDEX_CODE = "000300"           # 基准指数（沪深300）

# 静态上下文（写入 JSON meta）
ASSET_TYPE = "heavy"            # 资产类型："heavy" 或 "light"
MARGIN_REFERENCE = "净利率中枢 6%-12%"
CAPEX_REFERENCE = "资本开支/营收常态 15%-25%"

# 历史数据参数
PE_HISTORY_YEARS = 3            # 相对 PE 分位的历史窗口（年）
BETA_HISTORY_YEARS = 2          # β 回归的历史窗口（年）

# WACC 静态假设
PERPETUAL_GROWTH_RATE = 0.025   # 永续增长率（2.5%）
DCF_PROJECTION_YEARS = 5        # 反向 DCF 预测年数

# 路径
_BASE_DIR = Path(__file__).parent
MACRO_SNAPSHOT_PATH = _BASE_DIR / "output" / "macro_snapshot.json"
OUTPUT_PATH = _BASE_DIR / "output" / "finance_snapshot.json"
CONSENSUS_HISTORY_DIR = _BASE_DIR / "output" / "consensus_history"

# 网络参数
TIMEOUT = 15
RETRIES = 2
RETRY_INTERVAL = 3
# ================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ─── 通用工具 ────────────────────────────────────────────────────────────────────

def _round(val: Any, n: int = 4) -> Optional[float]:
    try:
        return round(float(val), n)
    except (TypeError, ValueError):
        return None


def _mk(value: Any, status: str = "ok", source: str = "derived",
        as_of: str = "") -> Dict:
    return {"value": value, "status": status, "source": source, "as_of": as_of}


def _unavail(source: str = "unknown", as_of: str = "") -> Dict:
    return {"value": None, "status": "unavailable", "source": source, "as_of": as_of}


def _insuf(source: str = "local_snapshot", as_of: str = "") -> Dict:
    return {"value": None, "status": "insufficient_history", "source": source, "as_of": as_of}


def _today_str() -> str:
    return date.today().isoformat()


def make_session() -> "requests.Session":
    session = requests.Session()
    retry = Retry(
        total=RETRIES,
        backoff_factor=RETRY_INTERVAL,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _col(df: "pd.DataFrame", *keywords: str) -> Optional[str]:
    """Find first column matching any keyword (case-insensitive)."""
    cols = list(df.columns)
    for kw in keywords:
        for c in cols:
            if kw in str(c):
                return c
    return None


def _get(df: "pd.DataFrame", row_idx: int, *keywords: str) -> Optional[float]:
    """Safe get value from df row by keyword column match."""
    col = _col(df, *keywords)
    if col is None:
        return None
    try:
        v = df.iloc[row_idx][col]
        return _round(v)
    except Exception:
        return None


# ─── TTM 计算 ─────────────────────────────────────────────────────────────────────

def _parse_report_date(s: Any) -> Optional[date]:
    """Parse report period string to date, e.g. '2025-09-30'."""
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def calc_ttm(df: "pd.DataFrame", col: str, errors: List) -> Optional[float]:
    """
    Compute TTM value from a financial statement DataFrame.
    df must be sorted descending by report date (latest first).
    Cumulative YTD logic:
      - If latest report is annual (month=12): use directly.
      - Otherwise: TTM = latest_YTD + prior_annual - same_period_prior_year
    东方财富接口的日期列名为 REPORT_DATE（datetime 类型）。
    """
    if df is None or df.empty or col not in df.columns:
        return None
    try:
        # 东方财富接口固定用 REPORT_DATE；兜底扫描其他中文列名
        if "REPORT_DATE" in df.columns:
            date_col = "REPORT_DATE"
        else:
            date_col = _col(df, "报告期", "报告日期", "日期") or df.columns[0]

        # Build (parsed_date, row_index) list
        dated = []
        for i in range(len(df)):
            d = _parse_report_date(df.iloc[i][date_col])
            if d:
                dated.append((d, i))
        if not dated:
            return None

        # Latest period
        latest_date, latest_idx = dated[0]

        def val(idx):
            try:
                v = df.iloc[idx][col]
                return float(v) if v is not None and str(v) not in ("", "nan", "--", "NaN") else None
            except Exception:
                return None

        latest_val = val(latest_idx)
        if latest_val is None:
            return None

        # If annual (December), use directly
        if latest_date.month == 12:
            return _round(latest_val)

        # Find prior annual (same year - 1, month = 12)
        prior_annual_val = None
        same_period_ly_val = None
        for d, idx in dated[1:]:
            if d.month == 12 and d.year == latest_date.year - 1 and prior_annual_val is None:
                prior_annual_val = val(idx)
            if d.month == latest_date.month and d.year == latest_date.year - 1 and same_period_ly_val is None:
                same_period_ly_val = val(idx)
            if prior_annual_val is not None and same_period_ly_val is not None:
                break

        if prior_annual_val is None or same_period_ly_val is None:
            # Fall back to latest YTD value (annualized rough estimate)
            months = latest_date.month
            return _round(latest_val * 12 / months) if months > 0 else None

        return _round(latest_val + prior_annual_val - same_period_ly_val)
    except Exception as e:
        errors.append({"indicator": f"calc_ttm:{col}", "error": str(e)})
        return None


# ─── 第一组：财报数据采集 ─────────────────────────────────────────────────────────

def fetch_financial_statements(errors: List) -> Dict:
    """Fetch income statement, balance sheet, cash flow from akshare (东方财富)."""
    result = {"profit": None, "balance": None, "cashflow": None,
              "report_period": None}
    if not _AK_OK:
        errors.append({"indicator": "financial_statements", "error": "akshare not installed"})
        return result

    # 东方财富接口要求 symbol 带市场前缀（沪: sh, 深: sz）
    # 002049 是深市，前缀 sz
    em_symbol = f"sz{STOCK_CODE}"

    # ── 利润表 ──
    try:
        log.info(f"Fetching income statement for {em_symbol}...")
        profit_df = ak.stock_profit_sheet_by_report_em(symbol=em_symbol)
        if profit_df is not None and not profit_df.empty:
            result["profit"] = profit_df
            d = _parse_report_date(profit_df.iloc[0]["REPORT_DATE"])
            if d:
                result["report_period"] = f"{d.year}Q{d.month // 3}"
            log.info(f"  利润表: {len(profit_df)} 期")
    except Exception as e:
        errors.append({"indicator": "income_statement", "error": str(e)})

    # ── 资产负债表 ──
    try:
        log.info(f"Fetching balance sheet for {em_symbol}...")
        balance_df = ak.stock_balance_sheet_by_report_em(symbol=em_symbol)
        if balance_df is not None and not balance_df.empty:
            result["balance"] = balance_df
            log.info(f"  资产负债表: {len(balance_df)} 期")
    except Exception as e:
        errors.append({"indicator": "balance_sheet", "error": str(e)})

    # ── 现金流量表 ──
    try:
        log.info(f"Fetching cash flow statement for {em_symbol}...")
        cf_df = ak.stock_cash_flow_sheet_by_report_em(symbol=em_symbol)
        if cf_df is not None and not cf_df.empty:
            result["cashflow"] = cf_df
            log.info(f"  现金流量表: {len(cf_df)} 期")
    except Exception as e:
        errors.append({"indicator": "cash_flow_statement", "error": str(e)})

    return result


def _extract_financials(fs: Dict, errors: List) -> Dict:
    """
    Extract raw financial fields from DataFrames.
    Returns a flat dict of numeric values.
    All values in yuan (元) units as returned by akshare.
    """
    profit = fs.get("profit")
    balance = fs.get("balance")
    cf = fs.get("cashflow")
    today = _today_str()
    rp = fs.get("report_period", "")

    out = {
        # Will be filled below; None means unavailable
        "net_profit_ttm": None,
        "non_recurring_profit": None,       # 扣非净利润（单期，非TTM）
        "revenue_ttm": None,
        "cogs_ttm": None,                   # 营业成本 TTM
        "ocf_ttm": None,                    # 经营活动现金流 TTM
        "capex_ttm": None,                  # 购建固定资产等 TTM
        "rd_expense": None,                 # 研发费用（最新期，单期）
        "rd_capitalized": None,             # 资本化研发（最新期）
        "interest_expense": None,           # 利息支出
        "tax_expense": None,                # 所得税费用（最新期）
        "profit_before_tax": None,          # 利润总额（最新期）
        # Balance sheet – current period
        "total_assets": None,
        "equity": None,                     # 归母股东权益
        "ar": None,                         # 应收账款
        "inventory": None,                  # 存货
        "goodwill": None,
        "short_term_debt": None,
        "long_term_debt": None,
        "bonds_payable": None,
        "current_noncurrent_debt": None,    # 一年内到期非流动负债
        "cash": None,                       # 货币资金
        "trading_financial_assets": None,
        "rd_capitalized_balance": None,     # 开发支出（资本化研发余额）
        # Balance sheet – prior year same period (for TTM-style averages)
        "total_assets_ly": None,
        "ar_ly": None,
        "inventory_ly": None,
        # Revenue and profit prior year (for YoY growth)
        "revenue_ly": None,
        "net_profit_ly": None,
        # Debt prior year (for Rd calculation average)
        "interest_bearing_debt_ly": None,
        "report_period": rp,
    }

    # ── 东方财富接口返回英文大写列名，直接按名称访问 ──
    def _fval(df, col, row=0):
        """Safe float extraction from EM financial DataFrame."""
        try:
            v = df.iloc[row][col]
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            return _round(float(v))
        except Exception:
            return None

    if profit is not None and not profit.empty:
        try:
            # TTM fields (累计YTD→TTM转换)
            out["net_profit_ttm"] = calc_ttm(profit, "PARENT_NETPROFIT", errors)
            out["revenue_ttm"] = calc_ttm(profit, "TOTAL_OPERATE_INCOME", errors)
            out["cogs_ttm"] = calc_ttm(profit, "OPERATE_COST", errors)

            # 扣非净利润（最新单期，用于利润质量比率）
            out["non_recurring_profit"] = _fval(profit, "DEDUCT_PARENT_NETPROFIT")

            # 研发费用（最新单期）
            out["rd_expense"] = _fval(profit, "RESEARCH_EXPENSE")

            # 利息支出：优先 INTEREST_EXPENSE，若为 NaN 用 FINANCE_EXPENSE（财务费用）近似
            ie = _fval(profit, "INTEREST_EXPENSE")
            out["interest_expense"] = ie if ie is not None else _fval(profit, "FINANCE_EXPENSE")

            # 所得税费用 & 利润总额（最新单期）
            out["tax_expense"] = _fval(profit, "INCOME_TAX")
            out["profit_before_tax"] = _fval(profit, "TOTAL_PROFIT")

            # 上年同期营收 & 归母净利润
            latest_d = _parse_report_date(profit.iloc[0]["REPORT_DATE"])
            if latest_d:
                for i in range(1, len(profit)):
                    d = _parse_report_date(profit.iloc[i]["REPORT_DATE"])
                    if d and d.month == latest_d.month and d.year == latest_d.year - 1:
                        out["revenue_ly"] = _fval(profit, "TOTAL_OPERATE_INCOME", i)
                        out["net_profit_ly"] = _fval(profit, "PARENT_NETPROFIT", i)
                        break
        except Exception as e:
            errors.append({"indicator": "profit_extract", "error": str(e)})

    if balance is not None and not balance.empty:
        try:
            out["total_assets"] = _fval(balance, "TOTAL_ASSETS")
            out["equity"] = _fval(balance, "TOTAL_PARENT_EQUITY")
            out["ar"] = _fval(balance, "ACCOUNTS_RECE")
            out["inventory"] = _fval(balance, "INVENTORY")
            out["goodwill"] = _fval(balance, "GOODWILL")
            out["short_term_debt"] = _fval(balance, "SHORT_LOAN")
            out["long_term_debt"] = _fval(balance, "LONG_LOAN")
            out["bonds_payable"] = _fval(balance, "BOND_PAYABLE")
            out["current_noncurrent_debt"] = _fval(balance, "NONCURRENT_ASSET_1YEAR")
            out["cash"] = _fval(balance, "MONETARYFUNDS")
            out["trading_financial_assets"] = _fval(balance, "TRADE_FINASSET")
            out["rd_capitalized_balance"] = _fval(balance, "DEVELOP_EXPENSE")

            # 上年同期资产负债数据（用于均值计算）
            latest_d = _parse_report_date(balance.iloc[0]["REPORT_DATE"])
            if latest_d:
                for i in range(1, len(balance)):
                    d = _parse_report_date(balance.iloc[i]["REPORT_DATE"])
                    if d and d.year == latest_d.year - 1 and d.month == latest_d.month:
                        out["total_assets_ly"] = _fval(balance, "TOTAL_ASSETS", i)
                        out["ar_ly"] = _fval(balance, "ACCOUNTS_RECE", i)
                        out["inventory_ly"] = _fval(balance, "INVENTORY", i)
                        parts = [_fval(balance, c, i) for c in
                                 ("SHORT_LOAN", "LONG_LOAN", "BOND_PAYABLE", "NONCURRENT_ASSET_1YEAR")]
                        parts = [x for x in parts if x is not None]
                        out["interest_bearing_debt_ly"] = _round(sum(parts)) if parts else None
                        break
        except Exception as e:
            errors.append({"indicator": "balance_extract", "error": str(e)})

    if cf is not None and not cf.empty:
        try:
            # 东方财富现金流量表列名：NETCASH_OPERATE, CONSTRUCT_LONG_ASSET
            out["ocf_ttm"] = calc_ttm(cf, "NETCASH_OPERATE", errors)
            out["capex_ttm"] = calc_ttm(cf, "CONSTRUCT_LONG_ASSET", errors)
        except Exception as e:
            errors.append({"indicator": "cashflow_extract", "error": str(e)})

    return out


# ─── 第二组：行情数据采集 ─────────────────────────────────────────────────────────

def fetch_market_data(errors: List) -> Dict:
    """Fetch stock price history, index history, PE data."""
    result = {
        "current_price": None,
        "market_cap": None,
        "pe_ttm": None,
        "stock_hist": None,
        "index_hist": None,
        "stock_pe_hist": None,
        "csi300_pe_hist": None,
        "csi300_pe_current": None,
    }
    if not _AK_OK or not _PD_OK:
        errors.append({"indicator": "market_data", "error": "akshare/pandas not installed"})
        return result

    today = date.today()
    beta_start = (today - timedelta(days=BETA_HISTORY_YEARS * 365 + 30)).strftime("%Y%m%d")
    pe_start = (today - timedelta(days=PE_HISTORY_YEARS * 365 + 30)).strftime("%Y%m%d")
    end_str = today.strftime("%Y%m%d")

    # ── 个股日线历史（Beta用）+ 当前价格 ──
    # stock_individual_spot_xq 在当前环境不可用（'data'键错误）
    # 直接从日线历史末行取收盘价作为当前价
    try:
        log.info(f"Fetching stock daily history for {STOCK_CODE}...")
        stock_hist = ak.stock_zh_a_hist(
            symbol=STOCK_CODE, period="daily",
            start_date=beta_start, end_date=end_str, adjust="qfq"
        )
        if stock_hist is not None and not stock_hist.empty:
            result["stock_hist"] = stock_hist
            log.info(f"  个股日线: {len(stock_hist)} 条")
            result["current_price"] = _round(stock_hist.iloc[-1]["收盘"])
    except Exception as e:
        errors.append({"indicator": "stock_hist", "error": str(e)})

    # ── 沪深300历史（含滚动PE）: stock_zh_index_hist_csindex ──
    # 该接口同时返回收盘价和滚动市盈率，一次解决 index_hist + CSI300 PE 两个需求
    # 验证可用：SSL bypass 后正常返回，列：日期/收盘/滚动市盈率
    csi300_ok = False
    try:
        log.info("Fetching CSI300 history + PE (stock_zh_index_hist_csindex)...")
        csi_start = (today - timedelta(days=max(BETA_HISTORY_YEARS, PE_HISTORY_YEARS) * 365 + 60)).strftime("%Y%m%d")
        csi_df = ak.stock_zh_index_hist_csindex(
            symbol=INDEX_CODE, start_date=csi_start, end_date=end_str
        )
        if csi_df is not None and not csi_df.empty:
            csi_df["日期"] = pd.to_datetime(csi_df["日期"])
            csi_df = csi_df.sort_values("日期")

            # 收盘价历史（用于Beta回归）
            result["index_hist"] = csi_df[["日期", "收盘"]].rename(columns={"日期": "日期"})

            # PE历史（用于相对PE分位）
            pe_cutoff = pd.Timestamp(today) - pd.DateOffset(years=PE_HISTORY_YEARS)
            csi_pe_df = csi_df[csi_df["日期"] >= pe_cutoff][["日期", "滚动市盈率"]].copy()
            csi_pe_df = csi_pe_df.dropna(subset=["滚动市盈率"])
            csi_pe_df = csi_pe_df.rename(columns={"日期": "date", "滚动市盈率": "pe"})
            result["csi300_pe_hist"] = csi_pe_df
            result["csi300_pe_current"] = _round(csi_pe_df.iloc[-1]["pe"])
            csi300_ok = True
            log.info(f"  沪深300: {len(csi_df)} 条, PE序列: {len(csi_pe_df)} 条, 当前PE: {result['csi300_pe_current']}")
    except Exception as e:
        errors.append({"indicator": "csi300_hist", "error": str(e)})

    if not csi300_ok:
        errors.append({
            "indicator": "csi300_pe_hist",
            "error": "stock_zh_index_hist_csindex failed. 相对PE分位和Beta计算无法完成。"
        })

    # ── 个股 PE 历史（通过季报EPS + 日线价格推导）──
    # stock_a_lg_indicator 不存在；改用 stock_financial_analysis_indicator 取季报YTD EPS
    # 再与日线收盘价合并，计算每日 TTM PE
    stock_hist = result.get("stock_hist")
    if stock_hist is not None and not stock_hist.empty:
        try:
            log.info(f"Computing stock PE history from financial indicator + price...")
            fa_df = ak.stock_financial_analysis_indicator(symbol=STOCK_CODE, start_year=str(today.year - PE_HISTORY_YEARS - 1))
            if fa_df is not None and not fa_df.empty and "摊薄每股收益(元)" in fa_df.columns:
                fa_df["日期"] = pd.to_datetime(fa_df["日期"])
                fa_df = fa_df.sort_values("日期")
                fa_df["eps_ytd"] = pd.to_numeric(fa_df["摊薄每股收益(元)"], errors="coerce")

                # 计算 TTM EPS：对每个报告期做 TTM 转换
                ttm_eps_list = []
                fa_list = list(fa_df.itertuples(index=False))
                for row_i, row in enumerate(fa_list):
                    d = row.日期
                    ytd = row.eps_ytd
                    if pd.isna(ytd):
                        continue
                    if d.month == 12:
                        ttm_eps_list.append((d, ytd))
                    else:
                        # 找上年年报和上年同期
                        prior_annual = next((r.eps_ytd for r in fa_list[:row_i]
                                            if r.日期.month == 12 and r.日期.year == d.year - 1
                                            and not pd.isna(r.eps_ytd)), None)
                        prior_same = next((r.eps_ytd for r in fa_list[:row_i]
                                          if r.日期.month == d.month and r.日期.year == d.year - 1
                                          and not pd.isna(r.eps_ytd)), None)
                        if prior_annual is not None and prior_same is not None:
                            ttm_eps_list.append((d, ytd + prior_annual - prior_same))
                        elif prior_annual is not None:
                            # 没有上年同期：用当前YTD+上年年报的月份占比近似
                            ttm_eps_list.append((d, ytd + prior_annual * (12 - d.month) / 12))

                if ttm_eps_list:
                    ttm_df = pd.DataFrame(ttm_eps_list, columns=["report_date", "ttm_eps"])
                    # 将季报TTM EPS按报告期向前填充到每个交易日
                    price_df = stock_hist[["日期", "收盘"]].copy()
                    price_df["日期"] = pd.to_datetime(price_df["日期"])
                    price_df = price_df.sort_values("日期")
                    # merge_asof: 每个交易日匹配最近的较早报告期
                    merged = pd.merge_asof(price_df, ttm_df, left_on="日期", right_on="report_date")
                    merged = merged.dropna(subset=["ttm_eps"])
                    merged = merged[merged["ttm_eps"] > 0]
                    merged["pe"] = merged["收盘"] / merged["ttm_eps"]
                    merged = merged[merged["pe"] > 0]
                    pe_cutoff = pd.Timestamp(today) - pd.DateOffset(years=PE_HISTORY_YEARS)
                    merged = merged[merged["日期"] >= pe_cutoff]
                    result["stock_pe_hist"] = merged[["日期", "pe"]].rename(columns={"日期": "date"})
                    # 当前 TTM PE
                    if result["pe_ttm"] is None and len(merged) > 0:
                        result["pe_ttm"] = _round(merged.iloc[-1]["pe"])
                    log.info(f"  个股PE历史: {len(merged)} 条, 当前TTM PE: {result['pe_ttm']}")
        except Exception as e:
            errors.append({"indicator": "stock_pe_hist", "error": str(e)})

    # ── 总市值：从日线数据取收盘价 × 总股本 ──
    # 方案：从 stock_zh_a_hist 的成交额和成交量估算，但不如直接查。
    # 实际上 Beta 中需要的是相对值，WACC 中需要市值做权重。
    # 改为从 stock_financial_analysis_indicator 里取总资产，或用简化方法：
    # 总市值 ≈ 流通股本在别处。这里用 stock_zh_a_hist 的 "总市值" 列（如有）。
    if stock_hist is not None and "总市值" in (stock_hist.columns if stock_hist is not None else []):
        try:
            result["market_cap"] = _round(stock_hist.iloc[-1]["总市值"])
        except Exception:
            pass

    return result


# ─── 第三组：券商一致预期 ──────────────────────────────────────────────────────────

def fetch_consensus(errors: List) -> Dict:
    """Fetch analyst consensus EPS forecasts.
    主：同花顺 stock_profit_forecast_ths（已验证可用，返回均值EPS和净利润）
    备：东方财富数据中台 API（RPT_ANALYST_PREDICT）
    """
    result = {
        "fy1_eps": None, "fy2_eps": None,
        "fy1_net_profit": None, "fy2_net_profit": None,
        "source": "unavailable",
    }

    current_year = date.today().year
    fy1_year = current_year + 1
    fy2_year = current_year + 2

    # ── 主：同花顺一致预期（stock_profit_forecast_ths）──
    # 返回列：年度 / 预测机构数 / 最小值 / 均值 / 最大值 / 行业平均数
    if _AK_OK:
        try:
            log.info("Fetching consensus EPS from THS (stock_profit_forecast_ths)...")
            eps_df = ak.stock_profit_forecast_ths(symbol=STOCK_CODE, indicator="预测年报每股收益")
            np_df = ak.stock_profit_forecast_ths(symbol=STOCK_CODE, indicator="预测年报净利润")

            for row in eps_df.itertuples(index=False):
                yr = int(row.年度)
                mean_val = _round(row.均值)
                if yr == fy1_year:
                    result["fy1_eps"] = mean_val
                elif yr == fy2_year:
                    result["fy2_eps"] = mean_val

            for row in np_df.itertuples(index=False):
                yr = int(row.年度)
                # 净利润单位是亿元，转换为元
                try:
                    mean_val = _round(float(row.均值) * 1e8)
                except Exception:
                    mean_val = None
                if yr == fy1_year:
                    result["fy1_net_profit"] = mean_val
                elif yr == fy2_year:
                    result["fy2_net_profit"] = mean_val

            if result["fy1_eps"] is not None or result["fy2_eps"] is not None:
                result["source"] = "ths_profit_forecast"
                log.info(f"  一致预期(THS): FY{fy1_year} EPS={result['fy1_eps']}, FY{fy2_year} EPS={result['fy2_eps']}")
                return result
        except Exception as e:
            errors.append({"indicator": "consensus_ths", "error": str(e)})

    if not _REQ_OK:
        errors.append({"indicator": "consensus", "error": "requests not installed"})
        return result

    # ── 备用：东方财富数据中台 API ──
    try:
        log.info("Fetching consensus from EastMoney datacenter (fallback)...")
        session = make_session()
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "reportName": "RPT_ANALYST_PREDICT",
            "columns": "SECURITY_CODE,PREDICT_YEAR,EPS,NET_PROFIT,PREDICT_YEAR_COUNT",
            "filter": f'(SECURITY_CODE="{STOCK_CODE}")',
            "pageNumber": 1,
            "pageSize": 20,
            "sortTypes": "-1,-1",
            "sortColumns": "PREDICT_YEAR,ORIGPREDICT_DATE",
            "source": "WEB",
            "client": "WEB",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "https://data.eastmoney.com/",
        }
        resp = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("result", {}).get("data", []) or []
        for row in rows:
            yr = int(row.get("PREDICT_YEAR", 0))
            eps = _round(row.get("EPS"))
            np_ = _round(row.get("NET_PROFIT"))
            if yr == fy1_year:
                result["fy1_eps"] = eps
                result["fy1_net_profit"] = np_
            elif yr == fy2_year:
                result["fy2_eps"] = eps
                result["fy2_net_profit"] = np_
        if result["fy1_eps"] is not None or result["fy2_eps"] is not None:
            result["source"] = "eastmoney_datacenter"
            log.info(f"  一致预期(EM): FY{fy1_year} EPS={result['fy1_eps']}, FY{fy2_year} EPS={result['fy2_eps']}")
            return result
    except Exception as e:
        errors.append({"indicator": "consensus_eastmoney", "error": str(e)})

    errors.append({"indicator": "consensus", "error": "All consensus sources failed"})
    return result


# ─── 第四组：读取宏观快照 ─────────────────────────────────────────────────────────

def read_macro_snapshot(errors: List) -> Dict:
    """Read Rf and ERP from macro_snapshot.json."""
    result = {"rf": None, "erp": None}
    try:
        if not MACRO_SNAPSHOT_PATH.exists():
            errors.append({"indicator": "macro_snapshot", "error": f"File not found: {MACRO_SNAPSHOT_PATH}"})
            return result
        with open(MACRO_SNAPSHOT_PATH, encoding="utf-8") as f:
            snap = json.load(f)
        rf_raw = snap.get("layer3_market", {}).get("BOND_YIELD_10Y", {}).get("yield_pct")
        erp_raw = snap.get("layer3_market", {}).get("ERP", {}).get("value")
        if rf_raw is not None:
            result["rf"] = _round(float(rf_raw) / 100, 6)  # convert % to decimal
        if erp_raw is not None:
            result["erp"] = _round(float(erp_raw), 6)
        log.info(f"  宏观: Rf={result['rf']}, ERP={result['erp']}")
    except Exception as e:
        errors.append({"indicator": "macro_snapshot", "error": str(e)})
    return result


# ─── 一致预期快照管理 ─────────────────────────────────────────────────────────────

def save_consensus_snapshot(consensus: Dict) -> None:
    """Save current consensus data to monthly snapshot file."""
    if consensus.get("fy1_eps") is None and consensus.get("fy2_eps") is None:
        return
    try:
        CONSENSUS_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        fname = CONSENSUS_HISTORY_DIR / f"consensus_{date.today().strftime('%Y%m')}.json"
        payload = {
            "snapshot_date": _today_str(),
            "stock_code": STOCK_CODE,
            "fy1_eps": consensus.get("fy1_eps"),
            "fy2_eps": consensus.get("fy2_eps"),
            "fy1_net_profit": consensus.get("fy1_net_profit"),
            "fy2_net_profit": consensus.get("fy2_net_profit"),
        }
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log.info(f"  一致预期快照已保存: {fname.name}")
    except Exception as e:
        log.warning(f"Failed to save consensus snapshot: {e}")


def load_consensus_snapshot_3m() -> Optional[Dict]:
    """Load consensus snapshot from ~3 months ago."""
    try:
        d = date.today()
        # Try exact 3m, then 2m as fallback
        for months_back in [3, 2]:
            month = d.month - months_back
            year = d.year
            while month <= 0:
                month += 12
                year -= 1
            fname = CONSENSUS_HISTORY_DIR / f"consensus_{year}{month:02d}.json"
            if fname.exists():
                with open(fname, encoding="utf-8") as f:
                    return json.load(f)
    except Exception:
        pass
    return None


# ─── 衍生指标计算 ──────────────────────────────────────────────────────────────────

def _safe_div(a, b) -> Optional[float]:
    try:
        if b is None or b == 0 or a is None:
            return None
        return _round(a / b)
    except Exception:
        return None


def _yoy_direction(current: Optional[float], prior: Optional[float]) -> Optional[str]:
    if current is None or prior is None or prior == 0:
        return None
    change = (current - prior) / abs(prior)
    if abs(change) <= 0.05:
        return "stable"
    return "up" if change > 0 else "down"


def calc_q1_indicators(fin: Dict, errors: List) -> Dict:
    """Q1: Profit quality indicators."""
    rp = fin.get("report_period", "")
    today = _today_str()

    ocf = fin.get("ocf_ttm")
    np_ttm = fin.get("net_profit_ttm")
    nr = fin.get("non_recurring_profit")
    rev_ttm = fin.get("revenue_ttm")
    ar = fin.get("ar")
    ar_ly = fin.get("ar_ly")
    goodwill = fin.get("goodwill")
    equity = fin.get("equity")

    def mk(v): return _mk(v, "ok", "derived", rp) if v is not None else _unavail("derived", rp)

    # 1.1 经营性现金流/净利润
    ocf_ratio = _safe_div(ocf, np_ttm)

    # 1.2 扣非净利润/净利润 (单期比值)
    nr_ratio = _safe_div(nr, np_ttm) if nr is not None else None

    # 1.3 应收账款周转率
    ar_avg = _safe_div((ar or 0) + (ar_ly or 0), 2) if (ar is not None and ar_ly is not None) else ar
    ar_turnover = _safe_div(rev_ttm, ar_avg)

    # 1.4 商誉/净资产
    gw_ratio = _safe_div(goodwill, equity)

    return {
        "ocf_to_net_profit": mk(ocf_ratio),
        "non_recurring_ratio": mk(nr_ratio),
        "ar_turnover": mk(ar_turnover),
        "goodwill_to_equity": mk(gw_ratio),
    }


def calc_q2_indicators(fin: Dict, errors: List) -> Dict:
    """Q2: Operating trend indicators."""
    rp = fin.get("report_period", "")

    np_ttm = fin.get("net_profit_ttm")
    rev_ttm = fin.get("revenue_ttm")
    cogs_ttm = fin.get("cogs_ttm")
    ta = fin.get("total_assets")
    ta_ly = fin.get("total_assets_ly")
    equity = fin.get("equity")
    inv = fin.get("inventory")
    inv_ly = fin.get("inventory_ly")
    std = fin.get("short_term_debt")
    ltd = fin.get("long_term_debt")
    bp = fin.get("bonds_payable")
    cnd = fin.get("current_noncurrent_debt")
    cash = fin.get("cash")
    tfa = fin.get("trading_financial_assets")
    capex = fin.get("capex_ttm")
    rd = fin.get("rd_expense")
    rd_cap = fin.get("rd_capitalized_balance")
    rev_ly = fin.get("revenue_ly")
    np_ly = fin.get("net_profit_ly")

    def mk(v): return _mk(v, "ok", "derived", rp) if v is not None else _unavail("derived", rp)

    # DuPont
    net_margin = _safe_div(np_ttm, rev_ttm)
    ta_avg = _safe_div((ta or 0) + (ta_ly or 0), 2) if (ta is not None and ta_ly is not None) else ta
    asset_turnover = _safe_div(rev_ttm, ta_avg)
    equity_multiplier = _safe_div(ta, equity)

    # YoY DuPont directions need prior year same period values
    # We only have current values here; direction calculation requires storing prior snapshot
    # For now: mark as ok with None value pending (or compute if we add prior year DuPont fields later)
    # Simple approach: mark direction None/unavailable since we don't store prior year DuPont values
    net_margin_yoy = _unavail("derived", rp)
    at_yoy = _unavail("derived", rp)
    em_yoy = _unavail("derived", rp)

    # Inventory turnover
    inv_avg = _safe_div((inv or 0) + (inv_ly or 0), 2) if (inv is not None and inv_ly is not None) else inv
    inv_turnover = _safe_div(cogs_ttm, inv_avg)

    # Interest-bearing debt
    ibd_parts = [x for x in [std, ltd, bp, cnd] if x is not None]
    ibd = _round(sum(ibd_parts)) if ibd_parts else None
    ibd_ratio = _safe_div(ibd, ta)

    # Cash coverage
    liquid = _round((cash or 0) + (tfa or 0)) if (cash is not None or tfa is not None) else None
    short_debt = _round((std or 0) + (cnd or 0)) if (std is not None or cnd is not None) else None
    cash_coverage = _safe_div(liquid, short_debt)

    # Capex / revenue
    capex_ratio = _safe_div(capex, rev_ttm)

    # RD intensity (single period rd / revenue)
    rd_intensity = _safe_div(rd, rev_ttm)

    # RD capitalization rate
    rd_cap_rate = None
    if rd is not None and rd_cap is not None:
        rd_total = _round(rd + rd_cap)
        rd_cap_rate = _safe_div(rd_cap, rd_total)

    # Revenue YoY growth
    rev_growth = _safe_div((rev_ttm or 0) - (rev_ly or 0), abs(rev_ly)) if (rev_ttm is not None and rev_ly is not None) else None
    np_growth = _safe_div((np_ttm or 0) - (np_ly or 0), abs(np_ly)) if (np_ttm is not None and np_ly is not None) else None

    return {
        "dupont": {
            "net_margin": mk(net_margin),
            "net_margin_yoy_direction": net_margin_yoy,
            "asset_turnover": mk(asset_turnover),
            "asset_turnover_yoy_direction": at_yoy,
            "equity_multiplier": mk(equity_multiplier),
            "equity_multiplier_yoy_direction": em_yoy,
        },
        "inventory_turnover": mk(inv_turnover),
        "financial_safety": {
            "interest_bearing_debt_ratio": mk(ibd_ratio),
            "cash_coverage_ratio": mk(cash_coverage),
        },
        "investment_intensity": {
            "capex_to_revenue": mk(capex_ratio),
            "rd_intensity": mk(rd_intensity),
            "rd_capitalization_rate": mk(rd_cap_rate) if rd_cap is not None else _unavail("akshare", rp),
        },
        "growth_quality": {
            "revenue_growth": mk(rev_growth),
            "net_profit_growth": mk(np_growth),
        },
    }


def calc_beta(market: Dict, errors: List) -> Optional[float]:
    """Compute beta via OLS regression of stock vs index daily returns."""
    if not _SCI_OK or not _PD_OK:
        errors.append({"indicator": "beta", "error": "scipy/pandas not installed"})
        return None
    stock_hist = market.get("stock_hist")
    index_hist = market.get("index_hist")
    if stock_hist is None or index_hist is None:
        errors.append({"indicator": "beta", "error": "Missing price history"})
        return None
    try:
        sc = _col(stock_hist, "收盘", "close", "Close")
        ic = _col(index_hist, "收盘", "close", "Close")
        sd = _col(stock_hist, "日期", "date", "Date")
        id_ = _col(index_hist, "日期", "date", "Date")
        if not all([sc, ic, sd, id_]):
            errors.append({"indicator": "beta", "error": "Cannot find close/date columns"})
            return None

        s = stock_hist[[sd, sc]].copy().rename(columns={sd: "date", sc: "price"})
        i = index_hist[[id_, ic]].copy().rename(columns={id_: "date", ic: "price"})
        s["date"] = pd.to_datetime(s["date"])
        i["date"] = pd.to_datetime(i["date"])
        s = s.sort_values("date").set_index("date")
        i = i.sort_values("date").set_index("date")

        sr = s["price"].pct_change().dropna()
        ir = i["price"].pct_change().dropna()
        combined = pd.concat([sr, ir], axis=1, keys=["stock", "index"]).dropna()

        if len(combined) < 30:
            errors.append({"indicator": "beta", "error": f"Insufficient data: {len(combined)} obs"})
            return None

        slope, _, _, _, _ = linregress(combined["index"], combined["stock"])
        beta = _round(slope, 4)
        log.info(f"  Beta (OLS, {len(combined)} obs): {beta}")
        return beta
    except Exception as e:
        errors.append({"indicator": "beta", "error": str(e)})
        return None


def calc_wacc(fin: Dict, market: Dict, macro: Dict,
              beta: Optional[float], errors: List) -> Dict:
    """Compute WACC and all components."""
    today = _today_str()
    unavail_wacc = {
        "wacc": _unavail("derived", today),
        "wacc_components": {
            "rf": _unavail("macro_snapshot"), "erp": _unavail("macro_snapshot"),
            "beta": _unavail("derived"), "re": _unavail("derived"),
            "rd": _unavail("derived"), "tax_rate": _unavail("derived"),
            "equity_weight": _unavail("derived"), "debt_weight": _unavail("derived"),
        }
    }

    rf = macro.get("rf")
    erp = macro.get("erp")
    if rf is None or erp is None:
        errors.append({"indicator": "wacc", "error": "Rf or ERP unavailable from macro_snapshot"})
        return unavail_wacc

    if beta is None:
        errors.append({"indicator": "wacc", "error": "Beta unavailable"})
        return unavail_wacc

    try:
        # Re = Rf + β × ERP
        re = _round(rf + beta * erp, 6)

        # Rd = interest expense / avg(interest-bearing debt)
        ie = fin.get("interest_expense")
        ibd = None
        parts = [x for x in [fin.get("short_term_debt"), fin.get("long_term_debt"),
                               fin.get("bonds_payable"), fin.get("current_noncurrent_debt")] if x is not None]
        if parts:
            ibd = sum(parts)
        ibd_ly = fin.get("interest_bearing_debt_ly")
        ibd_avg = None
        if ibd is not None and ibd_ly is not None:
            ibd_avg = (ibd + ibd_ly) / 2
        elif ibd is not None:
            ibd_avg = ibd
        rd = _safe_div(ie, ibd_avg) if ibd_avg else None

        # Tax rate
        tax_expense = fin.get("tax_expense")
        pbt = fin.get("profit_before_tax")
        tax_rate = _safe_div(tax_expense, pbt)
        if tax_rate is not None:
            tax_rate = min(max(tax_rate, 0), 0.5)  # clamp to [0, 50%]

        # Equity and debt weights using market equity + book debt
        market_cap = market.get("market_cap")
        if market_cap is None:
            errors.append({"indicator": "wacc", "error": "Market cap unavailable"})
            return unavail_wacc

        debt = ibd or 0
        total_capital = market_cap + debt
        e_weight = _safe_div(market_cap, total_capital)
        d_weight = _safe_div(debt, total_capital)

        # WACC
        if None in [re, rd, tax_rate, e_weight, d_weight]:
            errors.append({"indicator": "wacc",
                           "error": f"Missing component: re={re}, rd={rd}, t={tax_rate}, ew={e_weight}, dw={d_weight}"})
            wacc_val = None
        else:
            wacc_val = _round(re * e_weight + rd * (1 - tax_rate) * d_weight, 6)

        log.info(f"  WACC={wacc_val}, Re={re}, Rd={rd}, T={tax_rate}")
        return {
            "wacc": _mk(wacc_val, "ok" if wacc_val is not None else "unavailable", "derived", today),
            "wacc_components": {
                "rf": {"value": rf, "source": "macro_snapshot"},
                "erp": {"value": erp, "source": "macro_snapshot"},
                "beta": {"value": beta, "source": "derived"},
                "re": {"value": re, "source": "derived"},
                "rd": {"value": rd, "source": "derived"},
                "tax_rate": {"value": tax_rate, "source": "derived"},
                "equity_weight": {"value": e_weight, "source": "derived"},
                "debt_weight": {"value": d_weight, "source": "derived"},
            }
        }
    except Exception as e:
        errors.append({"indicator": "wacc", "error": str(e)})
        return unavail_wacc


def calc_q3_indicators(fin: Dict, market: Dict, consensus: Dict,
                       wacc_data: Dict, errors: List) -> Dict:
    """Q3: Valuation indicators."""
    today = _today_str()
    rp = fin.get("report_period", "")

    price = market.get("current_price")
    pe_ttm = market.get("pe_ttm")
    np_ttm = fin.get("net_profit_ttm")

    fy1_eps = consensus.get("fy1_eps")
    fy2_eps = consensus.get("fy2_eps")
    con_source = consensus.get("source", "unavailable")

    def mk(v, src="derived"): return _mk(v, "ok" if v is not None else "unavailable", src, today)
    def mk_con(v): return _mk(v, "ok" if v is not None else "unavailable", con_source, today)

    # TTM EPS (derived from PE and price or from net profit / shares)
    ttm_eps = None
    if price is not None and pe_ttm is not None and pe_ttm != 0:
        ttm_eps = _round(price / pe_ttm)

    # TTM PE (cross-check)
    ttm_pe = _round(price / ttm_eps) if (price and ttm_eps) else pe_ttm

    # Forward PE
    fpe1 = _safe_div(price, fy1_eps)
    fpe2 = _safe_div(price, fy2_eps)

    # FY+1 growth (vs TTM EPS)
    fy1_growth = _safe_div((fy1_eps or 0) - (ttm_eps or 0), abs(ttm_eps)) if (fy1_eps and ttm_eps) else None
    # FY+2 growth (vs FY+1)
    fy2_growth = _safe_div((fy2_eps or 0) - (fy1_eps or 0), abs(fy1_eps)) if (fy2_eps and fy1_eps) else None

    # Two-year CAGR: (FY+2 EPS / TTM EPS)^0.5 - 1
    two_yr_cagr = None
    if fy2_eps and ttm_eps and ttm_eps > 0 and fy2_eps > 0:
        two_yr_cagr = _round((fy2_eps / ttm_eps) ** 0.5 - 1, 4)

    # PEG short-term: Forward PE FY+1 / (FY+1 growth * 100)
    peg_short = _safe_div(fpe1, (fy1_growth * 100)) if (fpe1 and fy1_growth and fy1_growth > 0) else None
    # PEG medium-term: Forward PE FY+2 / (two_yr_cagr * 100)
    peg_med = _safe_div(fpe2, (two_yr_cagr * 100)) if (fpe2 and two_yr_cagr and two_yr_cagr > 0) else None

    # Relative PE percentile
    relative_pe_pct = None
    stock_pe_hist = market.get("stock_pe_hist")
    csi300_pe_hist = market.get("csi300_pe_hist")
    csi300_pe_cur = market.get("csi300_pe_current")
    if stock_pe_hist is not None and csi300_pe_hist is not None and csi300_pe_cur and pe_ttm:
        try:
            speh = stock_pe_hist.set_index("date")["pe"]
            cpeh = csi300_pe_hist.set_index("date")["pe"]
            combined = pd.concat([speh, cpeh], axis=1, keys=["stock_pe", "csi_pe"]).dropna()
            combined = combined[(combined["stock_pe"] > 0) & (combined["csi_pe"] > 0)]
            combined["rel_pe"] = combined["stock_pe"] / combined["csi_pe"]
            current_rel_pe = pe_ttm / csi300_pe_cur if csi300_pe_cur else None
            if current_rel_pe and len(combined) > 0:
                relative_pe_pct = _round((combined["rel_pe"] < current_rel_pe).mean(), 4)
        except Exception as e:
            errors.append({"indicator": "relative_pe_percentile", "error": str(e)})

    # Consensus revision (3-month)
    snap_3m = load_consensus_snapshot_3m()
    rev_fy1 = _insuf(today) if snap_3m is None else \
        _mk(_safe_div((fy1_eps or 0) - (snap_3m.get("fy1_eps") or 0),
                      abs(snap_3m.get("fy1_eps") or 1)), "ok", "local_snapshot", today)
    rev_fy2 = _insuf(today) if snap_3m is None else \
        _mk(_safe_div((fy2_eps or 0) - (snap_3m.get("fy2_eps") or 0),
                      abs(snap_3m.get("fy2_eps") or 1)), "ok", "local_snapshot", today)

    return {
        "ttm_pe": mk(ttm_pe),
        "forward_pe_fy1": mk(fpe1),
        "forward_pe_fy2": mk(fpe2),
        "peg_short_term": mk(peg_short),
        "peg_medium_term": mk(peg_med),
        "two_year_cagr": mk(two_yr_cagr),
        "relative_pe_percentile": mk(relative_pe_pct),
        "current_price": _mk(price, "ok" if price else "unavailable", "akshare", today),
        "consensus": {
            "fy1_eps": mk_con(fy1_eps),
            "fy2_eps": mk_con(fy2_eps),
            "fy1_net_profit": mk_con(consensus.get("fy1_net_profit")),
            "fy2_net_profit": mk_con(consensus.get("fy2_net_profit")),
            "fy1_growth": mk(fy1_growth),
            "fy2_growth": mk(fy2_growth),
            "fy1_eps_revision_3m": rev_fy1,
            "fy2_eps_revision_3m": rev_fy2,
        },
        "_ttm_eps_internal": ttm_eps,  # used by value_range
    }


def calc_value_range(q3: Dict, errors: List) -> Dict:
    """Calculate price value range using Method A (relative PE) and Method B (PEG)."""
    today = _today_str()
    unavail = lambda: _unavail("derived", today)

    stock_pe_hist = None
    csi300_pe_cur = None

    # We need to pass market data in; handled by accessing q3 internal fields
    # Method A: need 25/75 percentile of relative PE history * csi300 current PE
    # These come from market data; we'll accept None if not available

    fy2_eps = q3.get("consensus", {}).get("fy2_eps", {}).get("value")
    two_yr_cagr = q3.get("two_year_cagr", {}).get("value")

    # Method B: PEG-based
    method_b_pe_low = _round(two_yr_cagr * 100) if two_yr_cagr and two_yr_cagr > 0 else None
    method_b_pe_high = _round(two_yr_cagr * 150) if two_yr_cagr and two_yr_cagr > 0 else None

    price_b_low = _safe_div(fy2_eps * method_b_pe_low, 1) if (fy2_eps and method_b_pe_low) else None
    price_b_high = _safe_div(fy2_eps * method_b_pe_high, 1) if (fy2_eps and method_b_pe_high) else None

    return {
        "method_a_pe_low": unavail(),    # Filled by calc_value_range_full which has market access
        "method_a_pe_high": unavail(),
        "method_b_pe_low": _mk(method_b_pe_low, "ok" if method_b_pe_low else "unavailable", "derived", today),
        "method_b_pe_high": _mk(method_b_pe_high, "ok" if method_b_pe_high else "unavailable", "derived", today),
        "price_range_low": _mk(price_b_low, "ok" if price_b_low else "unavailable", "derived", today),
        "price_range_high": _mk(price_b_high, "ok" if price_b_high else "unavailable", "derived", today),
        "has_intersection": None,
        "current_price_position": None,
    }


def calc_value_range_full(q3: Dict, market: Dict, errors: List) -> Dict:
    """Full value range calculation with market data access."""
    today = _today_str()

    fy2_eps = q3.get("consensus", {}).get("fy2_eps", {}).get("value")
    two_yr_cagr = q3.get("two_year_cagr", {}).get("value")
    current_price = q3.get("current_price", {}).get("value")
    csi300_pe_cur = market.get("csi300_pe_current")

    stock_pe_hist = market.get("stock_pe_hist")
    csi300_pe_hist = market.get("csi300_pe_hist")

    def mk(v): return _mk(v, "ok" if v is not None else "unavailable", "derived", today)
    def unavail(): return _unavail("derived", today)

    # Method A: relative PE percentile 25/75 × CSI300 current PE
    method_a_pe_low, method_a_pe_high = None, None
    if stock_pe_hist is not None and csi300_pe_hist is not None and csi300_pe_cur:
        try:
            speh = stock_pe_hist.set_index("date")["pe"]
            cpeh = csi300_pe_hist.set_index("date")["pe"]
            combined = pd.concat([speh, cpeh], axis=1, keys=["s", "c"]).dropna()
            combined = combined[(combined["s"] > 0) & (combined["c"] > 0)]
            rel = combined["s"] / combined["c"]
            p25 = _round(float(rel.quantile(0.25)), 4)
            p75 = _round(float(rel.quantile(0.75)), 4)
            method_a_pe_low = _round(p25 * csi300_pe_cur)
            method_a_pe_high = _round(p75 * csi300_pe_cur)
        except Exception as e:
            errors.append({"indicator": "value_range_method_a", "error": str(e)})

    # Method B: PEG-based PE
    method_b_pe_low = _round(two_yr_cagr * 100) if two_yr_cagr and two_yr_cagr > 0 else None
    method_b_pe_high = _round(two_yr_cagr * 150) if two_yr_cagr and two_yr_cagr > 0 else None

    # Price ranges
    price_a_low = _round(fy2_eps * method_a_pe_low) if (fy2_eps and method_a_pe_low) else None
    price_a_high = _round(fy2_eps * method_a_pe_high) if (fy2_eps and method_a_pe_high) else None
    price_b_low = _round(fy2_eps * method_b_pe_low) if (fy2_eps and method_b_pe_low) else None
    price_b_high = _round(fy2_eps * method_b_pe_high) if (fy2_eps and method_b_pe_high) else None

    # Intersection
    has_intersection = None
    final_low, final_high = None, None
    if all(x is not None for x in [price_a_low, price_a_high, price_b_low, price_b_high]):
        inter_low = max(price_a_low, price_b_low)
        inter_high = min(price_a_high, price_b_high)
        has_intersection = inter_low <= inter_high
        if has_intersection:
            final_low, final_high = inter_low, inter_high
        else:
            final_low = min(price_a_low, price_b_low)
            final_high = max(price_a_high, price_b_high)
    elif price_b_low is not None:
        final_low, final_high = price_b_low, price_b_high
    elif price_a_low is not None:
        final_low, final_high = price_a_low, price_a_high

    # Current price position
    position = None
    if current_price and final_low and final_high:
        if current_price < final_low:
            position = "below_range"
        elif current_price > final_high:
            position = "above_range"
        else:
            position = "in_range"

    return {
        "method_a_pe_low": mk(method_a_pe_low),
        "method_a_pe_high": mk(method_a_pe_high),
        "method_b_pe_low": mk(method_b_pe_low),
        "method_b_pe_high": mk(method_b_pe_high),
        "price_range_a_low": mk(price_a_low),
        "price_range_a_high": mk(price_a_high),
        "price_range_b_low": mk(price_b_low),
        "price_range_b_high": mk(price_b_high),
        "price_range_low": mk(final_low),
        "price_range_high": mk(final_high),
        "has_intersection": has_intersection,
        "current_price_position": position,
    }


def calc_reverse_dcf(fin: Dict, market: Dict, wacc_data: Dict, errors: List) -> Dict:
    """Compute reverse DCF implied growth rate."""
    today = _today_str()
    rp = fin.get("report_period", "")
    unavail = lambda k: _unavail("derived", today)

    ocf = fin.get("ocf_ttm")
    capex = fin.get("capex_ttm")
    market_cap = market.get("market_cap")
    wacc_val = wacc_data.get("wacc", {}).get("value")

    fcf = None
    if ocf is not None and capex is not None:
        fcf = _round(ocf - capex)

    fcf_result = _mk(fcf, "ok" if fcf is not None else "unavailable", "derived", rp)

    if not _SCI_OK:
        errors.append({"indicator": "reverse_dcf", "error": "scipy not installed"})
        return {"ttm_fcf": fcf_result, "implied_growth_rate": _unavail("derived", today), "vs_consensus_cagr": None}

    if wacc_val is None or fcf is None or market_cap is None:
        missing = [k for k, v in [("wacc", wacc_val), ("fcf", fcf), ("market_cap", market_cap)] if v is None]
        errors.append({"indicator": "reverse_dcf", "error": f"Missing: {missing}"})
        return {"ttm_fcf": fcf_result, "implied_growth_rate": _unavail("derived", today), "vs_consensus_cagr": None}

    # Net debt
    ibd_parts = [x for x in [fin.get("short_term_debt"), fin.get("long_term_debt"),
                               fin.get("bonds_payable"), fin.get("current_noncurrent_debt")] if x is not None]
    ibd = sum(ibd_parts) if ibd_parts else 0
    cash = (fin.get("cash") or 0) + (fin.get("trading_financial_assets") or 0)
    net_debt = max(ibd - cash, 0)
    ev = market_cap + net_debt

    g = PERPETUAL_GROWTH_RATE
    n = DCF_PROJECTION_YEARS
    w = wacc_val

    if ev <= 0 or fcf == 0:
        errors.append({"indicator": "reverse_dcf", "error": f"Invalid EV={ev} or FCF={fcf}"})
        return {"ttm_fcf": fcf_result, "implied_growth_rate": _unavail("derived", today), "vs_consensus_cagr": None}

    try:
        def dcf_eq(g_imp):
            if abs(g_imp - w) < 1e-9:
                return 1e10
            pv = sum(fcf * (1 + g_imp) ** t / (1 + w) ** t for t in range(1, n + 1))
            tv = fcf * (1 + g_imp) ** n * (1 + g) / ((w - g) * (1 + w) ** n)
            return pv + tv - ev

        # Find bracket where function changes sign
        g_imp = None
        try:
            g_imp = brentq(dcf_eq, -0.9, 5.0, xtol=1e-6, maxiter=500)
        except ValueError:
            errors.append({"indicator": "reverse_dcf",
                           "error": "brentq: no sign change in [-0.9, 5.0]. EV may be too high/low relative to FCF."})

        implied = _round(g_imp, 4) if g_imp is not None else None
        log.info(f"  Reverse DCF implied growth: {implied}")
        return {
            "ttm_fcf": fcf_result,
            "implied_growth_rate": _mk(implied, "ok" if implied is not None else "unavailable", "derived", today),
            "vs_consensus_cagr": None,  # filled in build_output
        }
    except Exception as e:
        errors.append({"indicator": "reverse_dcf", "error": str(e)})
        return {"ttm_fcf": fcf_result, "implied_growth_rate": _unavail("derived", today), "vs_consensus_cagr": None}


# ─── 输出构建 ──────────────────────────────────────────────────────────────────────

def build_output(fin: Dict, market: Dict, consensus: Dict, macro: Dict,
                 q1: Dict, q2: Dict, q3: Dict, value_range: Dict,
                 wacc_data: Dict, reverse_dcf: Dict, errors: List) -> Dict:
    """Assemble final JSON output."""
    rp = fin.get("report_period", "unknown")
    today = _today_str()
    generated_at = datetime.now().astimezone().isoformat()

    # vs_consensus_cagr annotation
    implied_g = reverse_dcf.get("implied_growth_rate", {}).get("value")
    con_cagr = q3.get("two_year_cagr", {}).get("value")
    vs_cagr = None
    if implied_g is not None and con_cagr is not None:
        if implied_g > con_cagr * 1.1:
            vs_cagr = "implied growth significantly higher than consensus CAGR"
        elif implied_g < con_cagr * 0.9:
            vs_cagr = "implied growth significantly lower than consensus CAGR"
        else:
            vs_cagr = "implied growth roughly in line with consensus CAGR"
    reverse_dcf["vs_consensus_cagr"] = vs_cagr

    return {
        "meta": {
            "generated_at": generated_at,
            "stock_code": STOCK_CODE,
            "stock_name": STOCK_NAME,
            "index_code": INDEX_CODE,
            "asset_type": ASSET_TYPE,
            "margin_reference": MARGIN_REFERENCE,
            "capex_reference": CAPEX_REFERENCE,
            "latest_report_period": rp,
            "read_instructions": (
                "本文件包含企业财务估值的三层递进分析数据。"
                "请按以下顺序阅读：【问题一】先看扣非比(non_recurring_ratio)定底色，"
                "再看现金流比(ocf_to_net_profit)和应收周转率(ar_turnover)交叉验证，"
                "最后看商誉(goodwill_to_equity)排雷，输出一个利润真实性的定性结论。"
                "【问题二】看DuPont三因子的同比变动方向组合判断经营趋势，"
                "叠加存货周转率定位问题，看资本开支和研发强度趋势判断投入产出效率，"
                "看增速劈叉(revenue_growth vs net_profit_growth)判断增长质量，"
                "cash_coverage_ratio<1直接标红。"
                "【问题三】先看FY+1 Forward PE获得绝对水位直觉，再看PEG短期判断一年期赔率，"
                "看PEG中期判断长期资金视角，看预期修正方向判断预期差，"
                "看relative_pe_percentile确认情绪位置，"
                "最后看价值区间定位当前价格，用反向DCF隐含增速做校验。"
                "PEG短期合理但PEG中期偏贵时要特别警惕。问题一二的结论必须叠加到问题三的判断中。"
            ),
        },
        "q1_profit_quality": q1,
        "q2_operating_trend": q2,
        "q3_valuation": {
            "forward_pe_fy1": q3["forward_pe_fy1"],
            "forward_pe_fy2": q3["forward_pe_fy2"],
            "peg_short_term": q3["peg_short_term"],
            "peg_medium_term": q3["peg_medium_term"],
            "two_year_cagr": q3["two_year_cagr"],
            "ttm_pe": q3["ttm_pe"],
            "relative_pe_percentile": q3["relative_pe_percentile"],
            "current_price": q3["current_price"],
            "consensus": q3["consensus"],
            "value_range": value_range,
            "reverse_dcf": {
                **wacc_data,
                "ttm_fcf": reverse_dcf["ttm_fcf"],
                "implied_growth_rate": reverse_dcf["implied_growth_rate"],
                "vs_consensus_cagr": vs_cagr,
            },
        },
        "errors": errors,
    }


# ─── 主函数 ───────────────────────────────────────────────────────────────────────

def run() -> Dict:
    """Main entry point: fetch, calculate, return snapshot dict."""
    errors: List[Dict] = []
    log.info(f"=== fetch_finance start: {STOCK_NAME} ({STOCK_CODE}) ===")

    # ── 并行采集四组原始数据 ──
    raw: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            "fs_raw": pool.submit(fetch_financial_statements, errors),
            "market": pool.submit(fetch_market_data, errors),
            "consensus": pool.submit(fetch_consensus, errors),
            "macro": pool.submit(read_macro_snapshot, errors),
        }
        for key, future in futures.items():
            try:
                raw[key] = future.result()
            except Exception as e:
                errors.append({"indicator": key, "error": str(e)})
                raw[key] = {}

    # ── 从财报 DataFrames 提取原始字段 ──
    fin = _extract_financials(raw.get("fs_raw", {}), errors)
    fin["report_period"] = raw.get("fs_raw", {}).get("report_period", "")
    market = raw.get("market", {})
    consensus = raw.get("consensus", {})
    macro = raw.get("macro", {})

    # ── 串行计算衍生指标 ──
    q1 = calc_q1_indicators(fin, errors)
    q2 = calc_q2_indicators(fin, errors)
    beta = calc_beta(market, errors)
    wacc_data = calc_wacc(fin, market, macro, beta, errors)
    q3 = calc_q3_indicators(fin, market, consensus, wacc_data, errors)
    value_range = calc_value_range_full(q3, market, errors)
    reverse_dcf = calc_reverse_dcf(fin, market, wacc_data, errors)

    # ── 保存一致预期快照 ──
    save_consensus_snapshot(consensus)

    # ── 组装输出 ──
    snapshot = build_output(
        fin, market, consensus, macro,
        q1, q2, q3, value_range,
        wacc_data, reverse_dcf, errors
    )

    log.info(f"=== fetch_finance done. Errors: {len(errors)} ===")
    return snapshot


def main():
    snapshot = run()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Output saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
