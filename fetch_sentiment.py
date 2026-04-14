"""
fetch_sentiment.py
A股市场情绪监控 — 数据采集脚本
职责：可靠地获取数据、处理错误、输出干净的 JSON
输出：./output/sentiment_snapshot.json

列名已于 2026-04-13 通过实际接口调用确认。
"""

import json
import os
import time
import logging
import random
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

# 禁用代理设置，避免连接错误
# 清除所有可能的代理环境变量
proxy_env_vars = [
    'HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
    'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy'
]
for var in proxy_env_vars:
    if var in os.environ:
        del os.environ[var]

# ─── 第三方库（软导入）─────────────────────────────────────────────────────────

try:
    import akshare as ak
    _AK_OK = True
except ImportError:
    _AK_OK = False

try:
    import pandas as pd
    import numpy as np
    _PD_OK = True
except ImportError:
    _PD_OK = False

# ─── 常量区 ───────────────────────────────────────────────────────────────────

OUTPUT_DIR           = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
OUTPUT_FILE          = os.path.join(OUTPUT_DIR, "sentiment_snapshot.json")
TURNOVER_HISTORY_FILE = os.path.join(OUTPUT_DIR, "sentiment_turnover_history.json")
BREADTH_HISTORY_FILE  = os.path.join(OUTPUT_DIR, "sentiment_breadth_history.json")

TIMEOUT                    = 15   # 默认超时秒数
TIMEOUT_SPOT               = 30   # 全A行情专用超时
RETRY_COUNT                = 3    # 重试次数
RETRY_INTERVAL             = 5    # 重试间隔秒数
INDEX_LOOKBACK_DAYS        = 650  # 指数数据回看天数（用于成交额分位数）
MARGIN_LOOKBACK_DAYS       = 504  # 融资数据回看天数
QVIX_LOOKBACK_DAYS         = 504  # QVIX 分位数回看天数
BREADTH_MAX_HISTORY        = 120  # 涨跌比历史保留天数
BREADTH_MIN_FOR_PERCENTILE = 60   # 涨跌比分位数最少数据量
TURNOVER_MAX_HISTORY       = 800  # 成交额历史最大保留条数（约3年交易日）
TURNOVER_MIN_FOR_MA120     = 120  # MA120 最少数据量

READ_INSTRUCTIONS = (
    "本文件包含A股市场情绪监控数据。"
    "daily_activity 和 market_breadth 组合判断市场热度和方向："
    "放量+高涨跌比=狂热，放量+低涨跌比=恐慌抛售，缩量+低涨跌比=冷淡。"
    "leverage 中的融资买入占比衡量杠杆资金参与度。"
    "volatility 中的 QVIX 是恐慌独立确认项，飙升=恐慌。"
    "fund_flow 看资金进出动能。"
    "fund_reserve 是月度背景变量，高位=潜在燃料充足。"
    "derived 中的 percentile_snapshot 提供各核心指标的历史分位数，用于判断当前水平在历史中的位置。"
    "注：北向资金自2024-08-16起监管停止披露净买入数据，相关字段为unavailable。"
)

# ─── 日志 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# 尝试修改 requests 库的全局设置，确保所有请求都不使用代理
try:
    import requests
    # 禁用所有代理
    requests.Session().proxies = {}
    # 修改 requests.get 方法，添加浏览器请求头并强制禁用代理
    original_get = requests.get
    
    def patched_get(*args, **kwargs):
        # 添加合理的请求头
        headers = kwargs.get('headers', {})
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        kwargs['headers'] = headers
        # 强制禁用代理
        kwargs['proxies'] = {}
        # 增加超时设置
        kwargs.setdefault('timeout', 30)
        return original_get(*args, **kwargs)
    
    # 替换 requests.get
    requests.get = patched_get
    # 同时替换 requests.Session.get
    original_session_get = requests.Session.get
    def patched_session_get(self, *args, **kwargs):
        # 添加合理的请求头
        headers = kwargs.get('headers', {})
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        kwargs['headers'] = headers
        # 强制禁用代理
        kwargs['proxies'] = {}
        # 增加超时设置
        kwargs.setdefault('timeout', 30)
        return original_session_get(self, *args, **kwargs)
    requests.Session.get = patched_session_get
    log.info("已修改 requests 全局设置，禁用代理并添加浏览器请求头")
except Exception as e:
    log.warning(f"修改 requests 设置失败: {e}")

# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _unavail(source: str, reason: str) -> Dict[str, Any]:
    """标准 unavailable 结构。"""
    return {
        "value": None,
        "status": "unavailable",
        "source": source,
        "as_of": None,
        "reason": reason,
    }


def _metric(
    value: Any,
    source: str,
    as_of: Optional[str],
    unit: Optional[str] = None,
    **extras: Any,
) -> Dict[str, Any]:
    """构建标准指标字典。"""
    d: Dict[str, Any] = {
        "value": value,
        "status": "ok" if value is not None else "unavailable",
        "source": source,
        "as_of": as_of,
    }
    if unit is not None:
        d["unit"] = unit
    d.update(extras)
    return d


def _round(val: Any, n: int = 4) -> Optional[float]:
    try:
        return round(float(val), n)
    except (TypeError, ValueError):
        return None


def _today() -> str:
    return date.today().isoformat()


def _with_retry(fn, *args, retries: int = RETRY_COUNT, delay: int = RETRY_INTERVAL, **kwargs):
    """简单重试包装器：最多重试 retries 次，间隔 delay 秒。"""
    last_exc: Exception = RuntimeError("unknown")
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                # 增加随机延迟，避免被服务器识别为自动化请求
                random_delay = delay + random.uniform(0, 2)
                log.warning(
                    f"{fn.__name__} 第{attempt + 1}次失败: {exc}，{random_delay:.1f}s 后重试…"
                )
                time.sleep(random_delay)
    raise last_exc


def _percentile_rank(series: List[float], value: float) -> Optional[float]:
    """计算 value 在 series 中的百分位排名（0-100，低于历史多少%）。"""
    if not _PD_OK:
        return None
    arr = np.array([x for x in series if x is not None and not np.isnan(float(x))], dtype=float)
    if len(arr) == 0 or value is None:
        return None
    return round(float((arr < float(value)).mean() * 100), 1)


def _parse_yyyymm(s: str) -> str:
    """将 '2026年02月份' 或 '2026-02' 统一转为 'YYYY-MM'。"""
    s = str(s).strip()
    if "年" in s:
        year = s[:4]
        month = s[5:7]
        return f"{year}-{month}"
    return s[:7]


def _load_existing_snapshot() -> Tuple[List[Dict], List[Dict]]:
    """从独立历史文件读取 breadth_history 和 turnover_history。
    兼容旧版：若独立文件不存在则从 sentiment_snapshot.json 中迁移。
    文件不存在或格式错误时返回空列表。
    """
    def _read_json_list(path: str) -> List[Dict]:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []

    breadth  = _read_json_list(BREADTH_HISTORY_FILE)
    turnover = _read_json_list(TURNOVER_HISTORY_FILE)

    # 兼容迁移：若独立文件为空，尝试从旧 snapshot 读取
    if not breadth or not turnover:
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                old = json.load(f)
            if not breadth:
                breadth  = old.get("breadth_history", [])
            if not turnover:
                turnover = old.get("turnover_history", [])
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass

    return breadth, turnover

# ─── 数据采集函数 ─────────────────────────────────────────────────────────────
# 每个函数独立 try/except；失败时返回 unavailable dict，不抛异常到调用层。
# 返回 Tuple[str, Any]：key + 原始数据（DataFrame 或 dict）。

def fetch_sh_index() -> Tuple[str, Any]:
    """沪市上证指数日行情，过去 INDEX_LOOKBACK_DAYS 个交易日数据。
    确认列名（2026-04-13）：date, open, close, high, low, volume, amount
    amount 单位：元，÷1e8 → 亿元。数据升序，tail=最新。
    """
    log.info("Fetching SH index (sh000001) …")
    src = "akshare:stock_zh_index_daily_em"
    if not (_AK_OK and _PD_OK):
        return "SH_INDEX", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.stock_zh_index_daily_em, symbol="sh000001")
        df = df.tail(INDEX_LOOKBACK_DAYS).copy().reset_index(drop=True)
        df["amount_yi"] = pd.to_numeric(df["amount"], errors="coerce") / 1e8
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return "SH_INDEX", df
    except Exception as exc:
        log.error(f"fetch_sh_index failed: {exc}")
        return "SH_INDEX", _unavail(src, str(exc))


def fetch_sz_index() -> Tuple[str, Any]:
    """深市成指日行情，过去 INDEX_LOOKBACK_DAYS 个交易日数据。
    确认列名（2026-04-13）：date, open, close, high, low, volume, amount
    amount 单位：元，÷1e8 → 亿元。数据升序，tail=最新。
    """
    log.info("Fetching SZ index (sz399001) …")
    src = "akshare:stock_zh_index_daily_em"
    if not (_AK_OK and _PD_OK):
        return "SZ_INDEX", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.stock_zh_index_daily_em, symbol="sz399001")
        df = df.tail(INDEX_LOOKBACK_DAYS).copy().reset_index(drop=True)
        df["amount_yi"] = pd.to_numeric(df["amount"], errors="coerce") / 1e8
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return "SZ_INDEX", df
    except Exception as exc:
        log.error(f"fetch_sz_index failed: {exc}")
        return "SZ_INDEX", _unavail(src, str(exc))


def _count_advance_decline(df: "pd.DataFrame", col_chg: str, col_vol: str) -> Dict[str, Any]:
    """从 spot DataFrame 统计涨/跌/平家数。"""
    valid = df[(pd.to_numeric(df[col_vol], errors="coerce") > 0) & df[col_chg].notna()].copy()
    advance = int((valid[col_chg] > 0).sum())
    decline = int((valid[col_chg] < 0).sum())
    flat    = int((valid[col_chg] == 0).sum())
    total   = advance + decline + flat
    ratio   = _round(advance / total, 4) if total > 0 else None
    return {"advance": advance, "decline": decline, "flat": flat,
            "advance_ratio": ratio, "as_of": _today()}


def fetch_spot_em() -> Tuple[str, Any]:
    """全A股实时行情，计算涨/跌/平家数及比例。
    主源：东方财富 stock_zh_a_spot_em（含沪深）
    备源：新浪财经 stock_zh_a_spot（含沪深北，过滤北交所后使用）
    列名（已确认 2026-04-13）：
      EM：代码, 名称, 最新价, 涨跌幅, 成交量, ...
      Sina：代码, 名称, 最新价, 涨跌幅, 成交量, ...（代码以 bj 开头为北交所）
    """
    if not (_AK_OK and _PD_OK):
        return "SPOT", _unavail("akshare:stock_zh_a_spot_em", "akshare or pandas not installed")

    # ── 主源：东方财富 ────────────────────────────────────────────────────
    log.info("Fetching spot market via East Money (stock_zh_a_spot_em) …")
    try:
        df = ak.stock_zh_a_spot_em()
        result = _count_advance_decline(df, "涨跌幅", "成交量")
        result["source"] = "akshare:stock_zh_a_spot_em"
        return "SPOT", result
    except Exception as exc_em:
        log.warning(f"stock_zh_a_spot_em failed ({exc_em})，切换新浪备源…")

    # ── 备源：新浪财经 ────────────────────────────────────────────────────
    log.info("Fetching spot market via Sina (stock_zh_a_spot) …")
    try:
        df = ak.stock_zh_a_spot()
        # 过滤北交所股票（代码以 bj 开头），只保留沪深
        df = df[~df["代码"].str.startswith("bj")].copy()
        result = _count_advance_decline(df, "涨跌幅", "成交量")
        result["source"] = "akshare:stock_zh_a_spot(sina-fallback)"
        return "SPOT", result
    except Exception as exc_sina:
        log.error(f"stock_zh_a_spot (Sina fallback) also failed: {exc_sina}")
        return "SPOT", _unavail("akshare:stock_zh_a_spot_em+stock_zh_a_spot", str(exc_em))


def fetch_margin_sse() -> Tuple[str, Any]:
    """沪市融资买入额，最近约 550 个日历日数据（覆盖 504 交易日分位数）。
    确认列名（2026-04-13）：信用交易日期(YYYYMMDD), 融资余额, 融资买入额, ...
    融资买入额单位：元，÷1e8 → 亿元。接口返回降序（最新在前）。
    """
    log.info("Fetching SSE margin data …")
    src = "akshare:stock_margin_sse"
    if not (_AK_OK and _PD_OK):
        return "MARGIN_SSE", _unavail(src, "akshare or pandas not installed")
    try:
        start = (date.today() - timedelta(days=780)).strftime("%Y%m%d")
        end   = date.today().strftime("%Y%m%d")
        df = _with_retry(ak.stock_margin_sse, start_date=start, end_date=end)
        df = df.copy()
        df["buy_yi"] = pd.to_numeric(df["融资买入额"], errors="coerce") / 1e8
        # 日期列为 YYYYMMDD 整数或字符串，统一转为 YYYY-MM-DD 字符串
        df["date_iso"] = (
            df["信用交易日期"]
            .astype(str)
            .str[:8]
            .apply(lambda s: f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) >= 8 else s)
        )
        # 接口返回降序，转为升序（tail=最新）
        df = df.sort_values("date_iso", ascending=True).reset_index(drop=True)
        return "MARGIN_SSE", df
    except Exception as exc:
        log.error(f"fetch_margin_sse failed: {exc}")
        return "MARGIN_SSE", _unavail(src, str(exc))


def fetch_margin_szse() -> Tuple[str, Any]:
    """深市融资买入额（单日查询）。
    确认列名（2026-04-13）：融资买入额, 融资余额, 融券卖出量, 融券余量, 融券余额, 融资融券余额
    融资买入额单位：亿元（已是）。无日期列，日期由参数传入。
    从今日起向前尝试，跳过周末，最多回溯 7 天。
    """
    log.info("Fetching SZSE margin data …")
    src = "akshare:stock_margin_szse"
    if not (_AK_OK and _PD_OK):
        return "MARGIN_SZSE", _unavail(src, "akshare or pandas not installed")

    for delta in range(10):
        d = date.today() - timedelta(days=delta)
        if d.weekday() >= 5:   # 跳过周六日
            continue
        d_str = d.strftime("%Y%m%d")
        try:
            # 不使用 _with_retry：该接口对无数据日期报 "Length mismatch"，
            # 属于数据缺失而非网络抖动，无需重试同一日期，直接换到前一日。
            df = ak.stock_margin_szse(date=d_str)
            if df.empty or "融资买入额" not in df.columns:
                continue
            val = _round(float(df["融资买入额"].iloc[0]), 2)
            if val is None or val <= 0:
                continue
            return "MARGIN_SZSE", {"buy_yi": val, "as_of": d.isoformat()}
        except Exception:
            continue

    return "MARGIN_SZSE", _unavail(src, "过去10天内无可用数据")


def fetch_sse_daily_turnover() -> Tuple[str, Any]:
    """沪市当日股票成交额（上交所官网，覆盖主板+科创板）。
    确认列名（2026-04-14）：单日情况, 股票, 主板A, 主板B, 科创板, 股票回购
    '股票'列下'成交金额'行的值即为全部沪市股票成交额，单位：亿元（已是）。
    从今日起向前回溯，跳过周末，最多8天；今日数据通常收盘后才可用。
    """
    log.info("Fetching SSE daily turnover (stock_sse_deal_daily) …")
    src = "akshare:stock_sse_deal_daily"
    if not (_AK_OK and _PD_OK):
        return "SSE_DAILY", _unavail(src, "akshare or pandas not installed")

    for delta in range(10):
        d = date.today() - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y%m%d")
        try:
            df = ak.stock_sse_deal_daily(date=d_str)
            if df.empty:
                continue
            row = df[df["单日情况"] == "成交金额"]
            if row.empty:
                continue
            val = _round(float(row["股票"].values[0]), 2)
            if val and val > 0:
                return "SSE_DAILY", {"total_yi": val, "as_of": d.isoformat()}
        except Exception:
            continue

    return "SSE_DAILY", _unavail(src, "过去10天无可用数据")


def fetch_szse_daily_turnover() -> Tuple[str, Any]:
    """深市当日股票成交额（深交所官网，覆盖主板+创业板）。
    确认列名（2026-04-14）：证券类别, 数量, 成交金额, 总市值, 流通市值
    '证券类别'=='股票'行的'成交金额'即为全部深市股票成交额，单位：元（÷1e8→亿元）。
    从今日起向前回溯，跳过周末，最多8天；今日数据通常收盘后才可用。
    """
    log.info("Fetching SZSE daily turnover (stock_szse_summary) …")
    src = "akshare:stock_szse_summary"
    if not (_AK_OK and _PD_OK):
        return "SZSE_DAILY", _unavail(src, "akshare or pandas not installed")

    for delta in range(10):
        d = date.today() - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y%m%d")
        try:
            df = ak.stock_szse_summary(date=d_str)
            if df.empty:
                continue
            row = df[df["证券类别"] == "股票"]
            if row.empty:
                continue
            val = _round(float(row["成交金额"].values[0]) / 1e8, 2)
            if val and val > 0:
                return "SZSE_DAILY", {"total_yi": val, "as_of": d.isoformat()}
        except Exception:
            continue

    return "SZSE_DAILY", _unavail(src, "过去10天无可用数据")


def fetch_qvix() -> Tuple[str, Any]:
    """300ETF QVIX 隐含波动率全量历史。
    确认列名（2026-04-13）：date, open, high, low, close
    数据升序，tail=最新；末行可能有 NaN，需过滤。
    """
    log.info("Fetching 300ETF QVIX …")
    src = "akshare:index_option_300etf_qvix"
    if not (_AK_OK and _PD_OK):
        return "QVIX", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.index_option_300etf_qvix)
        df = df.dropna(subset=["close"]).copy()
        if df.empty:
            return "QVIX", _unavail(src, "返回数据为空")
        latest = df.iloc[-1]
        val   = _round(float(latest["close"]), 2)
        as_of = str(latest["date"])[:10]
        hist  = df["close"].tail(QVIX_LOOKBACK_DAYS).tolist()
        return "QVIX", {"value": val, "as_of": as_of, "hist_close": hist}
    except Exception as exc:
        log.error(f"fetch_qvix failed: {exc}")
        return "QVIX", _unavail(src, str(exc))


def fetch_new_investors() -> Tuple[str, Any]:
    """新增投资者数量（中登公司月度数据）。
    确认列名（2026-04-13）：数据日期, 新增投资者-数量, ...
    单位：万户。数据升序，tail=最新。
    """
    log.info("Fetching new investor statistics …")
    src = "akshare:stock_account_statistics_em"
    if not (_AK_OK and _PD_OK):
        return "NEW_INVESTORS", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.stock_account_statistics_em)
        if df.empty:
            return "NEW_INVESTORS", _unavail(src, "返回数据为空")
        last  = df.iloc[-1]
        val   = _round(float(last["新增投资者-数量"]), 2)
        as_of = str(last["数据日期"])[:7]  # YYYY-MM
        return "NEW_INVESTORS", {"value": val, "as_of": as_of}
    except Exception as exc:
        log.error(f"fetch_new_investors failed: {exc}")
        return "NEW_INVESTORS", _unavail(src, str(exc))


def fetch_money_supply() -> Tuple[str, Any]:
    """M1/M2 同比增速（月度）。
    确认列名（2026-04-13）：月份(格式'2026年02月份'), 货币和准货币(M2)-同比增长, 货币(M1)-同比增长
    值为数值型（非字符串）。数据降序，head=最新。
    """
    log.info("Fetching M1/M2 money supply …")
    src = "akshare:macro_china_money_supply"
    if not (_AK_OK and _PD_OK):
        return "MONEY_SUPPLY", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.macro_china_money_supply)
        if df.empty:
            return "MONEY_SUPPLY", _unavail(src, "返回数据为空")
        last  = df.iloc[0]  # 降序，index 0 为最新
        m2    = _round(float(last["货币和准货币(M2)-同比增长"]), 2)
        m1    = _round(float(last["货币(M1)-同比增长"]), 2)
        as_of = _parse_yyyymm(last["月份"])
        return "MONEY_SUPPLY", {"m1_yoy": m1, "m2_yoy": m2, "as_of": as_of}
    except Exception as exc:
        log.error(f"fetch_money_supply failed: {exc}")
        return "MONEY_SUPPLY", _unavail(src, str(exc))


def fetch_rmb_deposit() -> Tuple[str, Any]:
    """居民新增储蓄存款同比增速（月度）。
    确认列名（2026-04-13）：月份(YYYY-MM), 新增储蓄存款-同比(字符串含'%')
    数据升序，tail=最新。
    注：本指标为新增储蓄存款同比，非存量余额同比，是 macro_rmb_deposit 接口最接近的可用列。
    """
    log.info("Fetching RMB deposit statistics …")
    src = "akshare:macro_rmb_deposit"
    if not (_AK_OK and _PD_OK):
        return "RMB_DEPOSIT", _unavail(src, "akshare or pandas not installed")
    try:
        df = _with_retry(ak.macro_rmb_deposit)
        if df.empty:
            return "RMB_DEPOSIT", _unavail(src, "返回数据为空")
        last  = df.iloc[-1]
        raw   = str(last["新增储蓄存款-同比"]).replace("%", "").strip()
        val   = _round(float(raw), 2)
        as_of = str(last["月份"])[:7]
        return "RMB_DEPOSIT", {"value": val, "as_of": as_of}
    except Exception as exc:
        log.error(f"fetch_rmb_deposit failed: {exc}")
        return "RMB_DEPOSIT", _unavail(src, str(exc))

# ─── 增量追加（breadth_history 专用）─────────────────────────────────────────

def update_breadth_history(
    existing: List[Dict],
    today_date: str,
    today_ratio: Optional[float],
) -> List[Dict]:
    """将当日涨跌比追加到历史列表，去重、倒序、截断至 BREADTH_MAX_HISTORY。"""
    if today_ratio is None:
        return existing
    entries: Dict[str, Dict] = {e["date"]: e for e in existing}
    entries[today_date] = {"date": today_date, "advance_ratio": today_ratio}
    sorted_list = sorted(entries.values(), key=lambda x: x["date"], reverse=True)
    return sorted_list[:BREADTH_MAX_HISTORY]

def update_turnover_history(
    existing: List[Dict],
    new_by_date: Dict[str, Dict],
) -> List[Dict]:
    """合并新成交额数据到历史列表，去重倒序截断至 TURNOVER_MAX_HISTORY。
    new_by_date: {日期字符串: {"total_yi": float, "source": str}}
    """
    entries: Dict[str, Dict] = {e["date"]: e for e in existing}
    for d_str, info in new_by_date.items():
        val = info.get("total_yi")
        if val is not None and val > 0:
            entries[d_str] = {"date": d_str, "total_yi": val, "source": info.get("source", "unknown")}
    sorted_list = sorted(entries.values(), key=lambda x: x["date"], reverse=True)
    return sorted_list[:TURNOVER_MAX_HISTORY]


# ─── 衍生计算 ─────────────────────────────────────────────────────────────────

def _build_turnover_metrics(
    raw: Dict,
    turnover_history: List[Dict],
) -> Tuple[Dict, Optional[str], Optional[float], List[float], Dict[str, Dict]]:
    """
    计算成交额相关指标，支持两级回退：
      Level 1 — 东方财富历史 DataFrame（SH_INDEX + SZ_INDEX，650日）
      Level 2 — 官方单日数据（SSE_DAILY + SZSE_DAILY）+ 存量 turnover_history
    返回：(daily_activity, latest_date, total_turnover_yi, ratio_series, new_turnover_by_date)
    new_turnover_by_date 用于 update_turnover_history()。
    """
    sh = raw.get("SH_INDEX")
    sz = raw.get("SZ_INDEX")
    sh_ok = _PD_OK and isinstance(sh, pd.DataFrame) and not sh.empty
    sz_ok = _PD_OK and isinstance(sz, pd.DataFrame) and not sz.empty

    # ── Level 1：东方财富全量历史 ─────────────────────────────────────────
    if sh_ok and sz_ok:
        sh_df = sh.set_index("date")["amount_yi"].rename("sh")
        sz_df = sz.set_index("date")["amount_yi"].rename("sz")
        total = (sh_df + sz_df).dropna().sort_index()

        if len(total) >= TURNOVER_MIN_FOR_MA120:
            latest_date  = str(total.index[-1])
            latest_total = _round(float(total.iloc[-1]), 2)
            ma120        = _round(float(total.iloc[-120:].mean()), 2)
            ratio        = _round(latest_total / ma120, 4) if ma120 and ma120 > 0 else None

            vals = total.values.tolist()
            ratio_series: List[float] = []
            for i in range(119, len(vals)):
                wm = sum(vals[i - 119 : i + 1]) / 120
                if wm > 0:
                    ratio_series.append(vals[i] / wm)

            # 将本次东方财富全量数据写入 new_turnover_by_date（覆盖式更新）
            new_by_date = {
                str(dt): {"total_yi": _round(float(v), 2), "source": "akshare:stock_zh_index_daily_em"}
                for dt, v in total.items()
                if not np.isnan(float(v))
            }
            da = {
                "total_turnover": _metric(latest_total, "akshare:stock_zh_index_daily_em", latest_date, unit="亿元"),
                "turnover_ma120": _metric(ma120, "calculated", latest_date, unit="亿元"),
                "turnover_ratio": _metric(ratio, "calculated", latest_date),
            }
            return da, latest_date, latest_total, ratio_series, new_by_date

    # ── Level 2：官方单日 + 存量历史 ─────────────────────────────────────
    log.info("东方财富指数数据不可用，切换至 SSE+SZSE 官方单日数据 + 存量历史…")
    sse_raw  = raw.get("SSE_DAILY", {})
    szse_raw = raw.get("SZSE_DAILY", {})
    sse_ok   = isinstance(sse_raw, dict) and "total_yi" in sse_raw
    szse_ok  = isinstance(szse_raw, dict) and "total_yi" in szse_raw

    new_by_date: Dict[str, Dict] = {}
    latest_date  = None
    latest_total = None

    if sse_ok and szse_ok and sse_raw.get("as_of") == szse_raw.get("as_of"):
        # 同一交易日，直接合计
        day_total = _round(sse_raw["total_yi"] + szse_raw["total_yi"], 2)
        latest_date  = sse_raw["as_of"]
        latest_total = day_total
        new_by_date[latest_date] = {"total_yi": day_total, "source": "akshare:stock_sse_deal_daily+stock_szse_summary"}
    elif sse_ok:
        latest_date  = sse_raw.get("as_of")
        latest_total = sse_raw["total_yi"]
        new_by_date[latest_date] = {"total_yi": latest_total, "source": "akshare:stock_sse_deal_daily"}
    elif szse_ok:
        latest_date  = szse_raw.get("as_of")
        latest_total = szse_raw["total_yi"]
        new_by_date[latest_date] = {"total_yi": latest_total, "source": "akshare:stock_szse_summary"}

    # 合并到存量历史，用于 MA120 / 分位数
    merged = update_turnover_history(turnover_history, new_by_date)
    # merged 是倒序列表，转升序用于计算
    hist_vals = [e["total_yi"] for e in reversed(merged) if e.get("total_yi")]

    ma120  = None
    ratio  = None
    ratio_series = []
    if len(hist_vals) >= TURNOVER_MIN_FOR_MA120:
        ma120 = _round(sum(hist_vals[-120:]) / 120, 2)
        if latest_total and ma120 and ma120 > 0:
            ratio = _round(latest_total / ma120, 4)
        for i in range(119, len(hist_vals)):
            wm = sum(hist_vals[i - 119 : i + 1]) / 120
            if wm > 0:
                ratio_series.append(hist_vals[i] / wm)

    turnover_src = "akshare:stock_sse_deal_daily+stock_szse_summary" if (sse_ok or szse_ok) else "turnover_history"
    da = {
        "total_turnover": _metric(latest_total, turnover_src, latest_date, unit="亿元"),
        "turnover_ma120": _metric(ma120, "calculated(history)", latest_date, unit="亿元"),
        "turnover_ratio": _metric(ratio, "calculated(history)", latest_date),
    }
    return da, latest_date, latest_total, ratio_series, new_by_date


def _build_leverage_metrics(
    raw: Dict,
    latest_total: Optional[float],
    latest_date: Optional[str],
) -> Tuple[Dict, Optional[float]]:
    """
    计算融资买入额、占比及 SSE 分位数。
    分位数近似：用 SSE 融资买入额 ÷ 沪市成交额（单市场近似，避免 SZSE 504次逐日请求）。
    返回：(leverage dict, margin_ratio_pct)
    """
    margin_sse  = raw.get("MARGIN_SSE")
    margin_szse = raw.get("MARGIN_SZSE", {})
    sh          = raw.get("SH_INDEX")

    sse_ok  = _PD_OK and isinstance(margin_sse, pd.DataFrame) and not margin_sse.empty
    szse_ok = isinstance(margin_szse, dict) and "buy_yi" in margin_szse

    sse_val:  Optional[float] = None
    sse_date: Optional[str]   = None
    if sse_ok:
        last_row  = margin_sse.iloc[-1]
        sse_val   = _round(float(last_row["buy_yi"]), 2)
        sse_date  = str(last_row["date_iso"])

    szse_val = _round(margin_szse.get("buy_yi"), 2) if szse_ok else None

    # 全市融资买入额
    if sse_val is not None and szse_val is not None:
        total_margin = _round(sse_val + szse_val, 2)
        margin_date  = sse_date
        margin_src   = "akshare:stock_margin_sse+szse"
    elif sse_val is not None:
        total_margin = sse_val
        margin_date  = sse_date
        margin_src   = "akshare:stock_margin_sse"
    else:
        total_margin = None
        margin_date  = None
        margin_src   = "akshare:stock_margin_sse+szse"

    # 融资买入占比
    margin_buy_ratio: Optional[float] = None
    if total_margin is not None and latest_total is not None and latest_total > 0:
        margin_buy_ratio = _round(total_margin / latest_total, 4)

    # SSE 融资分位数：sse_buy / sh_amount（每日对齐）
    margin_ratio_pct: Optional[float] = None
    if sse_ok and _PD_OK and isinstance(sh, pd.DataFrame) and not sh.empty:
        sse_series = margin_sse.set_index("date_iso")["buy_yi"]
        sh_series  = sh.set_index("date")["amount_yi"]
        common     = sse_series.index.intersection(sh_series.index)
        if len(common) >= 30:
            aligned_ratio = (sse_series[common] / sh_series[common]).replace([np.inf, -np.inf], np.nan).dropna()
            hist_ratios   = aligned_ratio.tolist()
            # 当前 SSE 占比
            if sse_val is not None and sh_series.iloc[-1] and sh_series.iloc[-1] > 0:
                cur_sse_ratio = sse_val / float(sh_series.iloc[-1])
                margin_ratio_pct = _percentile_rank(hist_ratios, cur_sse_ratio)

    leverage = {
        "margin_buy_amount": {
            "value": total_margin,
            "unit": "亿元",
            "status": "ok" if total_margin is not None else "unavailable",
            "source": margin_src,
            "as_of": margin_date,
        },
        "margin_buy_ratio": _metric(margin_buy_ratio, "calculated", margin_date),
    }
    return leverage, margin_ratio_pct


def calc_all_derived(
    raw: Dict,
    breadth_history: List[Dict],
    turnover_history: List[Dict],
) -> Dict:
    """
    统一入口：从所有 fetch 结果计算衍生指标，组装完整 snapshot 各字段。
    返回包含所有顶层节点的 dict（含更新后的 turnover_history）。
    """
    errors: List[Dict] = []

    # ── 成交额 ─────────────────────────────────────────────────────────────
    daily_activity, latest_date, latest_total, ratio_series, new_turnover_by_date = \
        _build_turnover_metrics(raw, turnover_history)
    turnover_history = update_turnover_history(turnover_history, new_turnover_by_date)
    turnover_ratio = daily_activity["turnover_ratio"].get("value")
    turnover_ratio_pct = (
        _percentile_rank(ratio_series, turnover_ratio)
        if (ratio_series and turnover_ratio is not None)
        else None
    )

    # ── 融资 ──────────────────────────────────────────────────────────────
    leverage, margin_ratio_pct = _build_leverage_metrics(raw, latest_total, latest_date)

    # ── 涨跌比 ────────────────────────────────────────────────────────────
    spot = raw.get("SPOT", {})
    spot_ok = isinstance(spot, dict) and "advance" in spot
    advance        = spot.get("advance")       if spot_ok else None
    decline        = spot.get("decline")       if spot_ok else None
    flat           = spot.get("flat")          if spot_ok else None
    advance_ratio  = spot.get("advance_ratio") if spot_ok else None
    spot_as_of     = spot.get("as_of", _today())

    market_breadth = {
        "advance_count":  _metric(advance, "akshare:stock_zh_a_spot_em", spot_as_of),
        "decline_count":  _metric(decline, "akshare:stock_zh_a_spot_em", spot_as_of),
        "flat_count":     _metric(flat,    "akshare:stock_zh_a_spot_em", spot_as_of),
        "advance_ratio":  _metric(advance_ratio, "calculated", spot_as_of),
    }

    # 涨跌比分位数
    hist_ratios = [e["advance_ratio"] for e in breadth_history if e.get("advance_ratio") is not None]
    if len(hist_ratios) >= BREADTH_MIN_FOR_PERCENTILE and advance_ratio is not None:
        adv_pct = _percentile_rank(hist_ratios, advance_ratio)
        adv_pct_entry: Dict = {"value": adv_pct, "lookback_days": len(hist_ratios), "status": "ok"}
    else:
        adv_pct_entry = {
            "value": None,
            "lookback_days": len(hist_ratios),
            "status": "accumulating",
            "note": f"需积累至少{BREADTH_MIN_FOR_PERCENTILE}个交易日数据，当前{len(hist_ratios)}条",
        }

    # ── QVIX ──────────────────────────────────────────────────────────────
    qvix_raw   = raw.get("QVIX", {})
    qvix_val   = qvix_raw.get("value")   if isinstance(qvix_raw, dict) else None
    qvix_as_of = qvix_raw.get("as_of")   if isinstance(qvix_raw, dict) else None
    qvix_hist  = qvix_raw.get("hist_close", []) if isinstance(qvix_raw, dict) else []
    qvix_pct   = _percentile_rank(qvix_hist, qvix_val) if (qvix_hist and qvix_val is not None) else None

    volatility = {
        "qvix_300etf": {
            "value": qvix_val,
            "status": "ok" if qvix_val is not None else "unavailable",
            "source": "akshare:index_option_300etf_qvix",
            "as_of": qvix_as_of,
        }
    }

    # ── 资金流动 ───────────────────────────────────────────────────────────
    fund_flow = {
        "new_investors": _build_new_investors_metric(raw.get("NEW_INVESTORS", {})),
    }

    # ── M1/M2/存款 ────────────────────────────────────────────────────────
    money    = raw.get("MONEY_SUPPLY", {}) if isinstance(raw.get("MONEY_SUPPLY"), dict) else {}
    deposit  = raw.get("RMB_DEPOSIT", {})  if isinstance(raw.get("RMB_DEPOSIT"),  dict) else {}

    m1       = money.get("m1_yoy")
    m2       = money.get("m2_yoy")
    money_as = money.get("as_of")
    m1m2     = _round(m1 - m2, 2) if (m1 is not None and m2 is not None) else None

    dep_val  = deposit.get("value")
    dep_as   = deposit.get("as_of")

    fund_reserve = {
        "m1_yoy": {
            "value": m1, "unit": "%",
            "status": "ok" if m1 is not None else "unavailable",
            "source": "akshare:macro_china_money_supply", "as_of": money_as,
        },
        "m2_yoy": {
            "value": m2, "unit": "%",
            "status": "ok" if m2 is not None else "unavailable",
            "source": "akshare:macro_china_money_supply", "as_of": money_as,
        },
        "m1_m2_spread": {
            "value": m1m2, "unit": "pct",
            "status": "ok" if m1m2 is not None else "unavailable",
            "source": "calculated", "as_of": money_as,
        },
        "deposit_yoy": {
            "value": dep_val, "unit": "%",
            "status": "ok" if dep_val is not None else "unavailable",
            "source": "akshare:macro_rmb_deposit", "as_of": dep_as,
            "note": "新增储蓄存款同比增速（macro_rmb_deposit 接口最接近居民存款同比的可用列）",
        },
    }

    # ── 分位数快照 ─────────────────────────────────────────────────────────
    derived = {
        "percentile_snapshot": {
            "turnover_ratio_pct": {
                "value": turnover_ratio_pct,
                "lookback_days": QVIX_LOOKBACK_DAYS,
                "status": "ok" if turnover_ratio_pct is not None else "unavailable",
            },
            "margin_buy_ratio_pct": {
                "value": margin_ratio_pct,
                "lookback_days": MARGIN_LOOKBACK_DAYS,
                "status": "ok" if margin_ratio_pct is not None else "unavailable",
                "note": "仅用沪市融资数据近似（深市接口不支持范围查询）",
            },
            "qvix_pct": {
                "value": qvix_pct,
                "lookback_days": QVIX_LOOKBACK_DAYS,
                "status": "ok" if qvix_pct is not None else "unavailable",
            },
            "advance_ratio_pct": adv_pct_entry,
        }
    }

    return {
        "daily_activity":  daily_activity,
        "market_breadth":  market_breadth,
        "leverage":        leverage,
        "volatility":      volatility,
        "fund_flow":       fund_flow,
        "fund_reserve":    fund_reserve,
        "derived":         derived,
        "turnover_history": turnover_history,
        "errors":          errors,
    }


def _build_new_investors_metric(raw: Dict) -> Dict:
    if isinstance(raw, dict) and raw.get("value") is not None:
        return {
            "value": raw["value"],
            "unit": "万户",
            "status": "ok",
            "source": "akshare:stock_account_statistics_em",
            "as_of": raw.get("as_of"),
        }
    return {
        "value": None, "unit": "万户",
        "status": "unavailable",
        "source": "akshare:stock_account_statistics_em",
        "as_of": None,
        "reason": raw.get("reason", "unknown") if isinstance(raw, dict) else "fetch failed",
    }

# ─── 主流程 ───────────────────────────────────────────────────────────────────

def run() -> Dict[str, Any]:
    """采集所有指标，计算衍生值，组装最终 snapshot。"""
    errors: List[Dict] = []
    raw:    Dict[str, Any] = {}

    # Phase 1：串行采集
    batch_fns = [
        fetch_sh_index,            # 组1：东方财富指数（历史）
        fetch_sz_index,
        fetch_sse_daily_turnover,  # 组2：官方单日成交额（备源）
        fetch_szse_daily_turnover,
        fetch_margin_sse,          # 组3：融资
        fetch_margin_szse,
        fetch_qvix,                # 组4：其他
        fetch_new_investors,
        fetch_money_supply,
        fetch_rmb_deposit,
        fetch_spot_em,             # 含新浪兜底，最后执行（耗时较长）
    ]

    for fn in batch_fns:
        try:
            key, data = fn()
            raw[key] = data
            status = data.get("status", "ok") if isinstance(data, dict) else "ok"
            if status == "unavailable":
                error_msg = data.get("reason", "unavailable")
                if "RemoteDisconnected" in error_msg or "Connection" in error_msg:
                    error_msg = "网络连接被拒绝，请检查网络环境或稍后重试"
                errors.append({"indicator": key, "error": error_msg})
            time.sleep(1)
        except Exception as exc:
            error_msg = str(exc)
            if "RemoteDisconnected" in error_msg or "Connection" in error_msg:
                error_msg = "网络连接被拒绝，请检查网络环境或稍后重试"
            errors.append({"indicator": fn.__name__, "error": error_msg})
            log.error(f"Unexpected error in {fn.__name__}: {error_msg}")
            time.sleep(2)

    # Phase 2：读取已有历史
    breadth_history, turnover_history = _load_existing_snapshot()

    # Phase 3：更新涨跌比历史
    spot_data = raw.get("SPOT", {})
    today_ratio = (
        spot_data.get("advance_ratio")
        if isinstance(spot_data, dict) and "advance_ratio" in spot_data
        else None
    )
    breadth_history = update_breadth_history(breadth_history, _today(), today_ratio)

    # Phase 4：衍生计算（内部同步更新 turnover_history）
    derived_result = calc_all_derived(raw, breadth_history, turnover_history)
    errors.extend(derived_result.pop("errors", []))
    turnover_history = derived_result.pop("turnover_history", turnover_history)

    # Phase 5：组装最终 snapshot（不含大体积历史数据）
    snapshot: Dict[str, Any] = {
        "meta": {
            "generated_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "read_instructions": READ_INSTRUCTIONS,
        },
        "daily_activity":  derived_result["daily_activity"],
        "market_breadth":  derived_result["market_breadth"],
        "leverage":        derived_result["leverage"],
        "volatility":      derived_result["volatility"],
        "fund_flow":       derived_result["fund_flow"],
        "fund_reserve":    derived_result["fund_reserve"],
        "derived":         derived_result["derived"],
        # breadth_history 保留在 snapshot 中（条数少，供 Claude 直接读取近期趋势）
        "breadth_history": breadth_history,
        "errors":          errors,
        # 历史条数摘要（不含原始数据，原始数据存独立文件）
        "_history_meta": {
            "breadth_history_count":  len(breadth_history),
            "turnover_history_count": len(turnover_history),
            "history_files": {
                "breadth":  "sentiment_breadth_history.json",
                "turnover": "sentiment_turnover_history.json",
            },
        },
    }
    return snapshot, breadth_history, turnover_history


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info("=" * 55)
    log.info("  A股市场情绪监控 — 数据采集开始")
    log.info("=" * 55)
    t0 = datetime.now()

    snapshot, breadth_history, turnover_history = run()

    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"采集完成，耗时 {elapsed:.1f}s，错误数: {len(snapshot['errors'])}")

    # 主 snapshot（精简，供 Claude 读取）
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    log.info(f"Snapshot 已写入：{OUTPUT_FILE}")

    # 独立历史文件（供脚本下次运行读取，不暴露给 Claude）
    with open(BREADTH_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(breadth_history, f, ensure_ascii=False, indent=2)
    with open(TURNOVER_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(turnover_history, f, ensure_ascii=False, indent=2)
    log.info(f"历史文件已写入：{BREADTH_HISTORY_FILE}, {TURNOVER_HISTORY_FILE}")

    # 打印摘要
    print("\n─── 采集摘要 ────────────────────────────────────")
    da  = snapshot.get("daily_activity", {})
    mb  = snapshot.get("market_breadth", {})
    lev = snapshot.get("leverage", {})
    vol = snapshot.get("volatility", {})
    ff  = snapshot.get("fund_flow", {})
    fr  = snapshot.get("fund_reserve", {})
    pct = snapshot.get("derived", {}).get("percentile_snapshot", {})

    def _fmt(d: Dict, key: str) -> str:
        v = d.get(key, {})
        if isinstance(v, dict):
            return f"{v.get('value', 'N/A')} [{v.get('status', '?')}]"
        return str(v)

    print(f"  全A成交额:       {_fmt(da, 'total_turnover')} 亿元")
    print(f"  成交额/MA120:    {_fmt(da, 'turnover_ratio')}  (分位: {pct.get('turnover_ratio_pct', {}).get('value', 'N/A')}%)")
    print(f"  上涨家数占比:    {_fmt(mb, 'advance_ratio')}  (分位: {pct.get('advance_ratio_pct', {}).get('value', 'N/A')}%)")
    print(f"  融资买入额:      {_fmt(lev, 'margin_buy_amount')} 亿元")
    print(f"  融资买入占比:    {_fmt(lev, 'margin_buy_ratio')}  (分位: {pct.get('margin_buy_ratio_pct', {}).get('value', 'N/A')}%)")
    print(f"  QVIX:           {_fmt(vol, 'qvix_300etf')}  (分位: {pct.get('qvix_pct', {}).get('value', 'N/A')}%)")
    print(f"  M1同比:         {_fmt(fr, 'm1_yoy')}%  M2: {_fmt(fr, 'm2_yoy')}%  剪刀差: {_fmt(fr, 'm1_m2_spread')}pct")
    print(f"  breadth_history 条数: {len(snapshot.get('breadth_history', []))}")
    print(f"  错误数: {len(snapshot['errors'])}")
    for err in snapshot["errors"]:
        print(f"    ⚠️  {err.get('indicator', '?')}: {err.get('error', '')[:80]}")
    print("─────────────────────────────────────────────────")


if __name__ == "__main__":
    main()