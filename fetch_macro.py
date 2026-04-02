"""
fetch_macro.py
宏观股市分析框架 — 数据采集脚本
职责：可靠地获取数据、处理错误、输出干净的 JSON
输出：./output/macro_snapshot.json
"""

import json
import os
import logging
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

# ─── 第三方库（软导入，缺失时降级处理）─────────────────────────────────────────

try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False

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

# ─── 配置常量 ──────────────────────────────────────────────────────────────────

OUTPUT_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "macro_snapshot.json")

TIMEOUT     = 10          # 单次请求超时（秒）
MAX_RETRIES = 2           # HTTP 自动重试次数
MAX_WORKERS = 10          # 并行线程数

# Yahoo Finance tickers
YF_TICKERS = {
    "DXY":    "DX-Y.NYB",
    "BRENT":  "BZ=F",
    "COPPER": "HG=F",
    "GOLD":   "GC=F",
    "VIX":    "^VIX",
    "US10Y":  "^TNX",
    "USDCNY": "CNY=X",
}

# FOMC 会议日期（每年更新一次；取每次会议第一天）
# 来源：https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
FOMC_MEETING_DATES = [
    # 2025
    date(2025, 1, 28), date(2025, 3, 18), date(2025, 5, 6),
    date(2025, 6, 17), date(2025, 7, 29), date(2025, 9, 16),
    date(2025, 10, 28), date(2025, 12, 9),
    # 2026
    date(2026, 1, 27), date(2026, 3, 17), date(2026, 5, 5),
    date(2026, 6, 16), date(2026, 7, 28), date(2026, 9, 15),
    date(2026, 10, 27), date(2026, 12, 8),
]

# 30-Day Fed Funds Futures 月份代码（CME ZQ 合约）
ZQ_MONTH_CODE = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

# 人工查询备用链接（全部失败时写入 JSON 供用户参考）
MANUAL_URLS = {
    "PMI_official":   "https://data.stats.gov.cn/easyquery.htm?cn=A01",
    "PMI_caixin":     "https://www.markiteconomics.com/Public/Page/Release/Manufacturing",
    "new_loans":      "http://www.pbc.gov.cn/diaochatongji/116219/index.html",
    "PPI":            "https://data.stats.gov.cn/easyquery.htm?cn=A01",
    "CPI":            "https://data.stats.gov.cn/easyquery.htm?cn=A01",
    "CSI300_PE":      "https://www.eastmoney.com",
    "bond_yield_10y": "https://yield.chinabond.com.cn",
    "margin_balance": "http://www.sse.com.cn/market/margin/",
    "fedwatch":       "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html",
    "sp500_pe":       "https://www.multpl.com/s-p-500-pe-ratio",
    "hsi_pe":         "https://www.wsj.com/market-data/quotes/index/HK/HSI/financials/annual/income-statement",
}

# ─── 日志 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── 工具函数 ──────────────────────────────────────────────────────────────────

def make_session() -> "requests.Session":
    """返回带指数退避重试的 Session。"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    return session


def _unavail(reason: str, manual_url: Optional[str] = None) -> Dict:
    """构造标准 unavailable 结构。"""
    r: Dict[str, Any] = {"status": "unavailable", "reason": reason}
    if manual_url:
        r["manual_url"] = manual_url
    return r


def _round(val: Any, n: int = 4) -> Optional[float]:
    try:
        return round(float(val), n)
    except (TypeError, ValueError):
        return None


def _today() -> str:
    return date.today().isoformat()


def _latest_period(df, date_col: str) -> str:
    """从 DataFrame 取最新日期列的字符串值。"""
    try:
        return str(df[date_col].iloc[-1])
    except Exception:
        return "unknown"


# ─── Layer 1：外部约束 ─────────────────────────────────────────────────────────

def _fetch_yf(key: str) -> Dict:
    """通用 yfinance 单指标采集：返回最新价、当日/5日涨跌幅、5日/20日均值及偏离。"""
    if not _YF_OK:
        return _unavail("yfinance not installed")

    symbol = YF_TICKERS[key]
    try:
        hist = yf.Ticker(symbol).history(period="30d")
        if hist is None or hist.empty:
            return _unavail(f"yfinance returned empty data for {symbol}")

        closes = hist["Close"].dropna()
        if len(closes) < 1:
            return _unavail("No close prices available")

        current = float(closes.iloc[-1])

        # 当日涨跌幅（日环比）
        chg_1d = _round((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100, 2) \
                 if len(closes) >= 2 else None

        # 5日涨跌幅
        ref_idx = max(0, len(closes) - 6)
        ref     = float(closes.iloc[ref_idx])
        chg_5d  = _round((current - ref) / ref * 100, 2) if ref != 0 else None

        # 5日均值
        ma5  = _round(float(closes.iloc[-5:].mean()),  2) if len(closes) >= 5  else None
        # 20日均值
        ma20 = _round(float(closes.iloc[-20:].mean()), 2) if len(closes) >= 20 else None

        # 对均值的偏离百分比（正=高于均值，负=低于均值）
        dev_ma5  = _round((current - ma5)  / ma5  * 100, 2) if ma5  else None
        dev_ma20 = _round((current - ma20) / ma20 * 100, 2) if ma20 else None

        direction = (
            "up"   if (chg_5d or 0) >  0.05 else
            "down" if (chg_5d or 0) < -0.05 else
            "flat"
        )
        as_of = hist.index[-1].date().isoformat()

        return {
            "value":             _round(current, 3),
            "chg_1d_pct":        chg_1d,
            "chg_5d_pct":        chg_5d,
            "ma5":               ma5,
            "ma20":              ma20,
            "dev_from_ma5_pct":  dev_ma5,
            "dev_from_ma20_pct": dev_ma20,
            "direction":         direction,
            "status":            "ok",
            "source":            "yfinance",
            "as_of":             as_of,
        }
    except Exception as exc:
        return _unavail(str(exc))


def fetch_dxy() -> Tuple[str, Dict]:
    log.info("Fetching DXY …")
    return "DXY", _fetch_yf("DXY")


def fetch_brent() -> Tuple[str, Dict]:
    log.info("Fetching Brent Crude …")
    return "BRENT", _fetch_yf("BRENT")


def fetch_copper() -> Tuple[str, Dict]:
    log.info("Fetching Copper (COMEX HG=F, USD/lb) …")
    return "COPPER", _fetch_yf("COPPER")


def fetch_gold() -> Tuple[str, Dict]:
    log.info("Fetching Gold (COMEX GC=F, USD/oz) …")
    return "GOLD", _fetch_yf("GOLD")


def fetch_vix() -> Tuple[str, Dict]:
    log.info("Fetching VIX …")
    return "VIX", _fetch_yf("VIX")


def fetch_usdcny() -> Tuple[str, Dict]:
    log.info("Fetching USD/CNY spot rate …")
    return "USDCNY", _fetch_yf("USDCNY")


def fetch_fedwatch() -> Tuple[str, Dict]:
    """
    用 30-Day Fed Funds Futures（yfinance ZQ 合约）推算下次 FOMC 会议降息概率。

    算法（CME FedWatch 标准日加权法）：
      设会议在月份 M 的第 D 天，当月共 N 天：
        month_M_futures_rate = (D-1)/N * pre_rate + (N-D+1)/N * post_rate
      => post_rate = (month_M_rate * N - (D-1) * pre_rate) / (N-D+1)
        cut_25bp_prob  = max(0, (pre_rate - post_rate) / 0.25 * 100)
        hike_25bp_prob = max(0, (post_rate - pre_rate) / 0.25 * 100)
        hold_prob      = 100 - cut_25bp_prob - hike_25bp_prob

    注：FOMC_MEETING_DATES 需每年手动更新一次。
    """
    log.info("Fetching FedWatch (via ZQ futures) …")

    if not _YF_OK:
        return "FEDWATCH", _unavail("yfinance not installed", MANUAL_URLS["fedwatch"])

    import calendar

    try:
        today = date.today()

        # 找下次会议
        upcoming = [d for d in FOMC_MEETING_DATES if d >= today]
        if not upcoming:
            return "FEDWATCH", _unavail(
                "No upcoming FOMC dates in FOMC_MEETING_DATES; please update the list",
                MANUAL_URLS["fedwatch"],
            )
        next_meeting = upcoming[0]

        def _zq_ticker(yr: int, mo: int) -> str:
            return f"ZQ{ZQ_MONTH_CODE[mo]}{str(yr)[-2:]}.CBT"

        def _get_implied_rate(yr: int, mo: int) -> float:
            ticker = _zq_ticker(yr, mo)
            hist   = yf.Ticker(ticker).history(period="3d")
            if hist.empty:
                raise ValueError(f"No data for {ticker}")
            price = float(hist["Close"].iloc[-1])
            return round(100 - price, 6), ticker

        # 前月合约（当前隐含利率代理）
        # 月末最后几天用下月合约，避免到期噪声
        if today.day >= 25:
            pre_mo = next_meeting.month - 1 if next_meeting.month > 1 else 12
            pre_yr = next_meeting.year if next_meeting.month > 1 else next_meeting.year - 1
        else:
            pre_mo, pre_yr = today.month, today.year

        pre_rate, pre_ticker   = _get_implied_rate(pre_yr, pre_mo)
        meet_rate, meet_ticker = _get_implied_rate(next_meeting.year, next_meeting.month)

        # 日加权推算会后隐含利率
        N = calendar.monthrange(next_meeting.year, next_meeting.month)[1]
        D = next_meeting.day
        post_rate = round(
            (meet_rate * N - (D - 1) * pre_rate) / (N - D + 1), 6
        )

        cut_prob  = _round(max(0.0, min(100.0, (pre_rate - post_rate) / 0.25 * 100)), 1)
        hike_prob = _round(max(0.0, min(100.0, (post_rate - pre_rate) / 0.25 * 100)), 1)
        hold_prob = _round(max(0.0, 100.0 - (cut_prob or 0) - (hike_prob or 0)), 1)

        return "FEDWATCH", {
            "next_meeting_date":    next_meeting.isoformat(),
            "cut_25bp_prob_pct":    cut_prob,
            "hold_prob_pct":        hold_prob,
            "hike_25bp_prob_pct":   hike_prob,
            "pre_meeting_rate_pct": pre_rate,
            "post_meeting_implied": post_rate,
            "pre_contract":         pre_ticker,
            "meet_contract":        meet_ticker,
            "note": (
                "Day-count weighted probability from 30-Day Fed Funds Futures; "
                "FOMC_MEETING_DATES requires annual manual update"
            ),
            "status": "ok",
            "source": "yfinance/ZQ",
            "as_of":  _today(),
        }

    except Exception as exc:
        log.warning(f"FedWatch (ZQ futures) failed: {exc}")
        return "FEDWATCH", _unavail(str(exc), MANUAL_URLS["fedwatch"])


# ─── Layer 0：美债传导 ────────────────────────────────────────────────────────

def fetch_us10y() -> Tuple[str, Dict]:
    log.info("Fetching US 10Y Treasury Yield …")
    return "US10Y", _fetch_yf("US10Y")


def fetch_us2y() -> Tuple[str, Dict]:
    """
    美国2年期国债收益率。
    主力：FRED DGS2（公开 CSV，无需 API Key，超时 30s）
    备用：yfinance ^IRX（13周T-Bill，近似值，附 note 说明）
    """
    log.info("Fetching US 2Y Treasury Yield …")

    # ── 主力：FRED DGS2 ────────────────────────────────────────────────────────
    if _REQ_OK:
        try:
            import io
            import pandas as pd
            url  = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2"
            resp = make_session().get(url, timeout=30)   # FRED 响应较慢，单独用 30s
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text), parse_dates=["DATE"])
            df.columns = ["date", "yield"]
            df = df[df["yield"] != "."].copy()
            df["yield"] = df["yield"].astype(float)
            df = df.sort_values("date").reset_index(drop=True)
            if df.empty:
                raise ValueError("FRED DGS2 returned empty data")

            closes  = df["yield"]
            current = float(closes.iloc[-1])
            chg_1d  = _round(float(closes.iloc[-1] - closes.iloc[-2]), 4) \
                      if len(closes) >= 2 else None
            ref_idx  = max(0, len(closes) - 6)
            chg_5d   = _round((current - float(closes.iloc[ref_idx])) /
                               float(closes.iloc[ref_idx]) * 100, 2) \
                       if float(closes.iloc[ref_idx]) != 0 else None
            ma5      = _round(float(closes.iloc[-5:].mean()),  2) if len(closes) >= 5  else None
            ma20     = _round(float(closes.iloc[-20:].mean()), 2) if len(closes) >= 20 else None
            dev_ma5  = _round((current - ma5)  / ma5  * 100, 2) if ma5  else None
            dev_ma20 = _round((current - ma20) / ma20 * 100, 2) if ma20 else None
            direction = (
                "up"   if (chg_5d or 0) >  0.05 else
                "down" if (chg_5d or 0) < -0.05 else
                "flat"
            )
            return "US2Y", {
                "value":             _round(current, 3),
                "chg_1d_pct":        chg_1d,
                "chg_5d_pct":        chg_5d,
                "ma5":               ma5,
                "ma20":              ma20,
                "dev_from_ma5_pct":  dev_ma5,
                "dev_from_ma20_pct": dev_ma20,
                "direction":         direction,
                "status":            "ok",
                "source":            "FRED/DGS2",
                "as_of":             df["date"].iloc[-1].date().isoformat(),
            }
        except Exception as exc:
            log.warning(f"US2Y FRED failed ({exc}), falling back to yfinance ^IRX")

    # ── 备用：yfinance ^IRX（13周T-Bill） ──────────────────────────────────────
    if _YF_OK:
        try:
            hist   = yf.Ticker("^IRX").history(period="30d")
            closes = hist["Close"].dropna()
            if closes.empty:
                raise ValueError("^IRX returned empty data")
            current  = float(closes.iloc[-1])
            chg_1d   = _round(float(closes.iloc[-1] - closes.iloc[-2]), 4) \
                       if len(closes) >= 2 else None
            ref_idx  = max(0, len(closes) - 6)
            ref      = float(closes.iloc[ref_idx])
            chg_5d   = _round((current - ref) / ref * 100, 2) if ref != 0 else None
            ma5      = _round(float(closes.iloc[-5:].mean()),  2) if len(closes) >= 5  else None
            ma20     = _round(float(closes.iloc[-20:].mean()), 2) if len(closes) >= 20 else None
            dev_ma5  = _round((current - ma5)  / ma5  * 100, 2) if ma5  else None
            dev_ma20 = _round((current - ma20) / ma20 * 100, 2) if ma20 else None
            direction = (
                "up"   if (chg_5d or 0) >  0.05 else
                "down" if (chg_5d or 0) < -0.05 else
                "flat"
            )
            return "US2Y", {
                "value":             _round(current, 3),
                "chg_1d_pct":        chg_1d,
                "chg_5d_pct":        chg_5d,
                "ma5":               ma5,
                "ma20":              ma20,
                "dev_from_ma5_pct":  dev_ma5,
                "dev_from_ma20_pct": dev_ma20,
                "direction":         direction,
                "note":              "fallback: ^IRX is 13-week T-Bill rate, not true 2Y yield",
                "status":            "ok",
                "source":            "yfinance/^IRX (fallback)",
                "as_of":             hist.index[-1].date().isoformat(),
            }
        except Exception as exc:
            log.warning(f"US2Y ^IRX fallback failed: {exc}")

    return "US2Y", _unavail("both FRED and yfinance unavailable")


# ─── Layer 2：国内基本面 ────────────────────────────────────────────────────────

def fetch_pmi_official() -> Tuple[str, Dict]:
    """
    官方制造业 PMI 综合值。
    主力：akshare.macro_china_pmi()
    结构：['月份','制造业-指数','制造业-同比增长','非制造业-指数','非制造业-同比增长']
    **降序**（最新在 iloc[0]），数据更新到 2026-02。
    备用：macro_china_pmi_yearly()（财经日历格式，数据较旧止于 2025-08）
    PMI 分项（新订单、产成品库存）两个接口均不含，标注手工 URL。
    """
    log.info("Fetching Official PMI …")

    if not _AK_OK:
        return "PMI_OFFICIAL", _unavail("akshare not installed", MANUAL_URLS["PMI_official"])

    # 主力：macro_china_pmi（降序，iloc[0] = 最新）
    try:
        df = ak.macro_china_pmi()
        if df is not None and not df.empty:
            latest = df.iloc[0]
            cols   = df.columns.tolist()
            log.debug(f"PMI (macro_china_pmi) columns: {cols}")
            mfg_col = next((c for c in cols if "制造业-指数" in c or c == "制造业"), None)
            if mfg_col:
                return "PMI_OFFICIAL", {
                    "composite":                _round(latest[mfg_col], 1),
                    "new_orders":               None,
                    "finished_goods_inventory": None,
                    "period":                   str(latest.iloc[0]),  # '月份' 列
                    "note": "PMI sub-indices (new_orders/inventory) not available via akshare; check manually",
                    "manual_url": MANUAL_URLS["PMI_official"],
                    "status":     "ok",
                    "source":     "akshare/NBS",
                }
    except Exception as e:
        log.debug(f"macro_china_pmi failed: {e}")

    # 备用：macro_china_pmi_yearly（财经日历格式，升序，dropna 取最新非空行）
    try:
        df2 = ak.macro_china_pmi_yearly()
        if df2 is None or df2.empty:
            raise ValueError("akshare returned empty PMI data")
        cols2 = df2.columns.tolist()
        valid = df2.dropna(subset=["今值"]) if "今值" in cols2 else df2
        if valid.empty:
            raise ValueError("All PMI '今值' rows are NaN")
        latest    = valid.iloc[-1]
        composite = _round(latest["今值"], 1) if "今值" in cols2 else None
        period    = str(latest.get("日期", latest.iloc[0]))
        return "PMI_OFFICIAL", {
            "composite":                composite,
            "new_orders":               None,
            "finished_goods_inventory": None,
            "period":                   period,
            "note": "PMI sub-indices not available via akshare; check manually",
            "manual_url": MANUAL_URLS["PMI_official"],
            "status":     "ok",
            "source":     "akshare/NBS",
        }
    except Exception as exc:
        log.warning(f"Official PMI failed: {exc}")
        return "PMI_OFFICIAL", _unavail(str(exc), MANUAL_URLS["PMI_official"])


def fetch_pmi_caixin() -> Tuple[str, Dict]:
    """
    财新制造业 PMI。
    主力：akshare.index_pmi_man_cx()
    结构：['日期', '制造业PMI', '变化值']，升序，最新在 iloc[-1]，数据更新到 2026-02。
    备用：macro_china_cx_pmi_yearly()（财经日历格式，数据较旧）
    """
    log.info("Fetching Caixin PMI …")

    if not _AK_OK:
        return "PMI_CAIXIN", _unavail("akshare not installed", MANUAL_URLS["PMI_caixin"])

    # 主力：index_pmi_man_cx（数据最新）
    try:
        df = ak.index_pmi_man_cx()
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            cols   = df.columns.tolist()
            log.debug(f"Caixin PMI (index_pmi_man_cx) columns: {cols}")
            val_col = next((c for c in cols if "PMI" in c), None)
            if val_col:
                period = str(latest["日期"]) if "日期" in cols else str(latest.iloc[0])
                return "PMI_CAIXIN", {
                    "value":  _round(latest[val_col], 1),
                    "period": period,
                    "status": "ok",
                    "source": "akshare/Caixin",
                }
    except Exception as e:
        log.debug(f"index_pmi_man_cx failed: {e}")

    # 备用：macro_china_cx_pmi_yearly（财经日历格式）
    try:
        df2 = ak.macro_china_cx_pmi_yearly()
        if df2 is None or df2.empty:
            raise ValueError("akshare returned empty Caixin PMI data")
        cols2  = df2.columns.tolist()
        valid  = df2.dropna(subset=["今值"]) if "今值" in cols2 else df2
        latest = valid.iloc[-1]
        value  = _round(latest["今值"], 1) if "今值" in cols2 else None
        period = str(latest.get("日期", latest.iloc[0]))
        return "PMI_CAIXIN", {
            "value":  value,
            "period": period,
            "status": "ok",
            "source": "akshare/Caixin",
        }
    except Exception as exc:
        log.warning(f"Caixin PMI failed: {exc}")
        return "PMI_CAIXIN", _unavail(str(exc), MANUAL_URLS["PMI_caixin"])


def fetch_new_loans() -> Tuple[str, Dict]:
    """
    新增人民币贷款总额（亿元）及企业中长期贷款分项。
    主力：akshare.macro_rmb_loan()  → 新增人民币贷款-总额
    企业中长期分项：akshare 暂无直接接口，标注手工 URL 由用户补充。
    """
    log.info("Fetching New RMB Loans …")

    if not _AK_OK:
        return "NEW_LOANS", _unavail("akshare not installed", MANUAL_URLS["new_loans"])

    try:
        df = ak.macro_rmb_loan()
        if df is None or df.empty:
            raise ValueError("akshare returned empty loan data")

        latest = df.iloc[-1]
        cols   = df.columns.tolist()
        log.debug(f"Loan columns: {cols}")

        def _get_col(*cands):
            for c in cands:
                if c in cols:
                    try:
                        raw = latest[c]
                        # 去掉百分号等非数字字符
                        return _round(str(raw).replace("%", "").strip(), 0)
                    except Exception:
                        pass
            return None

        total_new = _get_col("新增人民币贷款-总额", "新增贷款", "人民币贷款")
        yoy       = _get_col("新增人民币贷款-同比")
        period    = str(latest.iloc[0])

        return "NEW_LOANS", {
            "total_new_loans_100m": total_new,
            "yoy_pct":              yoy,
            "corp_mlt_loans_100m":  None,
            "period":               period,
            "note": (
                "total_new_loans = 当月新增人民币贷款合计; "
                "corp_mlt_loans (企业中长期贷款分项) 暂无 akshare 接口，请手工补录"
            ),
            "status":     "ok",
            "source":     "akshare/PBOC",
            "manual_url": MANUAL_URLS["new_loans"],
        }

    except Exception as exc:
        log.warning(f"New loans failed: {exc}")
        return "NEW_LOANS", _unavail(str(exc), MANUAL_URLS["new_loans"])


def fetch_ppi() -> Tuple[str, Dict]:
    """
    PPI 当月同比（%）。
    akshare: macro_china_ppi()
    结构：['月份', '当月', '当月同比增长', '累计']，**降序**（最新在 iloc[0]）
    """
    log.info("Fetching PPI …")

    if not _AK_OK:
        return "PPI", _unavail("akshare not installed", MANUAL_URLS["PPI"])

    try:
        df = ak.macro_china_ppi()
        if df is None or df.empty:
            raise ValueError("akshare returned empty PPI data")

        # 数据降序，取 iloc[0] 获得最新月份
        latest = df.iloc[0]
        cols   = df.columns.tolist()
        log.debug(f"PPI columns: {cols}")

        yoy    = _round(latest.get("当月同比增长", latest.get("同比", None)), 2)
        period = str(latest.iloc[0])  # '月份' 列，如 "2026年02月份"

        return "PPI", {
            "yoy_pct": yoy,
            "period":  period,
            "status":  "ok",
            "source":  "akshare/NBS",
        }

    except Exception as exc:
        log.warning(f"PPI failed: {exc}")
        return "PPI", _unavail(str(exc), MANUAL_URLS["PPI"])


def fetch_cpi() -> Tuple[str, Dict]:
    """
    CPI 当月同比（%）。
    主力：akshare.macro_china_cpi()
    结构：['月份','全国-当月','全国-同比增长', ...]，**降序**（最新在 iloc[0]）
    备用：macro_china_cpi_yearly()（财经日历格式，dropna 取最新非空行）
    """
    log.info("Fetching CPI …")

    if not _AK_OK:
        return "CPI", _unavail("akshare not installed", MANUAL_URLS["CPI"])

    # 主力：macro_china_cpi（降序，最新 = iloc[0]）
    try:
        df = ak.macro_china_cpi()
        if df is not None and not df.empty:
            latest = df.iloc[0]
            cols   = df.columns.tolist()
            log.debug(f"CPI (macro_china_cpi) columns: {cols}")
            yoy_col = next((c for c in cols if "同比增长" in c or "同比" in c), None)
            if yoy_col:
                yoy    = _round(latest[yoy_col], 2)
                period = str(latest.iloc[0])
                return "CPI", {
                    "yoy_pct": yoy,
                    "period":  period,
                    "status":  "ok",
                    "source":  "akshare/NBS",
                }
    except Exception as e:
        log.debug(f"macro_china_cpi failed: {e}")

    # 备用：macro_china_cpi_yearly（财经日历格式）
    try:
        df2 = ak.macro_china_cpi_yearly()
        if df2 is None or df2.empty:
            raise ValueError("akshare returned empty CPI data")
        cols2  = df2.columns.tolist()
        valid  = df2.dropna(subset=["今值"]) if "今值" in cols2 else df2
        if valid.empty:
            raise ValueError("All CPI '今值' rows are NaN")
        latest = valid.iloc[-1]
        yoy    = _round(latest["今值"], 2) if "今值" in cols2 else None
        period = str(latest.get("日期", latest.iloc[0]))
        return "CPI", {
            "yoy_pct": yoy,
            "period":  period,
            "status":  "ok",
            "source":  "akshare/NBS",
        }
    except Exception as exc:
        log.warning(f"CPI failed: {exc}")
        return "CPI", _unavail(str(exc), MANUAL_URLS["CPI"])


def fetch_lpr() -> "List[Tuple[str, Dict]]":
    """
    ak.macro_china_lpr() 返回历史 LPR 序列，取最新两行计算变动量。
    列：TRADE_DATE, LPR1Y, LPR5Y（升序，最新在 iloc[-1]）
    单位：百分比（如 3.1 表示 3.1%）
    """
    log.info("Fetching LPR …")
    if not _AK_OK:
        return [
            ("LPR_1Y", _unavail("akshare not installed")),
            ("LPR_5Y", _unavail("akshare not installed")),
        ]
    try:
        df = ak.macro_china_lpr()
        if df is None or df.empty:
            raise ValueError("empty DataFrame")
        latest  = df.iloc[-1]
        prev    = df.iloc[-2] if len(df) >= 2 else latest
        period  = str(latest["TRADE_DATE"])[:7]   # YYYY-MM
        lpr1y_v = _round(float(latest["LPR1Y"]), 2)
        lpr5y_v = _round(float(latest["LPR5Y"]), 2)
        lpr1y_p = _round(float(prev["LPR1Y"]),   2)
        lpr5y_p = _round(float(prev["LPR5Y"]),   2)
        return [
            ("LPR_1Y", {
                "value":      lpr1y_v,
                "prev_value": lpr1y_p,
                "chg":        _round(lpr1y_v - lpr1y_p, 2) if lpr1y_v and lpr1y_p else None,
                "period":     period,
                "status":     "ok",
                "source":     "akshare/PBOC",
            }),
            ("LPR_5Y", {
                "value":      lpr5y_v,
                "prev_value": lpr5y_p,
                "chg":        _round(lpr5y_v - lpr5y_p, 2) if lpr5y_v and lpr5y_p else None,
                "period":     period,
                "status":     "ok",
                "source":     "akshare/PBOC",
            }),
        ]
    except Exception as exc:
        log.warning(f"LPR failed: {exc}")
        return [
            ("LPR_1Y", _unavail(str(exc))),
            ("LPR_5Y", _unavail(str(exc))),
        ]


# ─── Layer 3：市场定价 ─────────────────────────────────────────────────────────

def fetch_csi300_pe() -> Tuple[str, Dict]:
    """
    沪深300 市盈率（PE1 = 总股本加权，非严格 TTM，但为中证官网标准口径）。

    方案 A：直接下载中证指数官网静态 XLS（每月末更新，数据可能滞后 0-30 天）
            URL: https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/
                 file/autofile/indicator/000300indicator.xls
    方案 B：东方财富数据中心实时行情字段 f162（PE-TTM*100，指数不一定有值）
    """
    log.info("Fetching CSI300 PE …")

    if not _REQ_OK:
        return "CSI300_PE", _unavail("requests not installed", MANUAL_URLS["CSI300_PE"])

    import io
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = make_session()

    # 方案 A：中证官网静态 XLS（PE1/PE2，按总/流通股本加权）
    try:
        xls_url = (
            "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/"
            "file/autofile/indicator/000300indicator.xls"
        )
        resp = session.get(xls_url, timeout=15, verify=False)
        resp.raise_for_status()

        import pandas as pd
        df = pd.read_excel(io.BytesIO(resp.content))
        if df is None or df.empty:
            raise ValueError("CSIndex XLS is empty")

        latest   = df.iloc[-1]
        cols     = df.columns.tolist()
        date_col = cols[0]

        # 列名含"市盈率1"或"P/E1"（总股本加权）
        pe1_col = next((c for c in cols if "市盈率1" in c or "P/E1" in c), None)
        pe2_col = next((c for c in cols if "市盈率2" in c or "P/E2" in c), None)
        as_of   = str(latest[date_col])

        pe1 = _round(latest[pe1_col], 2) if pe1_col else None
        pe2 = _round(latest[pe2_col], 2) if pe2_col else None

        if pe1 is not None:
            return "CSI300_PE", {
                "pe_total_shares":  pe1,   # 总股本加权
                "pe_float_shares":  pe2,   # 流通股本加权
                "note": (
                    "pe_total_shares = 市盈率1（总股本），pe_float_shares = 市盈率2（流通股本）; "
                    "数据源为中证月末静态文件，最新日期见 as_of"
                ),
                "status": "ok",
                "source": "csindex.com.cn/xls",
                "as_of":  as_of,
            }
    except Exception as e:
        log.debug(f"CSIndex XLS failed: {e}")

    # 方案 B：东方财富行情 API（f162 = PE-TTM*100，指数若返回 '-' 则跳过）
    try:
        em_url = (
            "https://push2.eastmoney.com/api/qt/stock/get"
            "?secid=1.000300&fields=f162,f163&ut=fa5fd1943c7b386f172d6893dbfba10b&invt=2"
        )
        r2 = session.get(em_url, timeout=TIMEOUT)
        r2.raise_for_status()
        data = r2.json()
        f162 = (data.get("data") or {}).get("f162")
        if f162 and f162 != "-":
            return "CSI300_PE", {
                "pe_ttm":  _round(float(f162) / 100, 2),
                "status":  "ok",
                "source":  "eastmoney_api",
                "as_of":   _today(),
            }
    except Exception as e:
        log.debug(f"Eastmoney PE API failed: {e}")

    return "CSI300_PE", _unavail(
        "All PE sources failed", MANUAL_URLS["CSI300_PE"]
    )


def fetch_bond_yield_10y() -> Tuple[str, Dict]:
    """中国10年期国债收益率（%）。"""
    log.info("Fetching China 10Y Bond Yield …")

    if not _AK_OK:
        return "BOND_YIELD_10Y", _unavail("akshare not installed", MANUAL_URLS["bond_yield_10y"])

    try:
        df = ak.bond_zh_us_rate(start_date="20240101")
        if df is None or df.empty:
            raise ValueError("akshare returned empty bond yield data")

        cols = df.columns.tolist()
        log.debug(f"Bond yield columns: {cols}")

        # 找中国10年列
        cn10y_col = next(
            (c for c in cols if "中国" in c and "10年" in c),
            next((c for c in cols if "10" in c and "中" in c), None),
        )

        if cn10y_col is None:
            # 打印列名供调试，取第一个数值列
            numeric_cols = df.select_dtypes("number").columns.tolist()
            log.warning(f"Cannot find CN 10Y col in {cols}; using {numeric_cols}")
            cn10y_col = numeric_cols[0] if numeric_cols else None

        if cn10y_col is None:
            raise ValueError(f"CN 10Y column not found. Available: {cols}")

        latest = df.dropna(subset=[cn10y_col]).iloc[-1]
        date_col = next((c for c in cols if "日期" in c or "date" in c.lower()), cols[0])

        return "BOND_YIELD_10Y", {
            "yield_pct": _round(latest[cn10y_col], 4),
            "status":    "ok",
            "source":    "akshare/chinabond",
            "as_of":     str(latest[date_col]),
        }

    except Exception as exc:
        log.warning(f"Bond yield failed: {exc}")
        return "BOND_YIELD_10Y", _unavail(str(exc), MANUAL_URLS["bond_yield_10y"])


def fetch_margin_balance() -> Tuple[str, Dict]:
    """
    两市融资融券余额合计（亿元）= 上交所 + 深交所。
    akshare: macro_china_market_margin_sh() / macro_china_market_margin_sz()
    返回字段：融资融券余额（单位：元，脚本转换为亿元）
    """
    log.info("Fetching Margin Balance (SSE + SZSE) …")

    if not _AK_OK:
        return "MARGIN_BALANCE", _unavail("akshare not installed", MANUAL_URLS["margin_balance"])

    sse_val  = None
    szse_val = None
    sse_date = None

    def _extract_margin(df, label: str):
        """
        从融资融券 DataFrame 提取余额序列（元）和日期序列。
        返回：(series_values, series_dates, val_col)
        """
        if df is None or df.empty:
            raise ValueError(f"{label} returned empty DataFrame")
        cols     = df.columns.tolist()
        log.debug(f"{label} columns: {cols}")
        val_col  = next(
            (c for c in cols if "融资融券余额" in c),
            next((c for c in cols if "融资余额" in c), None),
        )
        date_col = next((c for c in cols if "日期" in c or "date" in c.lower()), cols[0])
        if val_col is None:
            raise ValueError(f"{label}: cannot find balance column in {cols}")
        values = df[val_col].astype(float)
        dates  = df[date_col].astype(str)
        return values, dates

    sse_vals = sse_dates = None
    szse_vals = None

    # 上交所
    try:
        df_sse = ak.macro_china_market_margin_sh()
        sse_vals, sse_dates = _extract_margin(df_sse, "SSE")
    except Exception as e:
        log.debug(f"SSE margin failed: {e}")

    # 深交所
    try:
        df_szse = ak.macro_china_market_margin_sz()
        szse_vals, _ = _extract_margin(df_szse, "SZSE")
    except Exception as e:
        log.debug(f"SZSE margin failed: {e}")

    if sse_vals is None and szse_vals is None:
        return "MARGIN_BALANCE", _unavail(
            "Both SSE and SZSE margin data unavailable",
            MANUAL_URLS["margin_balance"],
        )

    def _to_100m(v):
        if v is None:
            return None
        return _round(v / 1e8, 2)

    # 合并两市余额序列（按对齐的 index 相加）
    if sse_vals is not None and szse_vals is not None:
        # 对齐长度：取两者共同长度的末段
        n = min(len(sse_vals), len(szse_vals))
        total_series = sse_vals.iloc[-n:].values + szse_vals.iloc[-n:].values
    elif sse_vals is not None:
        total_series = sse_vals.values
    else:
        total_series = szse_vals.values

    current   = float(total_series[-1])
    ref_5d    = float(total_series[max(0, len(total_series) - 6)])   # ~5个交易日前
    ref_1m    = float(total_series[max(0, len(total_series) - 22)])  # ~1个月前

    chg_5d_amt  = _round((current - ref_5d) / 1e8, 2)               # 亿元变动量
    chg_5d_pct  = _round((current - ref_5d) / ref_5d * 100, 2) if ref_5d != 0 else None
    chg_1m_amt  = _round((current - ref_1m) / 1e8, 2)
    chg_1m_pct  = _round((current - ref_1m) / ref_1m * 100, 2) if ref_1m != 0 else None
    direction   = (
        "up"   if (chg_5d_pct or 0) >  0.1 else
        "down" if (chg_5d_pct or 0) < -0.1 else
        "flat"
    )

    sse_latest  = float(sse_vals.iloc[-1])  if sse_vals  is not None else None
    szse_latest = float(szse_vals.iloc[-1]) if szse_vals is not None else None

    return "MARGIN_BALANCE", {
        "total_100m":   _to_100m(current),
        "sse_100m":     _to_100m(sse_latest),
        "szse_100m":    _to_100m(szse_latest),
        "chg_5d_100m":  chg_5d_amt,
        "chg_5d_pct":   chg_5d_pct,
        "chg_1m_100m":  chg_1m_amt,
        "chg_1m_pct":   chg_1m_pct,
        "direction":    direction,
        "note":         "unit: 亿元; chg = change vs ~5 trading days / ~1 month ago",
        "status":       "partial" if (sse_vals is None or szse_vals is None) else "ok",
        "source":       "akshare/SSE+SZSE",
        "as_of":        str(sse_dates.iloc[-1]) if sse_dates is not None else _today(),
    }


def fetch_northbound_flow() -> Tuple[str, Dict]:
    """
    ak.stock_hsgt_hist_em(symbol="北向资金")
    列：日期, 当日成交净买额（亿元）
    升序，最新在 iloc[-1]
    """
    log.info("Fetching Northbound Flow …")
    if not _AK_OK:
        return "NORTHBOUND_FLOW", _unavail("akshare not installed")
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is None or df.empty:
            raise ValueError("empty DataFrame")
        df = df.sort_values("日期").reset_index(drop=True)
        net_col = "当日成交净买额"
        if net_col not in df.columns:
            raise ValueError(f"column '{net_col}' not found; actual: {df.columns.tolist()}")
        import math
        def _is_valid_num(v):
            try:
                return v is not None and not math.isnan(float(v))
            except Exception:
                return False

        today_val = _round(float(df[net_col].iloc[-1]), 2)
        as_of     = str(df["日期"].iloc[-1])

        if not _is_valid_num(today_val):
            return "NORTHBOUND_FLOW", _unavail(
                "NaN returned, possibly holiday or post-close"
            )

        cum_5d    = _round(float(df[net_col].iloc[-5:].sum()), 2)
        direction = (
            "inflow"  if (today_val or 0) > 0 else
            "outflow" if (today_val or 0) < 0 else
            "flat"
        )
        return "NORTHBOUND_FLOW", {
            "today_100m":         today_val,
            "cumulative_5d_100m": cum_5d,
            "direction":          direction,
            "note":               "unit: 亿元; data published after market close, may reflect prev day",
            "status":             "ok",
            "source":             "akshare/eastmoney",
            "as_of":              as_of,
        }
    except Exception as exc:
        log.warning(f"Northbound flow failed: {exc}")
        return "NORTHBOUND_FLOW", _unavail(str(exc))


# ─── Layer 4：全球市场估值 ─────────────────────────────────────────────────────

_SP500_PE_LOW,  _SP500_PE_HIGH  = 15.0, 25.0   # 历史均值约18-20x
_HSI_PE_LOW,    _HSI_PE_HIGH    = 8.0,  15.0   # 历史均值约10-12x


def _pe_zone(pe: float, low: float, high: float, mean_desc: str) -> str:
    if pe < low:
        return f"偏低（历史均值{mean_desc}）"
    if pe > high:
        return f"偏高（历史均值{mean_desc}）"
    return f"中性（历史均值{mean_desc}）"


def fetch_sp500_pe() -> Tuple[str, Dict]:
    """
    yfinance 对指数代码 ^GSPC 不返回 PE，改用 SPY（SPDR S&P 500 ETF）代理。
    SPY 的 trailingPE 与标普500整体 PE 高度一致。
    """
    log.info("Fetching S&P 500 PE (via SPY ETF) …")
    if not _YF_OK:
        return "SP500_PE", _unavail("yfinance not installed", MANUAL_URLS["sp500_pe"])
    try:
        info = yf.Ticker("SPY").info
        pe   = info.get("trailingPE") or info.get("forwardPE")
        if pe is None:
            raise ValueError("trailingPE not available in yfinance for SPY")
        pe = _round(float(pe), 2)
        return "SP500_PE", {
            "value":  pe,
            "zone":   _pe_zone(pe, _SP500_PE_LOW, _SP500_PE_HIGH, "18-20x"),
            "note":   "PE via SPY ETF (proxy for S&P 500)",
            "status": "ok",
            "source": "yfinance/SPY",
            "as_of":  _today(),
        }
    except Exception as exc:
        log.warning(f"SP500 PE failed: {exc}")
        return "SP500_PE", _unavail(str(exc), MANUAL_URLS["sp500_pe"])


def fetch_hsi_pe() -> Tuple[str, Dict]:
    """
    yfinance 对 HK 指数/ETF 的 PE 字段覆盖率不稳定，逐一尝试多个 ticker：
      2800.HK  盈富基金（最大 HSI 追踪 ETF）
      3188.HK  华夏沪深三百ETF（备选）
      EWH      iShares MSCI Hong Kong ETF（美股上市，港股代理）
    全部失败则记录 unavailable。
    """
    log.info("Fetching HSI PE …")
    if not _YF_OK:
        return "HSI_PE", _unavail("yfinance not installed", MANUAL_URLS["hsi_pe"])

    candidates = ["2800.HK", "3188.HK", "EWH"]
    for ticker in candidates:
        try:
            info = yf.Ticker(ticker).info
            pe   = info.get("trailingPE") or info.get("forwardPE")
            if pe is None:
                log.debug(f"HSI PE: {ticker} returned no PE field, trying next")
                continue
            pe = _round(float(pe), 2)
            return "HSI_PE", {
                "value":  pe,
                "zone":   _pe_zone(pe, _HSI_PE_LOW, _HSI_PE_HIGH, "10-12x"),
                "note":   f"PE via {ticker} (proxy for Hang Seng Index)",
                "status": "ok",
                "source": f"yfinance/{ticker}",
                "as_of":  _today(),
            }
        except Exception as e:
            log.debug(f"HSI PE: {ticker} failed: {e}")

    log.warning(f"HSI PE: all candidates {candidates} returned no PE")
    return "HSI_PE", _unavail(
        f"trailingPE not available for any of {candidates}",
        MANUAL_URLS["hsi_pe"],
    )


# ─── 衍生指标计算 ──────────────────────────────────────────────────────────────

def calc_derived(layer0: Dict, layer1: Dict, layer2: Dict, layer3: Dict, layer4: Dict) -> Dict:
    errors = []
    derived: Dict[str, Any] = {}

    # 铜金比 = 铜价(USD/lb) ÷ 金价(USD/oz) × 1000
    try:
        copper_v = (layer1.get("COPPER") or {}).get("value")
        gold_v   = (layer1.get("GOLD")   or {}).get("value")
        if copper_v and gold_v and gold_v != 0:
            ratio = _round(copper_v / gold_v * 1000, 2)
            # 经验区间（铜 USD/lb ÷ 金 USD/oz × 1000，历史范围约 1-5）
            # < 1.5 风险偏好弱，> 2.5 风险偏好强
            zone = (
                "bearish"  if ratio < 1.5 else
                "bullish"  if ratio > 2.5 else
                "neutral"
            )
            derived["copper_gold_ratio"] = {
                "value":              ratio,
                "interpretation_zone": zone,
                "status":             "ok",
            }
        else:
            derived["copper_gold_ratio"] = {
                "status": "unavailable",
                "reason": "copper or gold price missing",
            }
    except Exception as exc:
        derived["copper_gold_ratio"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "copper_gold_ratio", "error": str(exc)})

    # PPI-CPI 剪刀差
    try:
        ppi_v = (layer2.get("PPI") or {}).get("yoy_pct")
        cpi_v = (layer2.get("CPI") or {}).get("yoy_pct")
        if ppi_v is not None and cpi_v is not None:
            derived["ppi_cpi_spread"] = {
                "value":  _round(ppi_v - cpi_v, 2),
                "ppi":    ppi_v,
                "cpi":    cpi_v,
                "status": "ok",
            }
        else:
            derived["ppi_cpi_spread"] = {
                "status": "unavailable",
                "reason": "PPI or CPI yoy missing",
            }
    except Exception as exc:
        derived["ppi_cpi_spread"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "ppi_cpi_spread", "error": str(exc)})

    # ERP = 1 ÷ PE - 国债收益率 / 100（直接写入 layer3，不放 derived）
    try:
        pe_data  = layer3.get("CSI300_PE") or {}
        pe_v     = pe_data.get("pe_ttm") or pe_data.get("pe_total_shares")
        pe_as_of = pe_data.get("as_of", "unknown")
        pe_src   = pe_data.get("source", "unknown")
        bond_v   = (layer3.get("BOND_YIELD_10Y") or {}).get("yield_pct")
        bond_as_of = (layer3.get("BOND_YIELD_10Y") or {}).get("as_of", "unknown")
        if pe_v and pe_v != 0 and bond_v is not None:
            erp = _round(1 / pe_v - bond_v / 100, 6)
            layer3["ERP"] = {
                "value":          erp,
                "pe_static":      pe_v,
                "pe_as_of":       pe_as_of,
                "pe_source":      pe_src,
                "bond_yield_pct": bond_v,
                "bond_as_of":     bond_as_of,
                "note": (
                    "ERP = 1/PE - bond_yield/100; "
                    "PE is static (annual report basis, CSI methodology); "
                    "positive = equity premium over risk-free rate"
                ),
                "status": "ok",
                "as_of":  bond_as_of,
            }
        else:
            layer3["ERP"] = {
                "status": "unavailable",
                "reason": "PE or bond yield missing",
            }
    except Exception as exc:
        layer3["ERP"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "ERP", "error": str(exc)})

    # ── 中美10年期利差 ─────────────────────────────────────────────────────────
    try:
        us10y_v = (layer0.get("US10Y") or {}).get("value")
        cn10y_v = (layer3.get("BOND_YIELD_10Y") or {}).get("yield_pct")
        if us10y_v is not None and cn10y_v is not None:
            derived["cn_us_10y_spread"] = {
                "value":  _round(us10y_v - cn10y_v, 4),
                "us10y":  us10y_v,
                "cn10y":  cn10y_v,
                "note":   "US10Y - CN10Y；正值表示美国利率高于中国，负值表示中国利率高于美国",
                "status": "ok",
            }
        else:
            derived["cn_us_10y_spread"] = {
                "status": "unavailable",
                "reason": "US10Y or CN10Y missing",
            }
    except Exception as exc:
        derived["cn_us_10y_spread"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "cn_us_10y_spread", "error": str(exc)})

    # ── 美债期限利差 ───────────────────────────────────────────────────────────
    try:
        us10y_v = (layer0.get("US10Y") or {}).get("value")
        us2y_v  = (layer0.get("US2Y")  or {}).get("value")
        if us10y_v is not None and us2y_v is not None:
            derived["us_term_spread"] = {
                "value":  _round(us10y_v - us2y_v, 4),
                "us10y":  us10y_v,
                "us2y":   us2y_v,
                "note":   "US10Y - US2Y；负值=收益率曲线倒挂，历史上常为衰退先行指标",
                "status": "ok",
            }
        else:
            derived["us_term_spread"] = {
                "status": "unavailable",
                "reason": "US10Y or US2Y missing",
            }
    except Exception as exc:
        derived["us_term_spread"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "us_term_spread", "error": str(exc)})

    # ── 美国实际利率（近似） ───────────────────────────────────────────────────
    try:
        us10y_v = (layer0.get("US10Y") or {}).get("value")
        cn_cpi  = (layer2.get("CPI")   or {}).get("yoy_pct")
        if us10y_v is not None and cn_cpi is not None:
            derived["us_real_rate_approx"] = {
                "value":  _round(us10y_v - cn_cpi, 4),
                "us10y":  us10y_v,
                "cn_cpi": cn_cpi,
                "note":   "US10Y - CN_CPI（近似值，待条件允许时替换为美国CPI）",
                "status": "ok",
            }
        else:
            derived["us_real_rate_approx"] = {
                "status": "unavailable",
                "reason": "US10Y or CPI missing",
            }
    except Exception as exc:
        derived["us_real_rate_approx"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "us_real_rate_approx", "error": str(exc)})

    # ── 美股ERP（外资视角）= 1/SP500_PE − US10Y/100 ───────────────────────────
    try:
        sp500_pe = (layer4.get("SP500_PE") or {}).get("value")
        us10y_v  = (layer0.get("US10Y") or {}).get("value")
        if sp500_pe and sp500_pe != 0 and us10y_v is not None:
            derived["us_equity_erp"] = {
                "value":    _round(1 / sp500_pe - us10y_v / 100, 6),
                "sp500_pe": sp500_pe,
                "us10y":    us10y_v,
                "note":     "US ERP = 1/SP500_PE − US10Y/100；正值表示美股相对无风险利率仍有超额回报",
                "status":   "ok",
            }
        else:
            derived["us_equity_erp"] = {
                "status": "unavailable",
                "reason": "SP500_PE or US10Y missing",
            }
    except Exception as exc:
        derived["us_equity_erp"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "us_equity_erp", "error": str(exc)})

    # ── A股ERP（外资视角）= 1/CSI300_PE − US10Y/100 ──────────────────────────
    try:
        # CSI300 PE 已在 ERP 计算中存入 layer3["ERP"]["pe_static"]
        cn_pe    = (layer3.get("ERP") or {}).get("pe_static")
        us10y_v  = (layer0.get("US10Y") or {}).get("value")
        if cn_pe and cn_pe != 0 and us10y_v is not None:
            derived["cn_equity_erp_usd_basis"] = {
                "value":      _round(1 / cn_pe - us10y_v / 100, 6),
                "csi300_pe":  cn_pe,
                "us10y":      us10y_v,
                "note":       "A股ERP（外资视角）= 1/CSI300_PE − US10Y/100；反映外资用美国无风险利率衡量A股超额回报",
                "status":     "ok",
            }
        else:
            derived["cn_equity_erp_usd_basis"] = {
                "status": "unavailable",
                "reason": "CSI300_PE or US10Y missing",
            }
    except Exception as exc:
        derived["cn_equity_erp_usd_basis"] = {"status": "unavailable", "reason": str(exc)}
        errors.append({"indicator": "cn_equity_erp_usd_basis", "error": str(exc)})

    return derived, errors


# ─── 主流程 ────────────────────────────────────────────────────────────────────

def _latest_monthly_period(layer2: Dict) -> str:
    """
    从 layer2 数据中找最新统计期，只保留看起来像日期的字符串。
    接受格式：YYYY-MM[-DD]、YYYY年MM月[份]
    """
    import re
    date_pattern = re.compile(r"^\d{4}[-年]\d{1,2}")
    periods = []
    for v in layer2.values():
        p = (v or {}).get("period")
        if p and isinstance(p, str) and date_pattern.match(p.strip()):
            # 统一转成 YYYY-MM 便于比较
            normalized = p.strip()[:7].replace("年", "-")
            periods.append(normalized)
    return max(periods) if periods else "unknown"


def run() -> Dict:
    """并行采集所有指标，拼装 JSON。"""
    errors: List[Dict] = []

    # 注册所有采集函数（每个返回 (key, data)）
    fetch_tasks = [
        fetch_dxy,
        fetch_brent,
        fetch_copper,
        fetch_gold,
        fetch_vix,
        fetch_usdcny,
        fetch_fedwatch,
        fetch_pmi_official,
        fetch_pmi_caixin,
        fetch_new_loans,
        fetch_ppi,
        fetch_cpi,
        fetch_csi300_pe,
        fetch_bond_yield_10y,
        fetch_margin_balance,
        fetch_northbound_flow,
        fetch_us10y,
        fetch_us2y,
        fetch_sp500_pe,
        fetch_hsi_pe,
    ]

    layer0: Dict[str, Any] = {}
    layer1: Dict[str, Any] = {}
    layer2: Dict[str, Any] = {}
    layer3: Dict[str, Any] = {}
    layer4: Dict[str, Any] = {}

    # 归属映射
    L0_KEYS = {"US10Y", "US2Y"}
    L1_KEYS = {"DXY", "BRENT", "COPPER", "GOLD", "VIX", "FEDWATCH", "USDCNY"}
    L2_KEYS = {"PMI_OFFICIAL", "PMI_CAIXIN", "NEW_LOANS", "PPI", "CPI"}
    L3_KEYS = {"CSI300_PE", "BOND_YIELD_10Y", "MARGIN_BALANCE", "NORTHBOUND_FLOW"}
    L4_KEYS = {"SP500_PE", "HSI_PE"}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fn): fn.__name__ for fn in fetch_tasks}
        for future in as_completed(futures):
            fn_name = futures[future]
            try:
                key, data = future.result()
                if data.get("status") not in ("ok", "partial"):
                    errors.append({"indicator": key, "error": data.get("reason", "unknown")})
                if key in L0_KEYS:
                    layer0[key] = data
                elif key in L1_KEYS:
                    layer1[key] = data
                elif key in L2_KEYS:
                    layer2[key] = data
                elif key in L3_KEYS:
                    layer3[key] = data
                elif key in L4_KEYS:
                    layer4[key] = data
            except Exception as exc:
                errors.append({"indicator": fn_name, "error": str(exc)})
                log.error(f"Unexpected error in {fn_name}: {exc}")

    # LPR 串行执行（返回列表，需展开）
    for key, data in fetch_lpr():
        if data.get("status") not in ("ok", "partial"):
            errors.append({"indicator": key, "error": data.get("reason", "unknown")})
        layer2[key] = data

    # 衍生计算（ERP 在内部写入 layer3，其余衍生指标留在 derived）
    derived, derived_errors = calc_derived(layer0, layer1, layer2, layer3, layer4)
    errors.extend(derived_errors)

    # CSI300_PE 已作为 ERP 的中间变量，不在最终 JSON 顶层输出
    layer3.pop("CSI300_PE", None)

    # 组装 meta
    now = datetime.now()
    snapshot = {
        "meta": {
            "generated_at":   now.strftime("%Y-%m-%dT%H:%M:%S"),
            "data_freshness": {
                "realtime": _today(),
                "monthly":  _latest_monthly_period(layer2),
            },
        },
        "layer0_usd_bonds":        layer0,
        "layer1_external":         layer1,
        "layer2_domestic":         layer2,
        "layer3_market":           layer3,
        "layer4_global_valuation": layer4,
        "derived":                 derived,
        "errors":                  errors,
    }
    return snapshot


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info("=" * 55)
    log.info("  宏观股市分析框架 — 数据采集开始")
    log.info("=" * 55)
    t0 = datetime.now()

    snapshot = run()

    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"采集完成，耗时 {elapsed:.1f}s，错误数: {len(snapshot['errors'])}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    log.info(f"输出已写入：{OUTPUT_FILE}")

    # 打印摘要
    print("\n─── 采集摘要 ────────────────────────────────────")
    for layer_key in ("layer0_usd_bonds", "layer1_external", "layer2_domestic",
                      "layer3_market", "layer4_global_valuation"):
        for k, v in snapshot[layer_key].items():
            status = (v or {}).get("status", "?")
            mark   = "✓" if status == "ok" else ("~" if status == "partial" else "✗")
            print(f"  {mark} {k:<22} [{status}]")
    for k, v in snapshot["derived"].items():
        status = (v or {}).get("status", "?")
        mark   = "✓" if status == "ok" else ("~" if status == "partial" else "✗")
        print(f"  {mark} derived/{k:<18} [{status}]")
    if snapshot["errors"]:
        print(f"\n  错误详情：")
        for e in snapshot["errors"]:
            print(f"    - {e['indicator']}: {e['error']}")
    print("─────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
