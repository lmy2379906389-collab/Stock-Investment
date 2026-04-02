#!/usr/bin/env python3
"""
fetch_space.py — 航天行业信息采集
输出: ./output/space_snapshot.json
用法: python fetch_space.py [--mode daily|weekly]
"""

import json, re, time, logging, argparse
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ─── 配置区 ───────────────────────────────────────────────────────────────────

OUTPUT_DIR  = Path(__file__).parent / "output"
OUTPUT_FILE = OUTPUT_DIR / "space_snapshot.json"

WINDOW_DAYS     = 14    # 数据保留窗口（天）
MAX_FILE_KB     = 60    # 文件大小上限（KB）
REQUEST_TIMEOUT = 15    # 请求超时（秒）
REQUEST_RETRIES = 2     # 重试次数

# launch_events 关键词（命中任意一个则保留）
LAUNCH_KEYWORDS = ["发射", "入轨", "载人", "航天器", "火箭", "卫星", "天舟", "神舟", "长征"]

# procurement 搜索关键词 & 条数
PROCUREMENT_KEYWORDS = ["卫星", "火箭", "航天", "发射场", "载人航天", "液氧甲烷"]
PROCUREMENT_EXCLUDE  = ["大学", "学院", "学校", "医院", "附属", "研究所"]   # 排除高校/医院误匹配
PROCUREMENT_TOP_N    = 10
PROCUREMENT_PAGES    = 5     # 每个分类爬取的列表页数
PROCUREMENT_MIN_AMT  = 100   # 万元，0 = 不限

# announcements 目标上市公司（A 股代码）
SPACE_STOCKS = [
    ("600118", "中国卫星"),
    ("600343", "航天动力"),
    ("600271", "航天信息"),
    ("002151", "北斗星通"),
    ("002416", "航天发展"),
    ("603698", "航天工程"),
    ("601698", "中国卫通"),
    ("002829", "星网宇达"),
]
ANNOUNCE_PER_STOCK   = 2
ANNOUNCE_TOTAL       = 8
ANNOUNCE_WINDOW_DAYS = 60    # 重大合同发布频率低，用更长窗口
ANNOUNCE_KEYWORDS    = ["重大合同", "重大订单", "中标", "采购合同", "战略合作协议", "框架协议",
                        "投资建设", "投资项目", "新项目", "子公司投资"]

# funding_news 关键词
FUNDING_KEYWORDS = ["商业航天", "卫星互联网", "火箭", "航天"]
FUNDING_FILTER   = ["融资", "投资", "获投", "亿元", "战略融资"]
FUNDING_TOP_N    = 10

# ─── 日志 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── HTTP 工具 ─────────────────────────────────────────────────────────────────

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
})


def _get(url: str, **kwargs) -> requests.Response:
    """带重试的 GET 请求"""
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    last_exc: Exception = RuntimeError("no attempts")
    for attempt in range(REQUEST_RETRIES + 1):
        try:
            resp = _SESSION.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if attempt < REQUEST_RETRIES:
                time.sleep(1)
    raise last_exc


# ─── 日期工具 ──────────────────────────────────────────────────────────────────

def _parse_date(text: str) -> str:
    """从任意字符串中提取日期，返回 YYYY-MM-DD 或空字符串"""
    patterns = [
        (r"(\d{4}-\d{2}-\d{2})",          lambda m: m.group(1)),
        (r"(\d{4}/\d{2}/\d{2})",          lambda m: m.group(1).replace("/", "-")),
        (r"(\d{4})年(\d{1,2})月(\d{1,2})日",
         lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
        (r"(\d{4})年(\d{1,2})月",
         lambda m: f"{m.group(1)}-{int(m.group(2)):02d}"),
    ]
    for pattern, fmt in patterns:
        m = re.search(pattern, str(text))
        if m:
            return fmt(m)
    return ""


def _cutoff_date() -> str:
    return (datetime.now() - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")


def _within_window(date_str: str) -> bool:
    if not date_str:
        return True   # 无日期条目保留
    return date_str >= _cutoff_date()


# ─── 各类别采集 ────────────────────────────────────────────────────────────────

def fetch_launch_events() -> List[Dict]:
    """从 cnsa.gov.cn 采集近期发射任务新闻"""
    items: List[Dict] = []
    base_url = "https://www.cnsa.gov.cn"
    list_url = f"{base_url}/n6758823/n6758838/index.html"

    # 用 r.content 传入 BeautifulSoup，让 bs4 自动检测编码（避免 requests 误判 ISO-8859-1）
    resp = _get(list_url)
    soup = BeautifulSoup(resp.content, "html.parser")

    links: List[tuple] = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if any(kw in text for kw in LAUNCH_KEYWORDS):
            href = a["href"]
            full_href = urljoin(list_url, href)
            links.append((text, full_href))

    for title, link in links[:20]:
        # 过滤非 cnsa.gov.cn 链接（导航栏跳转到其他站点的误抓）
        if "cnsa.gov.cn" not in link:
            continue
        try:
            dr = _get(link)
            dsoup = BeautifulSoup(dr.content, "html.parser")

            date_str = ""
            for tag in dsoup.find_all(
                ["span", "p", "div"],
                class_=re.compile(r"date|time|pub", re.I)
            ):
                date_str = _parse_date(tag.get_text())
                if date_str:
                    break
            if not date_str:
                date_str = _parse_date(link) or _parse_date(title)

            # 丢弃无日期条目（通常是导航/广告链接）
            if not date_str:
                continue
            if not _within_window(date_str):
                continue

            summary = ""
            content_div = dsoup.find("div", class_=re.compile(r"content|article|text", re.I))
            if content_div:
                summary = " ".join(
                    p.get_text(strip=True) for p in content_div.find_all("p")[:3]
                )[:300]

            items.append({
                "title":   title,
                "date":    date_str,
                "url":     link,
                "summary": summary,
                "source":  "cnsa.gov.cn",
            })
        except Exception as e:
            log.debug(f"[launch_events] detail failed {link}: {e}")

    log.info(f"[launch_events] {len(items)} items")
    return items


def fetch_procurement() -> List[Dict]:
    """从 ccgp.gov.cn 各类公告列表页采集航天相关政府采购公告。
    search.ccgp.gov.cn 限制 IP 频率，改为直接爬列表页关键词过滤。"""
    all_items: List[Dict] = []
    seen_urls: set = set()

    # CCGP 各公告分类列表页（中标/招标/成交）
    CCGP_SECTIONS = [
        ("https://www.ccgp.gov.cn/cggg/zygg/zbgg/",  "招标公告"),
        ("https://www.ccgp.gov.cn/cggg/zygg/cjgg/",  "成交公告"),
        ("https://www.ccgp.gov.cn/cggg/dfgg/zbgg/",  "地方招标"),
    ]
    SPACE_KW = PROCUREMENT_KEYWORDS

    def _parse_amount(detail_url: str) -> Optional[float]:
        """从详情页提取中标金额（万元）"""
        try:
            resp = _get(detail_url)
            text = BeautifulSoup(resp.content, "html.parser").get_text(" ", strip=True)
            # 匹配"总中标金额 ￥91.5 万元"或"采购预算金额 100 万元"等格式
            m = re.search(r'(?:总[中成交]标金额|采购预算金额|合同金额)[^¥￥\d]{0,10}[¥￥]?\s*([\d,.]+)\s*万元', text)
            if m:
                return float(m.group(1).replace(",", ""))
        except Exception:
            pass
        return None

    def _scrape_section(section_url: str, section_name: str) -> List[Dict]:
        results: List[Dict] = []
        page_found = 0
        for page in range(1, PROCUREMENT_PAGES + 1):
            try:
                url = section_url if page == 1 else f"{section_url}index_{page}.htm"
                resp = _get(url)
                soup = BeautifulSoup(resp.content, "html.parser")

                for a in soup.find_all("a", href=re.compile(r"\./\d{6}/t\d+|/cggg/")):
                    title = a.get_text(strip=True)
                    if not title:
                        continue
                    if not any(kw in title for kw in SPACE_KW):
                        continue
                    if any(ex in title for ex in PROCUREMENT_EXCLUDE):
                        log.debug(f"[procurement] excluded (academic/hospital): {title[:50]}")
                        continue
                    href = a["href"]
                    if href.startswith("./"):
                        full_url = section_url + href[2:]
                    elif href.startswith("/"):
                        full_url = "https://www.ccgp.gov.cn" + href
                    else:
                        full_url = href

                    if full_url in seen_urls:
                        continue

                    # 日期：从相邻文本或 URL 提取
                    date_str = _parse_date(a.parent.get_text() if a.parent else "") or _parse_date(href)

                    if not _within_window(date_str):
                        continue

                    # 金额在详情页，单独请求
                    amount = _parse_amount(full_url)

                    results.append({
                        "title":       title,
                        "date":        date_str,
                        "url":         full_url,
                        "amount_wan":  amount,
                        "section":     section_name,
                        "source":      "ccgp.gov.cn",
                    })
                    page_found += 1
            except Exception as e:
                log.warning(f"[procurement] {section_name} page {page} request failed: {e}")
        if page_found == 0:
            log.info(f"[procurement] {section_name}: 0 space items in {PROCUREMENT_PAGES} pages "
                     f"(keywords={PROCUREMENT_KEYWORDS}, exclude={PROCUREMENT_EXCLUDE})")
        return results

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_scrape_section, url, name): name for url, name in CCGP_SECTIONS}
        for fut in as_completed(futures):
            for item in fut.result():
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    all_items.append(item)

    all_items.sort(key=lambda x: x.get("date", ""), reverse=True)
    all_items = all_items[:PROCUREMENT_TOP_N]
    log.info(f"[procurement] {len(all_items)} items")
    return all_items


def fetch_announcements() -> List[Dict]:
    """从 cninfo.com.cn 官方 API 采集目标上市公司重大公告。
    stock_notice_report(symbol=代码) 是错误用法——该函数 symbol 参数是公告类型，
    改为直接请求巨潮 API，先查 orgId，再按公司查近期公告，标题关键词过滤。"""

    # SH vs SZ 对应 column/plate 参数
    _EXCH = {
        "600": ("sse", "sh"), "601": ("sse", "sh"),
        "603": ("sse", "sh"), "688": ("sse", "sh"),
        "000": ("szse", "sz"), "001": ("szse", "sz"),
        "002": ("szse", "sz"), "003": ("szse", "sz"),
        "300": ("szse", "sz"), "301": ("szse", "sz"),
    }

    CNINFO_HEADERS = {
        "User-Agent": _SESSION.headers["User-Agent"],
        "Referer": "http://www.cninfo.com.cn/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    def _get_org_id(code: str, name: str) -> str:
        try:
            r = requests.post(
                "http://www.cninfo.com.cn/new/information/topSearch/query",
                data={"keyWord": name, "maxNum": 5},
                headers=CNINFO_HEADERS, timeout=10
            )
            for item in r.json():
                if item.get("code") == code:
                    return item.get("orgId", "")
        except Exception:
            pass
        return ""

    def _fetch_stock(code: str, name: str) -> List[Dict]:
        results: List[Dict] = []
        try:
            org_id = _get_org_id(code, name)
            if not org_id:
                return results

            prefix = code[:3]
            column, plate = _EXCH.get(prefix, ("sse", "sh"))
            start = (datetime.now() - timedelta(days=ANNOUNCE_WINDOW_DAYS)).strftime("%Y-%m-%d")
            end   = datetime.now().strftime("%Y-%m-%d")

            r = requests.post(
                "http://www.cninfo.com.cn/new/hisAnnouncement/query",
                data={
                    "stock": f"{code},{org_id}",
                    "tabName": "fulltext",
                    "pageSize": "30",
                    "pageNum": "1",
                    "column": column,
                    "category": "",
                    "plate": plate,
                    "seDate": f"{start}~{end}",
                    "searchkey": "",
                    "secid": "",
                    "sortName": "",
                    "sortType": "",
                    "isHLtitle": "true",
                },
                headers=CNINFO_HEADERS, timeout=10
            )
            anns = r.json().get("announcements") or []

            count = 0
            for ann in anns:
                if count >= ANNOUNCE_PER_STOCK:
                    break
                title = ann.get("announcementTitle", "")
                # Unix 毫秒时间戳 → YYYY-MM-DD
                ts = ann.get("announcementTime", 0)
                date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else ""
                ann_url = ("http://static.cninfo.com.cn/" + ann.get("adjunctUrl", "")
                           if ann.get("adjunctUrl") else "")

                if not any(kw in title for kw in ANNOUNCE_KEYWORDS):
                    continue

                results.append({
                    "title":      title,
                    "date":       date_str,
                    "url":        ann_url,
                    "company":    name,
                    "stock_code": code,
                    "source":     "cninfo.com.cn",
                })
                count += 1
        except Exception as e:
            log.debug(f"[announcements] {name}({code}) failed: {e}")
        return results

    items: List[Dict] = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_fetch_stock, code, name): (code, name) for code, name in SPACE_STOCKS}
        for fut in as_completed(futures):
            items.extend(fut.result())

    items.sort(key=lambda x: x.get("date", ""), reverse=True)
    items = items[:ANNOUNCE_TOTAL]
    log.info(f"[announcements] {len(items)} items")
    return items


def fetch_funding_news() -> List[Dict]:
    """从东方财富个股新闻接口采集航天行业融资/投资资讯。
    36kr 全 SPA 渲染无可用 API，改用 akshare.stock_news_em(keyword) 抓取行业新闻，
    标题/摘要过滤融资相关关键词。"""
    import akshare as ak

    items: List[Dict] = []
    seen_urls: set = set()
    seen_title_prefixes: set = set()   # 标题前10字去重

    for kw in FUNDING_KEYWORDS:
        try:
            df = ak.stock_news_em(symbol=kw)
            if df is None or df.empty:
                continue

            # 列名: 关键词 / 新闻标题 / 新闻内容 / 发布时间 / 文章来源 / 新闻链接
            for _, row in df.iterrows():
                title   = str(row.get("新闻标题", ""))
                content = str(row.get("新闻内容", ""))
                pub     = str(row.get("发布时间", ""))
                url     = str(row.get("新闻链接", ""))
                source  = str(row.get("文章来源", ""))

                combined = title + content
                if not any(f in combined for f in FUNDING_FILTER):
                    continue
                if url in seen_urls:
                    continue
                title_prefix = title[:10]
                if title_prefix and title_prefix in seen_title_prefixes:
                    continue

                date_str = _parse_date(pub)
                if not _within_window(date_str):
                    continue

                seen_urls.add(url)
                seen_title_prefixes.add(title_prefix)
                items.append({
                    "title":   title,
                    "date":    date_str,
                    "url":     url,
                    "summary": content[:200],
                    "keyword": kw,
                    "source":  source or "eastmoney.com",
                })
        except Exception as e:
            log.debug(f"[funding_news] keyword '{kw}' failed: {e}")

    items.sort(key=lambda x: x.get("date", ""), reverse=True)
    items = items[:FUNDING_TOP_N]
    log.info(f"[funding_news] {len(items)} items")
    return items


def fetch_ipo_progress() -> List[Dict]:
    """从 sse.com.cn 采集科创板航天企业 IPO 审核进度（仅 weekly 模式）"""
    items: List[Dict] = []
    SPACE_KEYWORDS = ["航天", "卫星", "火箭", "宇航", "空间", "导航", "北斗"]

    # SSE 公开查询接口（分页抓取全量）
    # 字段说明：stockAuditName=企业名, currStatus=审核状态码, updateDate=更新时间(YYYYMMDDHHmmss)
    CURR_STATUS_MAP = {
        1: "已受理", 2: "已问询", 3: "上市委会议", 4: "提交注册",
        5: "注册生效", 6: "中止", 7: "终止",
    }
    try:
        # SSE API 需要先访问首页获取 cookie，否则返回空响应
        sse_sess = requests.Session()
        sse_sess.headers.update({"User-Agent": _SESSION.headers["User-Agent"]})
        sse_sess.get("http://www.sse.com.cn/", timeout=REQUEST_TIMEOUT)

        api_url = "http://query.sse.com.cn/statusAction.do"
        base_params = {
            "isPagination": "true",
            "pageHelperCount": "100",
            "sqlId": "SH_XM_LB",
            "productType": "star",
        }
        headers = {"Referer": "http://www.sse.com.cn/"}
        start = 0
        while True:
            params = {**base_params, "pageHelperStart": str(start)}
            resp = sse_sess.get(api_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            data = resp.json()
            batch = data.get("result", [])
            if not batch:
                break
            for row in batch:
                company    = row.get("stockAuditName", "")
                status_code = row.get("currStatus", 0)
                status     = CURR_STATUS_MAP.get(status_code, str(status_code))
                update_str = str(row.get("updateDate", ""))
                # updateDate 格式 YYYYMMDDHHmmss → parse as YYYY-MM-DD
                update_date = (f"{update_str[:4]}-{update_str[4:6]}-{update_str[6:8]}"
                               if len(update_str) >= 8 else "")
                if not any(kw in company for kw in SPACE_KEYWORDS):
                    continue
                items.append({
                    "company":     company,
                    "status":      status,
                    "update_date": update_date,
                    "source":      "sse.com.cn",
                })
            # 判断是否还有更多页
            page_help = data.get("pageHelp", {})
            total = page_help.get("total", 0) if isinstance(page_help, dict) else 0
            start += len(batch)
            if start >= total or len(batch) < 100:
                break

        log.info(f"[ipo_progress] {len(items)} items (API)")
        return items

    except Exception as e:
        log.warning(f"[ipo_progress] API failed: {e}, trying playwright...")

    # playwright 备用（可选依赖）
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page    = browser.new_page()
            page.goto("https://www.sse.com.cn/star/renewal/", timeout=30000)
            page.wait_for_selector("table", timeout=10000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")
        for row in soup.select("table tbody tr"):
            cols = row.find_all("td")
            if len(cols) < 3:
                continue
            company    = cols[0].get_text(strip=True)
            status     = cols[2].get_text(strip=True) if len(cols) > 2 else ""
            update_date = _parse_date(cols[-1].get_text(strip=True))

            if not any(kw in company for kw in SPACE_KEYWORDS):
                continue
            items.append({
                "company":     company,
                "status":      status,
                "update_date": update_date,
                "source":      "sse.com.cn",
            })

        log.info(f"[ipo_progress] {len(items)} items (playwright)")
        return items

    except ImportError:
        log.warning("[ipo_progress] playwright not installed")
        raise RuntimeError("playwright not available")
    except Exception as e:
        log.warning(f"[ipo_progress] playwright also failed: {e}")
        raise


# ─── 增量合并 ──────────────────────────────────────────────────────────────────

def _merge_list(existing: List[Dict], new_items: List[Dict], key: str = "url") -> List[Dict]:
    """以 key 去重，新数据前插，按 date 降序，裁 WINDOW_DAYS 窗口"""
    existing_keys = {item.get(key) for item in existing if item.get(key)}
    merged = [item for item in new_items if item.get(key) not in existing_keys] + existing
    merged = [item for item in merged if _within_window(item.get("date", ""))]
    merged.sort(key=lambda x: x.get("date", ""), reverse=True)
    return merged


def _merge_ipo(existing: List[Dict], new_items: List[Dict]) -> List[Dict]:
    """IPO 进度以公司名去重，直接替换（状态会变更）"""
    by_company = {item["company"]: item for item in existing}
    for item in new_items:
        by_company[item["company"]] = item
    return list(by_company.values())


# ─── 体积控制 ──────────────────────────────────────────────────────────────────

def _size_bytes(snapshot: Dict) -> int:
    return len(json.dumps(snapshot, ensure_ascii=False).encode("utf-8"))


def _enforce_size_limit(snapshot: Dict) -> Dict:
    if _size_bytes(snapshot) <= MAX_FILE_KB * 1024:
        return snapshot

    log.warning("Snapshot exceeds size limit, truncating text fields...")
    for key in ["launch_events", "procurement", "announcements", "funding_news"]:
        for item in snapshot.get(key, []):
            for field in ["summary", "text", "description"]:
                if isinstance(item.get(field), str):
                    item[field] = item[field][:len(item[field]) // 2]

    if _size_bytes(snapshot) <= MAX_FILE_KB * 1024:
        return snapshot

    # 再按旧→新顺序删除超限条目
    for key in ["launch_events", "procurement", "announcements", "funding_news"]:
        lst = snapshot.get(key, [])
        while len(lst) > 1 and _size_bytes(snapshot) > MAX_FILE_KB * 1024:
            lst.pop()

    return snapshot


# ─── 主入口 ────────────────────────────────────────────────────────────────────

def run(mode: str = "daily") -> Dict:
    log.info(f"fetch_space.run() mode={mode}")

    # 读现有数据
    existing: Dict = {}
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    errors: List[Dict] = []
    new_data: Dict[str, List] = {}

    # 并行采集（daily 跳过 ipo_progress）
    fetch_tasks: Dict[str, object] = {
        "launch_events": fetch_launch_events,
        "procurement":   fetch_procurement,
        "announcements": fetch_announcements,
        "funding_news":  fetch_funding_news,
    }
    if mode == "weekly":
        fetch_tasks["ipo_progress"] = fetch_ipo_progress

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn): name for name, fn in fetch_tasks.items()}  # type: ignore
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                new_data[name] = fut.result()
            except Exception as e:
                log.warning(f"[{name}] failed: {e}")
                errors.append({
                    "category": name,
                    "error":    str(e),
                    "time":     datetime.now().isoformat(),
                })
                new_data[name] = []

    # 增量合并
    snapshot: Dict = {
        "launch_events": _merge_list(existing.get("launch_events", []), new_data.get("launch_events", [])),
        "procurement":   _merge_list(existing.get("procurement",   []), new_data.get("procurement",   [])),
        "announcements": _merge_list(existing.get("announcements", []), new_data.get("announcements", [])),
        "funding_news":  _merge_list(existing.get("funding_news",  []), new_data.get("funding_news",  [])),
    }
    if mode == "weekly":
        snapshot["ipo_progress"] = _merge_ipo(
            existing.get("ipo_progress", []), new_data.get("ipo_progress", [])
        )
    else:
        snapshot["ipo_progress"] = existing.get("ipo_progress", [])

    # 合并旧 errors（只保留 24h 内）
    cutoff_ts = (datetime.now() - timedelta(hours=24)).isoformat()
    old_errors = [e for e in existing.get("errors", []) if e.get("time", "") >= cutoff_ts]
    snapshot["errors"] = old_errors + errors

    # 体积控制
    _enforce_size_limit(snapshot)

    # Meta
    total_items = sum(
        len(snapshot.get(k, []))
        for k in ["launch_events", "procurement", "announcements", "funding_news", "ipo_progress"]
    )
    size_kb = _size_bytes(snapshot) / 1024

    snapshot["meta"] = {
        "generated_at":      datetime.now().isoformat(),
        "mode":              mode,
        "data_window_days":  WINDOW_DAYS,
        "total_items":       total_items,
        "size_kb":           round(size_kb, 1),
        "categories": {
            k: len(snapshot.get(k, []))
            for k in ["launch_events", "procurement", "announcements", "funding_news", "ipo_progress"]
        },
        "read_instructions": (
            "Space industry snapshot for macro analysis. "
            "launch_events: recent CNSA launch/mission news. "
            "procurement: govt procurement announcements sorted by amount desc. "
            "announcements: major contract notices from listed space companies (A-share). "
            "funding_news: recent investment/funding news from 36kr. "
            "ipo_progress: space company IPO status on STAR Market (weekly update only). "
            "errors: categories that failed in this run."
        ),
    }

    # 写文件
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    log.info(f"Saved {OUTPUT_FILE} ({size_kb:.1f} KB, {total_items} items)")

    # 终端摘要
    print(f"\n{'='*54}")
    print(f"Space Snapshot  {snapshot['meta']['generated_at']}")
    print(f"Mode: {mode}  |  Size: {size_kb:.1f} KB  |  Items: {total_items}")
    print(f"{'='*54}")
    for k in ["launch_events", "procurement", "announcements", "funding_news", "ipo_progress"]:
        n = len(snapshot.get(k, []))
        tag = "[ok]" if n > 0 else "[empty]"
        print(f"  {k:<22} {tag}  {n} items")
    if errors:
        print(f"  Errors: {len(errors)}")
        for err in errors:
            print(f"    - {err['category']}: {err['error'][:80]}")
    print(f"{'='*54}\n")

    return snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch space industry snapshot")
    parser.add_argument("--mode", choices=["daily", "weekly"], default="daily",
                        help="daily: skip ipo_progress; weekly: include ipo_progress")
    args = parser.parse_args()
    run(mode=args.mode)
