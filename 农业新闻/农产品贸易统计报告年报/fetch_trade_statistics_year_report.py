# -*- coding: utf-8 -*-
"""
农产品贸易统计报告年报抓取脚本

数据来源：农业农村部“全国农产品批发市场价格信息系统”
接口：POST https://ncpscxx.moa.gov.cn/product/farmImorex/trade/selectYearImorexTradeList

用法：
    python fetch_trade_statistics_year_report.py            # 增量：抓最新一页，把新报告插到 md 顶部
    python fetch_trade_statistics_year_report.py --full     # 全量抓取并重建 md（首次使用请先跑一次）

说明：
    - 默认只抓最新一页，把 md 中尚未存在的年报按年份倒序插入到文件顶部。
    - --full 会抓取所有分页并重建 md，适合首次初始化或修复文件。
    - 接口优先走代理，代理最多尝试 3 次；3 次都失败后改为直连。
    - 每次请求之间、重试之间都有随机休眠，避免不间断抓取。
    - pageSize 固定为 100，尽量减少请求次数（年报数据一年一条，通常一页即可）。
    - 接口返回的 wordJson 字段包含年报正文的全部条目，本脚本按原文分节解析。
    - 所有年报汇总到一个 md 文件，按报告年份倒序排列（最新在前），
      年报之间空两行。
"""

import argparse
import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "农产品贸易统计报告年报.md"

API_URL = "https://ncpscxx.moa.gov.cn/product/farmImorex/trade/selectYearImorexTradeList"

PROXY_URL = os.getenv("PROXY_URL", "")
proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://ncpscxx.moa.gov.cn",
    "Referer": "https://ncpscxx.moa.gov.cn/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0"
    ),
    "sec-ch-ua": '"Microsoft Edge";v="155", "Chromium";v="155", "Not(A:Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

PAGE_SIZE = 100
PROXY_ATTEMPTS = 3
DIRECT_ATTEMPTS = 3
PROXY_TIMEOUT = 15
DIRECT_TIMEOUT = 30
INITIAL_DELAY_MIN = 1.0
INITIAL_DELAY_MAX = 2.0
MIN_DELAY = 3.0
MAX_DELAY = 6.0
RETRY_DELAY_MIN = 2.0
RETRY_DELAY_MAX = 4.0

DOC_TITLE = "农产品贸易统计报告年报"
BLOCK_SEPARATOR = "\n\n\n"  # 年报之间空两行
REPORT_HEADING_RE = re.compile(r"(?m)^## (.+)$")

# 年报正文的分节顺序与字段对应关系（字段名来自接口的 wordJson）。
SECTION_LAYOUT = [
    ("一、粮食", ["GRAIN_DESCRIPTION", "WHEAT", "CORN", "RICE", "BARLEY"]),
    ("二、棉花、食糖", ["COTTON", "SUGAR"]),
    ("三、食用油籽", ["EDIBLE_OIL_SEED"]),
    ("四、蔬菜、水果", ["VEGETABLES", "FRUITS"]),
    (
        "五、畜产品、水产品",
        [
            "LIVESTOCK_PRODUCTS",
            "PIG",
            "COW",
            "SHEEP",
            "DAIRY",
            "EGGS",
            "AQUATIC_PRODUCT",
        ],
    ),
]

# wordJson 缺失或解析失败时，退回顶层同义字段。
FALLBACK_FIELDS = {
    "GRAIN_DESCRIPTION": "grainDescription",
    "WHEAT": "wheat",
    "CORN": "corn",
    "RICE": "rice",
    "BARLEY": "barley",
    "COTTON": "cotton",
    "SUGAR": "sugar",
    "EDIBLE_OIL_SEED": "edibleOilSeed",
    "VEGETABLES": "vegetables",
    "FRUITS": "fruits",
    "LIVESTOCK_PRODUCTS": "livestockProducts",
    "PIG": "pig",
    "COW": "cow",
    "SHEEP": "sheep",
    "DAIRY": "dairy",
    "EGGS": "eggs",
    "AQUATIC_PRODUCT": "aquaticProduct",
}

# wordJson 里目前没有、但接口顶层保留的附加条目：有值时补到对应分节，避免漏内容。
SECTION_EXTRAS = {
    "一、粮食": ["cassava", "sorghum", "cornDistillerGrains"],
    "三、食用油籽": ["edibleVegetableOil"],
}

META_KEYS = {"CONCLUSION", "SOURCE"}


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def polite_sleep(min_seconds=MIN_DELAY, max_seconds=MAX_DELAY):
    delay = random.uniform(min_seconds, max_seconds)
    log(f"休眠 {delay:.1f} 秒...")
    time.sleep(delay)


def clean_text(value):
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    return " ".join(text.split())


def sort_key(record):
    return (
        record.get("reportTime") or "",
        record.get("publishDate") or "",
        record.get("id") or "",
    )


# ---------------------------------------------------------------------------
# 请求
# ---------------------------------------------------------------------------

def _post(session, payload, use_proxy, timeout):
    return session.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        proxies=proxies if use_proxy else {},
        timeout=timeout,
    )


def _parse_response(response):
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}")

    try:
        obj = response.json()
    except ValueError as exc:
        raise RuntimeError(f"返回内容不是 JSON：{response.text[:120]!r}") from exc

    if obj.get("code") not in (0, 200):
        raise RuntimeError(f"接口返回 code={obj.get('code')}，msg={obj.get('msg')}")

    data = obj.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("接口返回缺少 data 字段")
    if "records" not in data:
        raise RuntimeError("接口返回缺少 data.records 字段")
    return data


def fetch_page(session, page_number):
    payload = {
        "pageSize": PAGE_SIZE,
        "currentPage": page_number,
        "startTime": "",
        "endTime": "",
    }

    last_error = None

    for attempt in range(1, PROXY_ATTEMPTS + 1):
        try:
            log(f"第 {page_number} 页：代理请求（第 {attempt}/{PROXY_ATTEMPTS} 次）")
            response = _post(session, payload, use_proxy=True, timeout=PROXY_TIMEOUT)
            return _parse_response(response)
        except Exception as exc:
            last_error = exc
            log(f"代理请求失败：{exc}")
            if attempt < PROXY_ATTEMPTS:
                polite_sleep(RETRY_DELAY_MIN, RETRY_DELAY_MAX)

    log(f"代理 {PROXY_ATTEMPTS} 次均失败，改为直连")
    polite_sleep()

    for attempt in range(1, DIRECT_ATTEMPTS + 1):
        try:
            log(f"第 {page_number} 页：直连请求（第 {attempt}/{DIRECT_ATTEMPTS} 次）")
            response = _post(session, payload, use_proxy=False, timeout=DIRECT_TIMEOUT)
            return _parse_response(response)
        except Exception as exc:
            last_error = exc
            log(f"直连请求失败：{exc}")
            if attempt < DIRECT_ATTEMPTS:
                polite_sleep(RETRY_DELAY_MIN, RETRY_DELAY_MAX)

    raise RuntimeError(f"第 {page_number} 页抓取失败：{last_error}")


def fetch_all_records(session):
    first_page = fetch_page(session, 1)
    pages = int(first_page.get("pages") or 0)
    total = int(first_page.get("total") or 0)
    if pages <= 0:
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    records = list(first_page.get("records") or [])
    log(f"接口共 {total} 条、{pages} 页，第 1 页拿到 {len(records)} 条")

    for page_number in range(2, pages + 1):
        polite_sleep()
        data = fetch_page(session, page_number)
        page_records = list(data.get("records") or [])
        records.extend(page_records)
        log(f"第 {page_number} 页拿到 {len(page_records)} 条")

    return _dedupe(records)


def fetch_latest_records(session):
    data = fetch_page(session, 1)
    records = _dedupe(list(data.get("records") or []))
    report_time = clean_text(records[0].get("reportTime")) if records else ""
    log(f"最新一页拿到 {len(records)} 条，最新年份：{report_time or '未知'}")
    return records


def _dedupe(records):
    seen = set()
    result = []
    for record in records:
        record_id = record.get("id")
        if not record_id or record_id in seen:
            continue
        seen.add(record_id)
        result.append(record)
    return result


# ---------------------------------------------------------------------------
# 解析与格式化
# ---------------------------------------------------------------------------

def parse_word_json(record):
    raw = record.get("wordJson")
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        log(f"wordJson 解析失败（id={record.get('id')}），改用顶层字段")
        return {}
    return payload if isinstance(payload, dict) else {}


def _item_value(payload, record, key):
    value = clean_text(payload.get(key))
    if value:
        return value
    fallback = FALLBACK_FIELDS.get(key)
    if fallback:
        return clean_text(record.get(fallback))
    return ""


def _default_title(record):
    report_time = clean_text(record.get("reportTime"))
    if report_time:
        return f"{report_time}年我国农产品进出口情况"
    return f"农产品贸易统计报告年报（{report_time or '未知年份'}）"


def report_title(record, payload=None):
    payload = parse_word_json(record) if payload is None else payload
    return (
            clean_text(payload.get("CONCLUSION"))
            or clean_text(record.get("conclusion"))
            or _default_title(record)
    )


def build_report_block(record, title=None):
    payload = parse_word_json(record)
    title = report_title(record, payload) if title is None else title

    lines = [f"## {title}", ""]

    meta = []
    published = clean_text(record.get("publishDate") or record.get("createDate"))[:10]
    if published:
        meta.append(f"发布时间：{published}")

    source = clean_text(payload.get("SOURCE")) or clean_text(record.get("source"))
    if source:
        meta.append(f"数据来源：{source}")

    if meta:
        lines.append("　".join(meta))
        lines.append("")

    summary = clean_text(record.get("executiveSummary"))
    if summary:
        lines.extend(["### 摘要", "", summary, ""])

    used_keys = set(META_KEYS)
    for section_title, keys in SECTION_LAYOUT:
        used_keys.update(keys)
        items = [_item_value(payload, record, key) for key in keys]
        for extra_key in SECTION_EXTRAS.get(section_title, []):
            items.append(clean_text(record.get(extra_key)))
        items = [item for item in items if item]
        if not items:
            continue

        lines.extend([f"### {section_title}", ""])
        for item in items:
            lines.extend([item, ""])

    # 接口以后若新增字段，也一并输出，避免漏内容。
    extras = []
    for key in sorted(payload):
        if key in used_keys:
            continue
        value = clean_text(payload.get(key))
        if value:
            extras.append(value)

    if extras:
        lines.extend(["### 其他", ""])
        for item in extras:
            lines.extend([item, ""])

    return "\n".join(lines).strip()


def existing_report_titles():
    if not OUTPUT_FILE.exists():
        return set()
    text = OUTPUT_FILE.read_text(encoding="utf-8")
    return {match.group(1).strip() for match in REPORT_HEADING_RE.finditer(text)}


def collect_new_records(records, existing_titles):
    seen = set(existing_titles)
    result = []
    for record in sorted(records, key=sort_key, reverse=True):
        title = report_title(record)
        if title in seen:
            continue
        seen.add(title)
        result.append((record, title))
    return result


def prepend_blocks(items):
    header = f"# {DOC_TITLE}"
    existing = OUTPUT_FILE.read_text(encoding="utf-8") if OUTPUT_FILE.exists() else ""
    if existing.startswith(header):
        rest = existing[len(header):].strip()
    else:
        rest = existing.strip()

    parts = [
        header,
        BLOCK_SEPARATOR.join(
            build_report_block(record, title) for record, title in items
        ),
    ]
    if rest:
        parts.append(rest)

    OUTPUT_FILE.write_text(
        BLOCK_SEPARATOR.join(parts).rstrip() + "\n", encoding="utf-8"
    )
    log(f"已插入到 {OUTPUT_FILE.name} 顶部：{len(items)} 期")


def write_output(records):
    records.sort(key=sort_key, reverse=True)
    blocks = [build_report_block(record) for record in records]
    text = f"# {DOC_TITLE}\n\n{BLOCK_SEPARATOR.join(blocks)}\n"
    OUTPUT_FILE.write_text(text, encoding="utf-8")
    log(f"已写入 {OUTPUT_FILE.name}：{len(blocks)} 期")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    global OUTPUT_FILE

    parser = argparse.ArgumentParser(description="抓取农产品贸易统计报告年报")
    parser.add_argument(
        "--full",
        action="store_true",
        help="全量抓取并重建 md（首次使用请先跑一次）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_FILE,
        help=f"输出文件路径（默认：{OUTPUT_FILE.name}）",
    )
    args = parser.parse_args()

    OUTPUT_FILE = args.output

    log(f"开始抓取，pageSize={PAGE_SIZE}")

    session = requests.Session()
    session.trust_env = False

    polite_sleep(INITIAL_DELAY_MIN, INITIAL_DELAY_MAX)

    if args.full:
        records = fetch_all_records(session)
        if not records:
            log("没有抓取到任何年报，输出文件不变")
            return
        write_output(records)
        return

    records = fetch_latest_records(session)
    new_items = collect_new_records(records, existing_report_titles())
    if not new_items:
        log("没有新的年报，md 文件不变")
        return
    prepend_blocks(new_items)


if __name__ == "__main__":
    main()
