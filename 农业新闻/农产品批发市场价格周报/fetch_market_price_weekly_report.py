# -*- coding: utf-8 -*-
"""
农产品批发市场价格周报抓取脚本

数据来源：农业农村部“全国农产品批发市场价格信息系统”
接口：POST https://ncpscxx.moa.gov.cn/product/FarmDaily/selectWeeklyList

用法：
    python fetch_market_price_weekly_report.py            # 每周增量：只抓最新一页，插入 md 顶部
    python fetch_market_price_weekly_report.py --full     # 全量抓取并重建 md 文件

说明：
    - 不带参数运行时默认执行“最新一期”增量抓取；首次全量请用 --full。
    - 接口优先走代理，代理最多尝试 3 次；3 次都失败后改为直连。
    - 每次请求之间、重试之间都有随机休眠，避免不间断抓取。
    - pageSize 固定为 100，尽量减少请求次数。
    - 所有周报汇总到一个 md 文件，按时间倒序排列（最新在前），周报之间空两行。
    - 增量抓取时，新周报插入到文件顶部，保持最新在前。
    - 网站每周五 14:00 左右发布新周报，周五到下周四为一周。
"""

import argparse
import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from lxml import html

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "农产品批发市场价格周报.md"
STATE_FILE = BASE_DIR / "fetched_state.json"

API_URL = "https://ncpscxx.moa.gov.cn/product/FarmDaily/selectWeeklyList"

PROXY_URL = os.getenv("PROXY_URL", "")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}

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
PROXY_TIMEOUT = 20
DIRECT_TIMEOUT = 30
MIN_DELAY = 3.0
MAX_DELAY = 6.0
RETRY_DELAY_MIN = 2.0
RETRY_DELAY_MAX = 4.0

DOC_TITLE = "农产品批发市场价格周报"
BLOCK_SEPARATOR = "\n\n\n"  # 周报之间空两行


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def polite_sleep(min_seconds=MIN_DELAY, max_seconds=MAX_DELAY):
    delay = random.uniform(min_seconds, max_seconds)
    log(f"休眠 {delay:.1f} 秒...")
    time.sleep(delay)


def publish_date(record):
    value = record.get("createDate") or record.get("updateDate") or ""
    return value[:10]


def sort_key(record):
    return (
        record.get("weeklyDate") or "",
        record.get("createDate") or "",
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
        proxies=PROXIES if use_proxy else {},
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
    pages = int(first_page.get("pages") or 1)
    total = first_page.get("total")
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
    log(f"最新一页拿到 {len(data.get('records') or [])} 条")
    return _dedupe(list(data.get("records") or []))


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
# HTML -> Markdown
# ---------------------------------------------------------------------------

def _clean_inline(text):
    text = (text or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _escape_table_cell(text):
    return _clean_inline(text).replace("|", "\\|")


def _int_attr(element, name):
    try:
        return max(1, int(element.get(name) or 1))
    except (TypeError, ValueError):
        return 1


def _build_grid(trs):
    """把带 rowspan/colspan 的表格展开成二维网格。"""
    grid = []
    spans = {}  # 列号 -> (文本, 还需向下覆盖的行数)

    for tr in trs:
        cells = tr.xpath("./td|./th")
        row = []
        cell_index = 0
        col = 0

        while cell_index < len(cells) or any(c >= col for c in spans):
            if col in spans:
                text, remaining = spans[col]
                row.append(text)
                if remaining <= 1:
                    del spans[col]
                else:
                    spans[col] = (text, remaining - 1)
                col += 1
                continue

            if cell_index >= len(cells):
                row.append("")
                col += 1
                continue

            cell = cells[cell_index]
            cell_index += 1
            text = _escape_table_cell(cell.text_content())
            colspan = _int_attr(cell, "colspan")
            rowspan = _int_attr(cell, "rowspan")

            for offset in range(colspan):
                row.append(text)
                if rowspan > 1:
                    spans[col + offset] = (text, rowspan - 1)
            col += colspan

        grid.append(row)

    if not grid:
        return []

    width = max(len(row) for row in grid)
    for row in grid:
        row.extend([""] * (width - len(row)))
    return grid


def _combine_header_rows(rows):
    """把多行表头合并成一行，例如“上周”+“单位（元/公斤）” -> “上周（元/公斤）”。"""
    if not rows:
        return []

    width = len(rows[0])
    headers = []

    for col in range(width):
        parts = []
        for row in rows:
            text = row[col].strip()
            if text and (not parts or parts[-1] != text):
                parts.append(text)

        if not parts:
            headers.append("")
            continue

        base = parts[0]
        units = []
        leftovers = []

        for part in parts[1:]:
            if part.startswith("单位（") and part.endswith("）"):
                units.append(part[len("单位（"):-1])
            elif part.startswith("（") and part.endswith("）"):
                units.append(part[1:-1])
            elif part != base:
                leftovers.append(part)

        if units:
            headers.append(f"{base}（{units[0]}）")
        elif leftovers:
            headers.append(" / ".join([base] + leftovers))
        else:
            headers.append(base)

    return headers


def _looks_like_header_continuation(row):
    """判断无 thead 的老表格中，紧跟表头的行是不是单位行。"""
    for cell in row:
        text = cell.replace("\\|", "|").strip()
        if text.startswith("单位") or text in ("（%）", "(%)", "%", "％"):
            return True
    return False


def _table_to_markdown(table):
    has_thead = bool(table.xpath("./thead/tr"))
    header_trs = table.xpath("./thead/tr")
    body_trs = table.xpath("./tbody/tr")

    if not header_trs and not body_trs:
        all_trs = table.xpath(".//tr")
        header_trs = all_trs[:1]
        body_trs = all_trs[1:]

    header_grid = _build_grid(header_trs)
    body_grid = _build_grid(body_trs)

    if not header_grid:
        if not body_grid:
            return ""
        header_grid, body_grid = body_grid[:1], body_grid[1:]

    if not has_thead:
        # 老格式没有 thead，第一行表头后面通常还有一行单位，需要并入表头
        while body_grid and _looks_like_header_continuation(body_grid[0]):
            header_grid.append(body_grid.pop(0))

    headers = _combine_header_rows(header_grid)
    width = len(headers)
    if body_grid:
        width = max(width, max(len(row) for row in body_grid))

    headers.extend([""] * (width - len(headers)))

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * width) + " |",
    ]
    for row in body_grid:
        row = row + [""] * (width - len(row))
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def _walk_element(element, blocks):
    tag = element.tag.lower() if isinstance(element.tag, str) else ""

    if tag == "p":
        text = _clean_inline(element.text_content())
        if text:
            # 部分老周报的表格标题写在 p 标签里，这里统一成小标题
            if re.match(r"^表\s*\d+(?!\d)", text) and not re.match(
                r"^表\s*\d+\s*(所示|中|显示|列出|可知)", text
            ):
                blocks.append(f"#### {text}")
            else:
                blocks.append(text)
        return

    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        text = _clean_inline(element.text_content())
        if text:
            level = {"h1": 3, "h2": 3, "h3": 4, "h4": 4, "h5": 5, "h6": 6}[tag]
            blocks.append("#" * level + " " + text)
        return

    if tag == "table":
        table_md = _table_to_markdown(element)
        if table_md:
            blocks.append(table_md)
        return

    if tag in ("ul", "ol"):
        lines = []
        items = element.xpath("./li")
        for index, item in enumerate(items, 1):
            text = _clean_inline(item.text_content())
            if text:
                prefix = f"{index}." if tag == "ol" else "-"
                lines.append(f"{prefix} {text}")
        if lines:
            blocks.append("\n".join(lines))
        return

    if tag == "img":
        src = element.get("src")
        if src:
            src = urljoin("https://ncpscxx.moa.gov.cn/", src)
            alt = _clean_inline(element.get("alt") or "图片")
            blocks.append(f"![{alt}]({src})")
        return

    if tag == "br":
        return

    children = [child for child in element if isinstance(child.tag, str)]
    if children:
        for child in children:
            _walk_element(child, blocks)
    else:
        text = _clean_inline(element.text_content())
        if text:
            blocks.append(text)


def html_to_markdown(fragment):
    if not fragment:
        return ""

    if "<" not in fragment:
        return _clean_inline(fragment)

    try:
        root = html.fromstring(f"<div>{fragment}</div>")
    except Exception:
        return _clean_inline(re.sub(r"<[^>]+>", "", fragment))

    blocks = []
    for child in root:
        _walk_element(child, blocks)
    return "\n\n".join(block for block in blocks if block.strip()).strip()


# ---------------------------------------------------------------------------
# 周报格式化
# ---------------------------------------------------------------------------

def format_period(weekly_date):
    """20261009-20261015 -> 2026年10月09日—2026年10月15日"""
    if not weekly_date or "-" not in weekly_date:
        return ""
    try:
        start_text, end_text = weekly_date.split("-", 1)
        start = datetime.strptime(start_text, "%Y%m%d")
        end = datetime.strptime(end_text, "%Y%m%d")
        return f"{start.strftime('%Y年%m月%d日')}—{end.strftime('%Y年%m月%d日')}"
    except ValueError:
        return weekly_date


def build_report_block(record):
    year = (record.get("year") or "").strip()
    weekly_num = (record.get("weeklyNum") or "").strip()

    if year and weekly_num:
        title = f"{year}年第{weekly_num}周-农产品批发市场价格周报"
    else:
        title = f"农产品批发市场价格周报（{record.get('weeklyDate') or '未知周期'}）"

    lines = [f"## {title}", ""]

    meta_lines = []
    period = format_period(record.get("weeklyDate") or "")
    if period:
        meta_lines.append(f"- 报告周期：{period}")

    source = _clean_inline(record.get("source") or "")
    if source:
        meta_lines.append(f"- 来源：{source}")

    published = publish_date(record)
    if published:
        meta_lines.append(f"- 发布时间：{published}")

    if meta_lines:
        lines.extend(meta_lines)
        lines.append("")

    content_md = html_to_markdown(record.get("content") or "")
    lines.append(content_md or "暂无数据")

    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# 状态与输出
# ---------------------------------------------------------------------------

def load_state():
    if not STATE_FILE.exists():
        return {"fetched_ids": []}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as handle:
            raw = handle.read().strip()
        if not raw:
            return {"fetched_ids": []}
        state = json.loads(raw)
        if isinstance(state, list):
            state = {"fetched_ids": state}
        if not isinstance(state, dict):
            raise ValueError("状态文件格式不正确")
        state.setdefault("fetched_ids", [])
        return state
    except Exception as exc:
        raise RuntimeError(
            f"读取状态文件失败：{exc}。为避免重复追加，请先运行 --full 重建。"
        ) from exc


def save_state(fetched_ids):
    state = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fetched_ids": sorted(fetched_ids),
    }
    with STATE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)
    log(f"状态文件已更新：{STATE_FILE.name}（{len(fetched_ids)} 条）")


def write_full(blocks):
    text = f"# {DOC_TITLE}\n"
    if blocks:
        text += "\n" + BLOCK_SEPARATOR.join(blocks) + "\n"
    OUTPUT_FILE.write_text(text, encoding="utf-8")
    log(f"已写入 {OUTPUT_FILE.name}：{len(blocks)} 期")


def prepend_blocks(blocks):
    if OUTPUT_FILE.exists():
        existing = OUTPUT_FILE.read_text(encoding="utf-8").strip()
    else:
        existing = ""

    header = f"# {DOC_TITLE}"
    if existing.startswith(header):
        rest = existing[len(header):].strip()
    else:
        rest = existing

    parts = [header, BLOCK_SEPARATOR.join(blocks)]
    if rest:
        parts.append(rest)

    text = BLOCK_SEPARATOR.join(parts) + "\n"
    OUTPUT_FILE.write_text(text, encoding="utf-8")
    log(f"已插入到 {OUTPUT_FILE.name} 顶部：{len(blocks)} 期")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="抓取农产品批发市场价格周报")
    parser.add_argument("--full", action="store_true", help="全量抓取并重建 md")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="兼容旧用法：抓取最新一页（默认行为）",
    )
    args = parser.parse_args()

    mode = "full" if args.full else "latest"

    log(f"运行模式：{'全量' if mode == 'full' else '增量（最新一页）'}")

    if mode == "latest" and not OUTPUT_FILE.exists():
        log("未找到已有 md 文件，请先运行 --full 进行首次全量抓取。")
        raise SystemExit(1)

    session = requests.Session()
    session.trust_env = False

    if mode == "full":
        records = fetch_all_records(session)
        records.sort(key=sort_key, reverse=True)
        log(f"待写入 {len(records)} 期")

        blocks = [build_report_block(record) for record in records]
        write_full(blocks)
        save_state({record["id"] for record in records if record.get("id")})
        return

    records = fetch_latest_records(session)
    try:
        state = load_state()
    except RuntimeError as exc:
        log(str(exc))
        raise SystemExit(1)
    fetched_ids = set(state.get("fetched_ids") or [])

    if not fetched_ids and OUTPUT_FILE.exists():
        existing_text = OUTPUT_FILE.read_text(encoding="utf-8")
        if re.search(r"(?m)^## ", existing_text):
            log("状态文件为空但 md 已有内容，为避免重复插入，请先运行 --full 重建。")
            raise SystemExit(1)

    new_records = [
        record
        for record in records
        if record.get("id") and record["id"] not in fetched_ids
    ]
    new_records.sort(key=sort_key, reverse=True)

    if not new_records:
        log("没有新的周报，md 文件不变")
        return

    blocks = [build_report_block(record) for record in new_records]
    prepend_blocks(blocks)
    fetched_ids.update(record["id"] for record in new_records)
    save_state(fetched_ids)


if __name__ == "__main__":
    main()
