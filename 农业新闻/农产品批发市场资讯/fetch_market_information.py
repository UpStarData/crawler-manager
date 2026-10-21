# -*- coding: utf-8 -*-
"""
农产品批发市场资讯抓取脚本(输出 Markdown + 图片)

数据来源: 农业农村部"全国农产品批发市场价格信息系统"资讯栏目
  https://pfsc.agri.cn/price_portal/web/portal-price-information/selectListByPage
  https://pfsc.agri.cn/price_portal/web/portal-price-information/detail

规则:
1. 首次运行抓取 2024-01-01 及以后的全部资讯; 之后默认增量, 只抓上次最新发布日期
   (回看 3 天, 防止同一天晚些时候补发的资讯漏掉)之后的数据;
2. 每篇资讯存一个 md 文件, 文件名取列表里的 title(仅替换 Windows 不允许的字符);
3. 图片下载到该来源目录下的 images/<md文件名>/ 中, md 里用相对路径引用;
4. 请求先走代理, 每次失败重试, 共 3 次; 3 次都失败再直连, 同样 3 次;
5. 每篇之间随机休眠 1.5~2.5 秒, 列表翻页休眠 2~4 秒, 不做不间断抓取;
6. 支持断点续跑: 已存在、资讯 ID 相同且图片齐全的 md 直接跳过。

用法:
    python fetch_market_information.py                  # 增量: 抓上次最新发布日期之后的数据
    python fetch_market_information.py --full           # 全量扫描 2024 年至今(已存在的仍跳过)
    python fetch_market_information.py --reset          # 清空输出目录后全量重抓
    python fetch_market_information.py --limit 5        # 只抓前 5 篇(调试)
    python fetch_market_information.py --start 2025-01-01

输出:
    md/<来源>/<列表 title>.md
    md/<来源>/images/<列表 title>/img_01.png ...
    md/fetch_state.json                                 # 上次抓取时间/最新发布日期, 用于增量
"""
import argparse
import base64
import json
import os
import random
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urljoin

import requests
from lxml import html as lxml_html

# ---------------- 可调配置 ----------------
BASE_URL = "https://pfsc.agri.cn"
LIST_URL = BASE_URL + "/price_portal/web/portal-price-information/selectListByPage"
DETAIL_URL = BASE_URL + "/price_portal/web/portal-price-information/detail"
ARTICLE_PAGE = BASE_URL + "/#/newsInformationDetail?id={id}"

DEFAULT_START_DATE = "2024-01-01"
STATE_FILENAME = "fetch_state.json"
INCREMENTAL_LOOKBACK_DAYS = 3   # 增量抓取时从上次最新发布日期往前多回看几天
PAGE_SIZE = 100
MAX_RETRIES = 3                  # 代理、直连各自最多尝试次数
REQUEST_TIMEOUT = 120            # 详情含 base64 图片, 体积可达数 MB
RETRY_SLEEP = 2                  # 重试前休眠(秒)
PAGE_SLEEP_RANGE = (2, 4)        # 列表翻页休眠
DETAIL_SLEEP_RANGE = (1.5, 2.5)  # 相邻两篇资讯之间的休眠
PROGRESS_EVERY = 20              # 每处理多少篇打印一次进度

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "md"

PROXY_URL = os.getenv("PROXY_URL", "")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": BASE_URL,
    "Referer": BASE_URL + "/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0"),
    "sec-ch-ua": "\"Microsoft Edge\";v=\"155\", \"Chromium\";v=\"155\", \"Not(A:Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
}

HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
BLOCK_TAGS = HEADING_TAGS | {"p", "div", "section", "article", "li", "ul", "ol", "table", "tr", "td", "th"}
SKIP_TAGS = {"script", "style", "noscript", "template"}
CAPTION_RE = re.compile(
    r"^\s*(?:图|表|图表|附图|附表)\s*[0-9一二三四五六七八九十]"
    r"|^\s*(?:注|说明|数据来源|资料来源)\s*[:：]"
)


# ---------------- 网络请求 ----------------
def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.trust_env = False  # 忽略系统代理, 只使用下面显式指定的代理
    return session


def _request_with_fallback(session, method, url, **kwargs):
    """先代理重试 3 次, 仍失败再直连重试 3 次。"""
    last_error = None
    for label, extra in (("代理", {"proxies": PROXIES}), ("直连", {})):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.request(method, url, timeout=REQUEST_TIMEOUT, **extra, **kwargs)
                resp.raise_for_status()
                return resp
            except Exception as exc:
                last_error = exc
                print(f"    {label}第{attempt}/{MAX_RETRIES}次失败: {str(exc)[:140]}", flush=True)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)
        if label == "代理":
            print("    代理 3 次均失败, 切换直连", flush=True)
    raise RuntimeError(f"代理与直连共 {MAX_RETRIES * 2} 次请求均失败: {last_error}")


def api_post(session, url, payload=None, params=None):
    resp = _request_with_fallback(session, "POST", url, json=payload, params=params)
    try:
        result = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"接口返回的不是 JSON(可能被拦截): {resp.text[:200]}") from exc
    if result.get("code") != 0:
        raise RuntimeError(f"接口返回异常 code={result.get('code')} msg={result.get('msg')}")
    data = result.get("data")
    if data is None:
        raise RuntimeError("接口未返回 data")
    return data


def fetch_list_page(session, page_no, start_time, end_time):
    payload = {
        "pageSize": PAGE_SIZE,
        "currentPage": page_no,
        "queryStartTime": start_time,
        "queryEndTime": end_time,
    }
    return api_post(session, LIST_URL, payload=payload)


def fetch_all_records(session, start_time, end_time, limit=None):
    """按时间范围抓完列表; 返回接口记录(最新在前)。"""
    records, seen = [], set()
    page_no, total_pages = 1, None
    while True:
        data = fetch_list_page(session, page_no, start_time, end_time)
        if total_pages is None:
            total_pages = data.get("pages") or 1
            print(f"列表共 {data.get('total', 0)} 条, 每页 {data.get('size') or PAGE_SIZE} 条, "
                  f"合计 {total_pages} 页", flush=True)
        for rec in data.get("records") or []:
            rid = rec.get("id")
            if rid and rid not in seen:
                seen.add(rid)
                records.append(rec)
                if limit and len(records) >= limit:
                    return records
        print(f"  列表第 {page_no}/{total_pages} 页完成, 累计 {len(records)} 条", flush=True)
        page_no += 1
        if page_no > total_pages:
            break
        time.sleep(random.uniform(*PAGE_SLEEP_RANGE))
    return records


# ---------------- HTML 解析 ----------------
@dataclass
class Block:
    kind: str                   # heading / paragraph / caption / image / table
    text: str = ""
    runs: list = field(default_factory=list)   # [(text, bold), ...]
    level: int = 0
    image_src: str = ""
    image_alt: str = ""
    image_rel: str = ""         # md 里的相对路径, 保存成功后填入
    rows: list = field(default_factory=list)


def _tag(el):
    return el.tag.lower() if isinstance(el.tag, str) else ""


def _clean_space(text):
    text = (text or "").replace("\xa0", " ").replace("\u3000", " ").replace("\t", " ")
    return re.sub(r"[ ]+", " ", text)


def _is_bold(el):
    if _tag(el) in ("strong", "b"):
        return True
    style = (el.get("style") or "").replace(" ", "").lower()
    return any(f"font-weight:{w}" in style for w in ("bold", "700", "800", "900"))


def _collect_flow(node, bold=False):
    """按文档顺序收集行内内容: ('text', 文本, 加粗) / ('img', 元素, 加粗)。"""
    flow = []

    def walk(el, inherited_bold):
        if el.text:
            flow.append(("text", _clean_space(el.text), inherited_bold))
        for child in el:
            tag = _tag(child)
            if tag in SKIP_TAGS:
                pass
            elif tag == "img":
                flow.append(("img", child, inherited_bold))
            elif tag == "br":
                flow.append(("text", " ", inherited_bold))
            else:
                walk(child, inherited_bold or _is_bold(child))
            if child.tail:
                flow.append(("text", _clean_space(child.tail), inherited_bold))

    walk(node, bold)
    return flow


def _normalize_runs(runs):
    merged = []
    for text, bold in runs:
        if not text:
            continue
        if merged and merged[-1][1] == bold:
            merged[-1] = (merged[-1][0] + text, bold)
        else:
            merged.append((text, bold))
    if not merged:
        return []
    merged[0] = (merged[0][0].lstrip(), merged[0][1])
    merged[-1] = (merged[-1][0].rstrip(), merged[-1][1])
    return [(t, b) for t, b in merged if t]


def _runs_text(runs):
    return "".join(t for t, _ in runs).strip()


def _flow_to_runs(flow):
    return _normalize_runs([(value, bold) for kind, value, bold in flow if kind == "text"])


def _emit_flow(flow, blocks):
    buffer = []

    def flush():
        runs = _normalize_runs(buffer)
        buffer.clear()
        text = _runs_text(runs)
        if text:
            blocks.append(Block("paragraph", text=text, runs=runs))

    for kind, value, bold in flow:
        if kind == "text":
            buffer.append((value, bold))
        elif kind == "img":
            flush()
            src = (value.get("src") or "").strip()
            if src:
                blocks.append(Block("image", image_src=src,
                                    image_alt=(value.get("alt") or "").strip()))
    flush()


def _parse_table(el, blocks):
    rows = []
    for tr in el.iter():
        if _tag(tr) != "tr":
            continue
        cells = []
        for cell in tr:
            if _tag(cell) in ("td", "th"):
                cells.append(_runs_text(_flow_to_runs(_collect_flow(cell))))
        if cells:
            rows.append(cells)
    if rows:
        blocks.append(Block("table", rows=rows))


def _walk_block(el, blocks):
    tag = _tag(el)
    if tag in SKIP_TAGS:
        return
    if tag in HEADING_TAGS:
        runs = _flow_to_runs(_collect_flow(el))
        text = _runs_text(runs)
        if text:
            blocks.append(Block("heading", text=text, runs=runs, level=int(tag[1])))
        return
    if tag == "table":
        _parse_table(el, blocks)
        return
    if tag in ("ul", "ol"):
        for child in el:
            if _tag(child) == "li":
                _walk_block(child, blocks)
        return

    has_block_child = any(_tag(child) in BLOCK_TAGS for child in el)
    if has_block_child:
        for child in el:
            if _tag(child) in BLOCK_TAGS:
                _walk_block(child, blocks)
            else:
                _emit_flow(_collect_flow(child, _is_bold(child)), blocks)
        return
    _emit_flow(_collect_flow(el), blocks)


def parse_content_html(raw):
    """把详情 content(HTML 片段) 解析成 Block 列表。"""
    if not (raw or "").strip():
        return []
    root = lxml_html.fromstring("<div>" + raw + "</div>")
    blocks = []
    for child in root:
        _walk_block(child, blocks)

    # 紧跟在图片后、形如“图1：……”的短段落作为图注, 同时作为图片 alt
    for i, block in enumerate(blocks):
        if (block.kind == "paragraph" and i > 0 and blocks[i - 1].kind == "image"
                and len(block.text) <= 80 and CAPTION_RE.match(block.text)):
            block.kind = "caption"
            if not blocks[i - 1].image_alt:
                blocks[i - 1].image_alt = block.text
    return blocks


# ---------------- 图片下载 ----------------
def _sniff_ext(data):
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if data.startswith(b"BM"):
        return "bmp"
    if data[:4] in (b"II*\x00", b"MM\x00*"):
        return "tiff"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    return ""


def _guess_ext(content_type="", data=b"", url=""):
    ext = _sniff_ext(data)
    if ext:
        return ext
    mime = (content_type or "").split(";")[0].strip().lower()
    ext = mime.split("/")[-1]
    ext = {"jpeg": "jpg", "svg+xml": "svg", "x-icon": "ico"}.get(ext, ext)
    if re.fullmatch(r"[a-z0-9]{2,5}", ext or ""):
        return ext
    suffix = Path(url.split("?")[0]).suffix.lstrip(".").lower()
    return suffix if re.fullmatch(r"[a-z0-9]{2,5}", suffix or "") else "png"


def load_image(session, image_src):
    """把 data URI 或远程图片读成 (bytes, ext); 失败返回 None。"""
    if not image_src:
        return None
    try:
        if image_src.startswith("data:"):
            header, _, payload = image_src.partition(",")
            mime = header[5:].split(";")[0].strip().lower()
            if ";base64" in header.lower():
                data = base64.b64decode(payload)
            else:
                data = unquote(payload).encode("utf-8")
            return data, _guess_ext(mime, data)
        url = urljoin(BASE_URL + "/", image_src)
        resp = _request_with_fallback(session, "GET", url)
        data = resp.content
        return data, _guess_ext(resp.headers.get("Content-Type"), data, url)
    except Exception as exc:
        print(f"    图片下载失败({image_src[:50]}...): {str(exc)[:120]}", flush=True)
        return None


def save_images(session, blocks, image_dir, base):
    """保存本篇图片到 image_dir, 返回成功张数; 同时填充 block.image_rel。"""
    if image_dir.exists():
        shutil.rmtree(image_dir)
    image_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for block in blocks:
        if block.kind != "image":
            continue
        loaded = load_image(session, block.image_src)
        if not loaded:
            continue
        data, ext = loaded
        count += 1
        filename = f"img_{count:02d}.{ext}"
        (image_dir / filename).write_bytes(data)
        block.image_rel = f"images/{base}/{filename}"
    if count == 0:
        image_dir.rmdir()
        try:
            image_dir.parent.rmdir()
        except OSError:
            pass
    return count


def drop_broken_images(blocks):
    """去掉下载失败的图片块, 以及紧跟在它后面的图注。"""
    cleaned, last_image_failed = [], False
    for block in blocks:
        if block.kind == "image":
            last_image_failed = not block.image_rel
            if last_image_failed:
                continue
        elif block.kind == "caption" and last_image_failed:
            last_image_failed = False
            continue
        else:
            last_image_failed = False
        cleaned.append(block)
    return cleaned


# ---------------- Markdown ----------------
def _runs_to_md(runs):
    out = []
    for text, bold in runs:
        if bold and text.strip():
            lead = text[:len(text) - len(text.lstrip())]
            trail = text[len(text.rstrip()):]
            out.append(f"{lead}**{text.strip()}**{trail}")
        else:
            out.append(text)
    return re.sub(r"[ ]{2,}", " ", "".join(out)).strip()


def _md_cell(text):
    return (text or "").replace("|", "\\|").replace("\n", " ").strip()


def _md_table(rows):
    if not rows:
        return []
    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]
    lines = ["| " + " | ".join(_md_cell(c) for c in rows[0]) + " |",
             "|" + "|".join([" --- "] * width) + "|"]
    for row in rows[1:]:
        lines.append("| " + " | ".join(_md_cell(c) for c in row) + " |")
    return lines


def _md_alt(text):
    return (text or "图片").replace("]", "\\]").replace("[", "\\[").replace("\n", " ")


def _md_url(url):
    """含空格/括号的路径用尖括号包住, 否则 Markdown 会把链接截断。"""
    return f"<{url}>" if re.search(r"[\s()<>]", url) else url


def build_markdown(article, blocks):
    lines = [f"# {article.title}", ""]
    lines.append(f"> 来源：{article.source}　发布时间：{article.publish_date}")
    lines.append(">")
    lines.append(f"> 资讯ID：{article.id}")
    lines.append(">")
    lines.append(f"> 原文：{article.url}")
    lines.append("")
    for block in blocks:
        if block.kind == "heading":
            level = min(block.level + 1, 6)
            lines += ["#" * level + " " + block.text, ""]
        elif block.kind == "paragraph":
            lines += [_runs_to_md(block.runs) if block.runs else block.text, ""]
        elif block.kind == "caption":
            lines += [f"*{block.text}*", ""]
        elif block.kind == "image" and block.image_rel:
            lines += [f"![{_md_alt(block.image_alt)}]({_md_url(block.image_rel)})", ""]
        elif block.kind == "table":
            lines += _md_table(block.rows) + [""]
    return "\n".join(lines).rstrip() + "\n"


# ---------------- 资讯记录与文件命名 ----------------
@dataclass
class Article:
    id: str
    title: str
    source: str
    publish_date: str
    url: str
    content_html: str = ""


def _detail_to_article(detail):
    article_id = detail.get("id") or ""
    title = re.sub(r"\s+", " ", detail.get("title") or "").strip() or f"资讯_{article_id[:8]}"
    source = re.sub(r"\s+", " ", detail.get("source") or "").strip() or "未知来源"
    publish_date = (detail.get("publishDate") or "")[:10] or "未知日期"
    content = detail.get("content") or detail.get("contentstr") or ""
    return Article(article_id, title, source, publish_date,
                   ARTICLE_PAGE.format(id=article_id), content)


INVALID_FILENAME_MAP = {
    "<": "＜", ">": "＞", ":": "：", '"': "＂", "/": "／",
    "\\": "＼", "|": "｜", "?": "？", "*": "＊",
}


def safe_filename(text, max_len=150):
    """只替换 Windows 文件名非法字符, 尽量保持列表 title 原样。"""
    text = (text or "").replace("\r", " ").replace("\n", " ").strip()
    for bad, good in INVALID_FILENAME_MAP.items():
        text = text.replace(bad, good)
    text = re.sub(r"[\x00-\x1f]", "", text)
    text = re.sub(r"[ ]{2,}", " ", text).strip(" .")
    return (text[:max_len].rstrip(" .") or "未命名")


def md_is_complete(path, article_id):
    """md 存在、ID 匹配且引用的图片都在, 视为已完成。"""
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return False
    if f"资讯ID：{article_id}" not in text[:800]:
        return False
    for rel in re.findall(r"!\[(?:\\.|[^\]\\])*\]\((<[^>]+>|[^)]+)\)", text):
        rel = rel.strip().strip("<>")
        if rel.startswith(("data:", "http://", "https://")):
            continue
        if not (path.parent / rel).exists():
            return False
    return True


def resolve_output_path(folder, title, article_id):
    """按 title 生成 md 路径; 已完成返回 None, 未完成返回路径以便重写。"""
    base = safe_filename(title)
    for index in range(100):
        suffix = "" if index == 0 else f" ({index + 1})"
        path = folder / f"{base}{suffix}.md"
        if not path.exists():
            return path
        try:
            head = path.read_text(encoding="utf-8")[:800]
        except Exception:
            head = ""
        if f"资讯ID：{article_id}" in head:
            return None if md_is_complete(path, article_id) else path
    raise RuntimeError(f"同名文件过多: {base}")


def build_id_index(output_root):
    """扫描已有 md, 建立 资讯ID -> md 路径 的索引, 防止标题变更后重复保存。"""
    index = {}
    if not output_root.exists():
        return index
    for path in output_root.rglob("*.md"):
        try:
            with path.open(encoding="utf-8") as f:
                head = f.read(800)
        except Exception:
            continue
        m = re.search(r"资讯ID：(\w+)", head)
        if m:
            index[m.group(1)] = path
    return index


def process_article(session, record, output_root, id_index):
    """抓取一篇资讯, 下载图片并写入 md; 返回 'ok' / 'skip'。"""
    existing_path = id_index.get(record["id"])
    if existing_path is not None:
        # 同一个 ID 已经存在: 完整就直接跳过, 不完整则在原路径重写
        if md_is_complete(existing_path, record["id"]):
            return "skip"
        detail = api_post(session, DETAIL_URL, params={"id": record["id"]})
        article = _detail_to_article(detail)
        path = existing_path
        folder = existing_path.parent
    else:
        pre_folder = output_root / safe_filename((record.get("source") or "").strip() or "未知来源")
        pre_path = resolve_output_path(pre_folder, record.get("title") or record["id"], record["id"])
        if pre_path is None:
            return "skip"

        detail = api_post(session, DETAIL_URL, params={"id": record["id"]})
        article = _detail_to_article(detail)
        folder = output_root / safe_filename(article.source)
        folder.mkdir(parents=True, exist_ok=True)
        path = resolve_output_path(folder, article.title, article.id)
        if path is None:
            return "skip"

    folder.mkdir(parents=True, exist_ok=True)
    base = path.stem
    blocks = parse_content_html(article.content_html)
    image_dir = folder / "images" / base
    image_count = save_images(session, blocks, image_dir, base)
    blocks = drop_broken_images(blocks)

    md_text = build_markdown(article, blocks)
    tmp = path.with_name(path.stem + ".tmp.md")
    tmp.write_text(md_text, encoding="utf-8")
    tmp.replace(path)
    id_index[article.id] = path

    paragraphs = sum(1 for b in blocks if b.kind == "paragraph")
    print(f"    {paragraphs} 段 / {image_count} 图 / {len(md_text) / 1024:.0f}KB -> {path.name}",
          flush=True)
    return "ok"


def _format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def latest_publish_date_in_output(output_root):
    """扫描已有 md 的元数据, 返回最大的发布日期(YYYY-MM-DD)。"""
    latest = ""
    if not output_root.exists():
        return latest
    for path in output_root.rglob("*.md"):
        try:
            with path.open(encoding="utf-8") as f:
                head = f.read(1200)
        except Exception:
            continue
        m = re.search(r"发布时间：(\d{4}-\d{2}-\d{2})", head)
        if m and m.group(1) > latest:
            latest = m.group(1)
    return latest


def load_state(output_root):
    """读取增量状态; 没有状态文件时从已有 md 反推上次最新发布日期。"""
    state_path = output_root / STATE_FILENAME
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    if not state.get("last_publish_date"):
        latest = latest_publish_date_in_output(output_root)
        if latest:
            state["last_publish_date"] = latest
            state["bootstrapped_from_md"] = True
            print(f"未找到状态文件, 从已有 md 反推上次最新发布日期: {latest}", flush=True)
    return state, state_path


def save_state(state_path, state):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_name(state_path.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, state_path)


def resolve_start_date(args, state):
    """返回 (开始日期, 模式说明)。"""
    if args.start:
        return args.start, "手动指定开始日期"
    if args.reset or args.full:
        return DEFAULT_START_DATE, "全量"
    last_date = (state.get("last_publish_date") or "").strip()
    if not last_date:
        return DEFAULT_START_DATE, "首次全量"
    try:
        start = (datetime.strptime(last_date, "%Y-%m-%d").date()
                 - timedelta(days=INCREMENTAL_LOOKBACK_DAYS))
        return start.isoformat(), f"增量(上次最新 {last_date}, 回看 {INCREMENTAL_LOOKBACK_DAYS} 天)"
    except ValueError:
        print(f"状态文件里的日期格式异常: {last_date}, 改为全量", flush=True)
        return DEFAULT_START_DATE, "全量"


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="抓取农产品批发市场资讯并输出 Markdown")
    parser.add_argument("--start", default=None, help="手动指定开始日期(YYYY-MM-DD)")
    parser.add_argument("--end", default="", help="结束日期, 默认今天")
    parser.add_argument("--limit", type=int, help="最多处理多少篇(调试用)")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--full", action="store_true",
                        help="忽略状态文件, 从 2024-01-01 全量扫描(已存在的仍跳过)")
    parser.add_argument("--reset", action="store_true", help="清空输出目录后全量重抓")
    args = parser.parse_args()

    output_root = Path(args.output_dir)
    if args.reset and output_root.exists():
        print(f"清空输出目录: {output_root}", flush=True)
        shutil.rmtree(output_root, ignore_errors=True)

    state, state_path = load_state(output_root)
    id_index = build_id_index(output_root)
    if id_index:
        print(f"已索引 {len(id_index)} 篇已有资讯", flush=True)
    start_date, mode = resolve_start_date(args, state)
    if args.full and not args.start:
        mode = "全量(忽略状态文件)"
    end_date = args.end or time.strftime("%Y-%m-%d")
    start_time = f"{start_date} 00:00:00"
    end_time = f"{end_date} 23:59:59"

    session = make_session()
    print(f"模式: {mode}; 抓取范围: {start_time} ~ {end_time}", flush=True)
    records = fetch_all_records(session, start_time, end_time, limit=args.limit)
    if args.limit:
        records = records[:args.limit]
    # 接口的 queryStartTime 偶尔会带出边界前(发布时间在开始日期之前)的记录, 这里再按发布日期过滤
    records = [r for r in records
               if not (r.get("publishDate") or "")[:10]
               or (r.get("publishDate") or "")[:10] >= start_date]
    print(f"待处理 {len(records)} 篇, 输出目录: {output_root}", flush=True)

    ok = skipped = failed = 0
    covered = []  # 已写入/已确认存在的记录, 用于推进状态里的最新发布日期
    started = time.time()
    for i, record in enumerate(records, 1):
        print(f"[{i}/{len(records)}] {record.get('title') or record['id']}", flush=True)
        try:
            result = process_article(session, record, output_root, id_index)
            if result == "ok":
                ok += 1
            else:
                skipped += 1
            covered.append(record)
        except KeyboardInterrupt:
            print("\n手动中断, 已完成的文件保留", flush=True)
            break
        except Exception as exc:
            failed += 1
            print(f"    处理失败: {str(exc)[:200]}", flush=True)

        if i % PROGRESS_EVERY == 0 or i == len(records):
            elapsed = time.time() - started
            speed = elapsed / i
            eta = speed * (len(records) - i)
            print(f"--- 进度 {i}/{len(records)} ({i / len(records) * 100:.1f}%), "
                  f"成功 {ok}, 跳过 {skipped}, 失败 {failed}, "
                  f"已用 {_format_duration(elapsed)}, 预计剩余 {_format_duration(eta)} ---",
                  flush=True)

        if i < len(records):
            time.sleep(random.uniform(*DETAIL_SLEEP_RANGE))

    print(f"\n完成: 成功 {ok} 篇, 跳过 {skipped} 篇, 失败 {failed} 篇, "
          f"总耗时 {_format_duration(time.time() - started)}", flush=True)

    # 只根据已成功/已存在的记录推进日期; 失败条目下次会落在 3 天回看窗口内
    last_date = state.get("last_publish_date") or ""
    for record in covered:
        d = (record.get("publishDate") or "")[:10]
        if d > last_date:
            last_date = d
    total_md = sum(1 for _ in output_root.rglob("*.md")) if output_root.exists() else 0
    state.update({
        "last_publish_date": last_date or start_date,
        "last_run_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "last_start_date": start_date,
        "last_end_date": end_date,
        "last_mode": mode,
        "total_md": total_md,
    })
    save_state(state_path, state)
    print(f"状态已更新: {state_path} (上次最新发布日期: {state['last_publish_date']})", flush=True)
    if failed:
        print("有失败条目, 下次增量会回看 3 天重新扫描, 不会漏掉。", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
