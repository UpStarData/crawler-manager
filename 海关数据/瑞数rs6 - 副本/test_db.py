# -*- coding: utf-8 -*-
"""
海关统计数据查询平台自动查询（瑞数 RS6）+ 材料导入 Kafka 生产者（进出口分向、分月、多年份、多商品、翻页）

按「材料导入 Kafka 消息契约 specVersion 0.1」把查询到的海关统计逐月打包成材料消息投递
Kafka（默认 crawl.backend.material.create）。本脚本是采集侧（生产者）：
    - 进出口分开查询：每轮固定一个方向（iEType=1 进口 / 0 出口），入库带 imp_exp_type
    - 一条消息 = 一个月份 × 商品 × 方向 的聚合材料：content 顶层为年月/商品/iEType/币制，
      正文 records 数组为按 (贸易伙伴编码, 注册地编码) 排序的明细行（含 first/second 数量与单位、金额）
    - materialId 为 UUIDv7（每条消息唯一）
    - 发送前按 contentHash 判重：已发送哈希集合持久在 runtime/sent_content_hashes.json，
      跨运行有效；同月同商品同方向内容重复时不重发 Kafka
    - title 形如「2026年08月 其他鲜樱桃」；publishedAt 为该月 1 日（+08:00）；url 为当轮查询地址
    - capturedAt 带时区偏移（ISO 8601）；信封层不夹带任何未登记字段（消费端拒收未知字段）

相对 test_csv.py 的改动（test_csv.py 未动）：
    1. 查询结果不再写 CSV，逐月打包成契约消息投 Kafka；可选同时入库 MySQL（SINK_MODE 控制）
    2. 分组字段 = 商品编码(CODE_TS) × 贸易伙伴(ORIGIN_COUNTRY) × 收发货人注册地(TRADE_CO_PORT)
    3. 开启分月展示（monthFlag），行内带"月份"列 -> data_year_month
    4. 多年份 × 多商品 × 进出口方向逐轮查询（见 QUERY_YEARS / GOODS_CODES）
    5. 结果分页拉取（PAGE_SIZE 可改 20/50/100/200），整轮取完后按月聚合投递
    6. 三级重试：单请求重试 REQ_RETRIES 次 / 单页重试 PAGE_RETRIES 次（重试时换新列表页）
       / 整轮重试 MAX_RUN_ATTEMPTS 次；连续多轮失败自动中止（疑似代理失效）
    7. 轮间休眠 ROUND_INTERVAL 秒，对站点友好

用法:
    python test_db.py
    python test_db.py --goods 08092900,08081000 --years 2026:1:8 --sink kafka
    （后台管理页面以同样参数触发；不带参数时用文件顶部常量，原行为不变）

依赖:
    pip install curl_cffi loguru kafka-python
    （SINK_MODE 含 mysql 时额外需要 pymysql）

Kafka:
    连接与主题见顶部 KAFKA_* 配置（SASL 账号 app）。materialId 为 UUIDv7；发送前按
    contentHash 判重（集合持久在 runtime/sent_content_hashes.json），已发送过的内容不再投递。

数据库:
    SINK_MODE 含 mysql 时写入 customs_import_export_data（含 imp_exp_type 进出口类型），
    幂等 UPSERT 依赖唯一键 uk_business_unique(年月,方向,商品,贸易伙伴,注册地)。

退出码: 全部轮次成功 0，有失败 1
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse

from curl_cffi import requests as curl_requests
from curl_cffi.requests import impersonate as curl_impersonate
from loguru import logger

# ============================ 日志配置 ============================
logger.remove()
logger.add(
    sys.stderr,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
    level="INFO",
)

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"
BASE = "http://stats.customs.gov.cn"
HOME_URL = BASE + "/"
QUERY_PAGE = BASE + "/queryData/queryDataByWhere"
QUERY_LIST = BASE + "/queryData/queryDataList"
QUERY_API = BASE + "/queryData/getQueryDataListByWhere"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0"

# ============================ 查询计划配置 ============================
# 商品编码（逐个查询，共 10 种）
GOODS_CODES = [
    # "08106000",  # 鲜榴莲
    # "08052110",  # 焦柑
    # "08052190",  # 其他柑橘（包括小蜜橘及萨摩蜜柑橘）
    # "08052200",  # 克里曼丁橘（用户要求不跑）
    # "08052900",  # 韦尔金橘及类似的杂交柑橘
    # "08059000",  # 未列名柑桔属水果
    # "08081000",  # 鲜苹果
    # "08092100",  # 鲜欧洲酸樱桃
    # "08092900",  # 其他鲜樱桃
    "07096000",  # 鲜或冷藏的辣椒属及多香果属的果实（2022 已完成）
]
# 查询年份段列表 [(年, 起月, 止月), ...]，一次放开一段；同一年跨库边界时拆成多段
# 海关数据发布滞后约 2 个月：2026 年目前可取到 8 月，有新数据时更新这里
# 2025 年边界（用户实测）：1 月在历史库(2)，2-12 月在活跃库(1)
QUERY_YEARS = [
    # (2022, 1, 12),    # 22 年已完成（07096000 也已完成）
    # (2023, 1, 12),    # 23 年已完成（07096000）
    # (2024, 1, 12),    # 24 年已完成（07096000）
    # (2025, 1, 1),     # 25 年已完成（历史库，07096000）
    # (2025, 2, 12),    # 25 年已完成（活跃库，07096000）
    (2026, 1, 8),       # 放开 26 年（活跃库，数据发布到 8 月）
]
PAGE_SIZE = "200"  # 每页条数：20 / 50 / 100 / 200（数据多建议 200）
MONTH_FLAG = "1"  # 分月展示：1=开（若站点不生效，尝试改为 "true"）


# 库选择（2026-10-12 用户实测边界）：两库 state=3 接口返回 200 空响应，禁用；
#   2025年1月及更早 = 历史库(2)；2025年2月起 = 活跃库(1)
def year_state(year, month):
    if year <= 2024:
        return "2"
    if year == 2025:
        return "2" if month <= 1 else "1"
    return "1"


MAX_PAGES = 5000  # 单轮翻页安全上限（防止死循环）
MAX_RUN_ATTEMPTS = 3  # 整轮重试次数
REQ_RETRIES = 2  # 单请求就地重试次数
PAGE_RETRIES = 6  # 单页失败重试次数（隧道抖动大，POST 约一半概率被网关拦成 200 空响应）
PAGE_RETRY_BASE_SLEEP = 2  # 单页重试退避基数秒（第 n 次等 2+n 秒）
PAGE_INTERVAL = 1  # 翻页间隔秒数
ROUND_INTERVAL = 2  # 轮间休眠秒数
CONSECUTIVE_FAIL_ABORT = 5  # 连续失败 N 轮直接中止（疑似代理/环境失效）
FIREFOX = getattr(curl_impersonate, "DEFAULT_FIREFOX", "firefox135")

# 隧道代理（带认证）；整轮必须同一出口 IP，curl 会话复用连接天然满足
TUNNEL_HOST = os.getenv("PROXY_HOST", "t130.juliangip.cc")
TUNNEL_PORT = int(os.getenv("PROXY_PORT", "13781"))
TUNNEL_USERNAME = os.getenv("PROXY_USERNAME", "")
TUNNEL_PASSWORD = os.getenv("PROXY_PASSWORD", "")
PROXY_URL = os.getenv("PROXY_URL", "") or (
    f"http://{TUNNEL_USERNAME}:{TUNNEL_PASSWORD}@{TUNNEL_HOST}:{TUNNEL_PORT}"
    if TUNNEL_USERNAME and TUNNEL_PASSWORD else ""
)

# ============================ 输出目标（SINK_MODE）与 Kafka 配置 ============================
# kafka = 只投 Kafka（默认，生产者）；mysql = 只入库 MySQL；both = 两者都做
SINK_MODE = "both"

# 材料导入 Kafka（契约 specVersion 0.1，一条消息 = 一份材料）
KAFKA_BOOTSTRAP = ["192.168.1.154:9092"]
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"  # SASL_PLAINTEXT / SASL_SSL / PLAINTEXT
KAFKA_SASL_MECHANISM = "PLAIN"  # 常见 PLAIN；broker 用 SCRAM 时改 SCRAM-SHA-256
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "app")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_TOPIC = "crawl.backend.material.create"
KAFKA_ACKS = "all"  # 0 / 1 / all
KAFKA_SEND_TIMEOUT = 30  # 单条发送确认超时（秒）
KAFKA_RETRIES = 3  # 生产者内部重试（broker 抖动）

# 材料元数据（采集侧身份，写入每条消息）
MATERIAL_SOURCE_ID = "customs-trade-stats"  # 来源站点/渠道稳定标识
MATERIAL_SOURCE_KIND = "statistics"  # 来源类型（不落库，智能体直接取用）
MATERIAL_SPEC_VERSION = "0.1"
SENT_HASH_FILE = "sent_content_hashes.json"  # runtime/ 下：已发送 contentHash 集合（跨运行持久）

# ============================ MySQL 配置 ============================
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "ap_dev",
    "charset": "utf8mb4",
}
DB_TABLE = "customs_import_export_data"
# 入库字段顺序（与 INSERT 语句、records 元组顺序一致）
DB_COLUMNS = [
    "data_year_month",  # 数据年月 202501
    "imp_exp_type",  # 进出口类型: 1=进口 0=出口（每轮固定，不来自表头）
    "goods_code",  # 商品编码
    "goods_name",  # 商品名称
    "trade_partner_code",  # 贸易伙伴编码
    "trade_partner_name",  # 贸易伙伴名称
    "register_area_code",  # 注册地编码
    "register_area_name",  # 注册地名称
    "first_qty",  # 第一数量
    "first_unit",  # 第一计量单位
    "second_qty",  # 第二数量
    "second_unit",  # 第二计量单位
    "rmb_amount",  # 人民币金额
]
NUMERIC_COLS = {"first_qty", "second_qty", "rmb_amount"}
NULLABLE_COLS = {"first_unit", "second_unit"}
DB_RETRIES = 3  # 入库失败重试次数
DB_RETRY_SLEEP = 5  # 入库重试间隔秒数

# 表头 value 属性 -> 表字段（value 取自列表页 thead 的 tr-line div，与站点字段代码对应）
HEADER_FIELD_MAP = {
    "CODE_TS": "goods_code",
    "HS_NAME_CN": "goods_name",
    "ORIGIN_COUNTRY": "trade_partner_code",
    "COUNTRY_NAME_CN": "trade_partner_name",
    "TRADE_CO_PORT": "register_area_code",
    "TRADE_CO_PORT_CN": "register_area_name",
    "QTY_1": "first_qty",
    "QTY_2": "second_qty",
    "RMB_PRICE": "rmb_amount",
}
# 无 value 属性的表头按文字匹配
TEXT_FIELD_MAP = {
    "第一计量单位": "first_unit",
    "第二计量单位": "second_unit",
}


class RetryableError(Exception):
    """网络/代理/瞬时会话问题：整轮重试可恢复"""


# ============================ 基础工具 ============================
def write_runtime(name, text):
    RUNTIME.mkdir(exist_ok=True)
    path = RUNTIME / name
    path.write_text(text, encoding="utf-8")
    return path


def rel(path):
    return str(path.relative_to(ROOT)).replace("\\", "/")


COOKIE_ORDER = ["AV7KYchI7HHaS", "SF_cookie_251", "__jsluid_h", "AV7KYchI7HHaT", "JSESSIONID"]


def cookie_header_from_map(mapping):
    parts, seen = [], set()
    for name in COOKIE_ORDER:
        if mapping.get(name):
            parts.append(f"{name}={mapping[name]}")
            seen.add(name)
    for name, value in mapping.items():
        if name not in seen and value and not name.startswith("enable_"):
            parts.append(f"{name}={value}")
    return "; ".join(parts)


def cookie_header(session):
    return cookie_header_from_map(session.cookies.get_dict())


def form_body(form):
    """生成 POST body（逗号->%2C、空格->+，与站点 jQuery 序列化一致）"""
    from urllib.parse import urlencode

    return urlencode(form)


def build_form(year, code, m1, m2, page, imp_exp):
    """按轮次参数拼查询表单：分组=商品编码×贸易伙伴×收发货人注册地，分月展示。
    imp_exp: "1"=进口 "0"=出口（进出口分开查询，不用 10 合计）"""
    return {
        "pageSize": PAGE_SIZE,
        "pageNum": str(page),
        "iEType": str(imp_exp),  # 1=进口 0=出口
        "currencyType": "rmb",  # rmb / usd
        "year": str(year),
        "startMonth": str(m1),
        "endMonth": str(m2),
        "monthFlag": MONTH_FLAG,  # 分月展示
        "unitFlag": "true",
        "unitFlag1": "true",
        "codeLength": "8",
        "outerField1": "CODE_TS",
        "outerValue1": code,  # 商品编码
        "outerField2": "ORIGIN_COUNTRY",
        "outerValue2": "",  # 全部贸易伙伴
        "outerField3": "TRADE_CO_PORT",
        "outerValue3": "",  # 全部收发货人注册地
        "outerField4": "",
        "outerValue4": "",
        "orderType": "CODE ASC DEFAULT",
        "selectTableState": year_state(year, m1),
        "currentStartTime": "202502",  # 平台下发值，勿改
    }


def captcha_params(year, code, m1, m2, imp_exp):
    """滑块页参数 = 查询表单 + 平台当前数据窗口参数（沿用原文件，与查询范围无关）"""
    params = build_form(year, code, m1, m2, 1, imp_exp)
    params.update({
        "currentRecentTime": "202607",
        "currentDateBySource": "202608",
        "currentEndTime": "202608",
        "unitFlag": "false",
        "unitFlag1": "false",
    })
    return params


def call_node(payload):
    proc = subprocess.run(
        ["node", str(ROOT / "code.js")],
        cwd=ROOT,
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or "") + "\n" + (proc.stdout or "")
        raise RuntimeError(err.strip()[:4000])
    return json.loads(proc.stdout)


def http(session, method, url, *, timeout=30, **kw):
    """带就地重试的请求：超时/断连重试 REQ_RETRIES 次，仍失败抛 RetryableError"""
    short = url.split("?")[0].replace(BASE, "")
    last = None
    for i in range(REQ_RETRIES):
        try:
            return getattr(session, method)(url, timeout=timeout, **kw)
        except Exception as exc:
            last = exc
            if i < REQ_RETRIES - 1:
                logger.warning(f"{method.upper()} {short} 第{i + 1}次请求异常[{type(exc).__name__}]，1秒后重试")
                time.sleep(1)
            else:
                logger.warning(f"{method.upper()} {short} 连续{REQ_RETRIES}次异常[{type(last).__name__}]")
    raise RetryableError(f"{method.upper()} {short}: {type(last).__name__}") from last


def extract_static_url(html, base_url):
    match = re.search(
        r"""<script[^>]+src=["']([^"']+)["'][^>]*r\s*=\s*['"]m['"]""",
        html,
        re.I,
    )
    if not match:
        match = re.search(r"""<script[^>]+src=["']([^"']*Qy6JDI4LYvbD[^"']+)["']""", html, re.I)
    return urljoin(base_url, match.group(1)) if match else ""


def fetch_static_js(session, html, base_url):
    """
    拉取瑞数静态 JS，按文件名缓存在 runtime/。
    瑞数脚本版本在文件名里（如 YWB5qmnxo45M.2437991.js），命中缓存则跳过下载。
    返回 (本地路径, 资源URL)；失败返回 (None, url)。
    """
    static_url = extract_static_url(html, base_url)
    if not static_url:
        return None, ""
    name = static_url.rsplit("/", 1)[-1]
    path = RUNTIME / name
    if path.exists() and path.stat().st_size > 10000:
        logger.info(f"瑞数脚本 {name}（缓存命中 {path.stat().st_size // 1024}KB，免下载）")
        return path, static_url
    resp = http(
        session, "get", static_url, timeout=30,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": base_url,
        },
    )
    if resp.status_code != 200 or not resp.text:
        return None, static_url
    RUNTIME.mkdir(exist_ok=True)
    path.write_text(resp.text, encoding="utf-8")
    logger.info(f"瑞数脚本 {name}（下载 {len(resp.text) // 1024}KB，已缓存）")
    return path, static_url


def apply_node_cookies(session, info):
    """只把 Node 生成的 cookie 写入 jar（不清理既有项）；跳过 enable_ 和空值。
    注意：不能整包替换——server 下发的 S/__jsluid_h 必须原样保留。"""
    cookies = (info.get("rs6") or {}).get("cookies") or info.get("cookies") or {}
    host = urlparse(BASE).hostname
    for name, value in cookies.items():
        if value in (None, "") or name.startswith("enable_"):
            continue
        session.cookies.set(name, str(value), domain=host, path="/")


def nav_headers(referer=None):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "Priority": "u=0, i",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def make_session():
    """隧道代理固定配置；curl 会话内连接复用，保证整轮同一出口 IP"""
    session = curl_requests.Session(impersonate=FIREFOX)
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session


# ============================ Node 瑞数 ============================
def node_rs6(session, html_path, page_url, static_path, static_url, form,
             skip_xhr=False, stop_on_first_cookie=False):
    payload = {
        "action": "get_rs6",
        "userAgent": USER_AGENT,
        "pageUrl": page_url,
        "pageHtmlPath": rel(html_path),
        "requestCookie": cookie_header(session),
        "cookies": session.cookies.get_dict(),
        "skipXhrBoundary": skip_xhr,
        "stopOnFirstCookie": stop_on_first_cookie,
        "targetPath": "/queryData/getQueryDataListByWhere",
        "method": "POST",
        "form": form,
        "scriptTimeoutMs": 15000,
        "body": form_body(form),
    }
    if static_path:
        payload["staticSourcePath"] = rel(static_path)
    if static_url:
        payload["staticScriptSrc"] = static_url
    return call_node(payload)


def node_stage_errors(info):
    return [f"{s.get('name')}: {s.get('error')}" for s in (info.get("stages") or []) if not s.get("ok")]


def parse_result(html):
    """返回 (总数, 纯文本)——总数来自响应里的"共查询到 N 条数据"（该轮全部分页的总量）"""
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
    m = re.search(r"共查询到\s*(\d+)\s*条数据", text)
    rows = int(m.group(1)) if m else -1
    return rows, text


# ============================ 响应解析 ============================
def _clean_cell(raw):
    """单元格清洗：去标签、实体反转义、压缩空白"""
    text = unescape(re.sub(r"<[^>]+>", "", raw or ""))
    return re.sub(r"\s+", " ", text).strip()


def parse_result_rows(html):
    """从接口返回 HTML 提取数据行：每个 <tr> 一行，取其中全部 th-line 单元格文本。
    站点会把长单元格文本截断显示（以 … 结尾），完整值在其 title 属性里——此时取 title。"""
    rows = []
    for tr in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", html or "", re.S | re.I):
        cells = []
        for dm in re.finditer(
                r"""<div\b[^>]*class=["'][^"']*\bth-line\b[^"']*["'][^>]*>(.*?)</div>""",
                tr.group(1),
                re.S | re.I,
        ):
            text = _clean_cell(dm.group(1))
            if text.endswith("..."):
                tm = re.search(r"""title\s*=\s*["']([^"']+)["']""", dm.group(0))
                if tm and tm.group(1).strip():
                    text = _clean_cell(tm.group(1))
            cells.append(text)
        if any(cells):
            rows.append(cells)
    return rows


def parse_thead_columns(html):
    """从列表页 HTML 的 <thead> 提取列定义 [(显示文本, value属性或None), ...]，
    顺序与接口返回行单元格顺序一致"""
    m = re.search(r"<thead\b[^>]*>(.*?)</thead>", html or "", re.S | re.I)
    if not m:
        return []
    cols = []
    for dm in re.finditer(
            r"""<div\b[^>]*class=["'][^"']*\btr-line\b[^"']*["'][^>]*>(.*?)</div>""",
            m.group(1),
            re.S | re.I,
    ):
        tag = dm.group(0)
        vm = re.search(r"""value\s*=\s*["']([^"']+)["']""", tag)
        cols.append((_clean_cell(dm.group(1)), vm.group(1) if vm else None))
    return cols


def build_column_map(headers_cols):
    """表头列 -> {db字段: 单元格下标}。
    value 属性优先（HEADER_FIELD_MAP），无 value 的按文字匹配（单位列），
    含"月"字的列视为分月展示的月份列 -> data_year_month。"""
    colmap = {}
    for i, (text, value) in enumerate(headers_cols):
        if value in HEADER_FIELD_MAP:
            colmap[HEADER_FIELD_MAP[value]] = i
            continue
        matched = False
        if value is None:
            for key, field in TEXT_FIELD_MAP.items():
                if key in text:
                    colmap[field] = i
                    matched = True
                    break
        if not matched and "月" in text:
            colmap["data_year_month"] = i
    return colmap


def norm_year_month(raw, default_year):
    """月份单元格 -> 'YYYYMM'；支持 202601 / 2026-01 / 2026年1月 / 01月 / 1 等格式；
    无法解析返回 None（该行跳过）"""
    text = str(raw or "").strip()
    if not text:
        return None
    m = re.search(r"(20\d{2})\D*(\d{1,2})", text)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}"
    m = re.search(r"(\d{1,2})\s*月", text)
    if m:
        return f"{default_year}{int(m.group(1)):02d}"
    if re.fullmatch(r"\d{1,2}", text) and 1 <= int(text) <= 12:
        return f"{default_year}{int(text):02d}"
    return None


def to_number(raw):
    """数量/金额 -> 数值：去千分位逗号；'—'/空 -> 0"""
    text = str(raw or "").replace(",", "").strip()
    if text in ("", "—", "-", "--"):
        return 0
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return 0


def build_record(cells, colmap, default_year, imp_exp):
    """一行单元格 -> 按 DB_COLUMNS 顺序的取值元组；月份解析失败返回 None（跳过该行）。
    imp_exp_type 不来自表头，由本轮查询方向给定。"""
    record = []
    for col in DB_COLUMNS:
        if col == "imp_exp_type":
            record.append(int(imp_exp))
            continue
        idx = colmap.get(col)
        raw = cells[idx] if idx is not None and idx < len(cells) else ""
        if col == "data_year_month":
            ym = norm_year_month(raw, default_year)
            if ym is None:
                return None
            record.append(ym)
        elif col in NUMERIC_COLS:
            record.append(to_number(raw))
        elif col in NULLABLE_COLS:
            text = str(raw or "").strip()
            record.append(None if text in ("", "—", "-") else text)
        else:
            text = str(raw or "").strip()
            record.append("" if text in ("—", "-") else text)
    return record


# ============================ 材料消息与 Kafka 投递 ============================
def uuidv7():
    """生成 UUIDv7（RFC 9562）：48 位毫秒时间戳 + 随机位，如 0193c3f5-7a1b-7000-8000-xxxxxxxxxxxx"""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = os.urandom(10)
    b = bytearray(16)
    b[0:6] = ms.to_bytes(6, "big")
    b[6] = 0x70 | (rand[0] & 0x0F)  # version 7
    b[7] = rand[1]
    b[8] = 0x80 | (rand[2] & 0x3F)  # variant 10
    b[9:] = rand[3:]
    return str(uuid.UUID(bytes=bytes(b)))


def _canon_num(v):
    """数量/金额 -> 规范化数值：整数值取 int，否则 float；兼容 Decimal。
    保证"页面解析路径"与"DB 播种路径"生成完全一致的 JSON，使 contentHash 可复现。"""
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else v
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return 0


def build_month_material(ym, goods_code, goods_name, imp_exp, records, url):
    """同一月份(ym) × 商品 × 进出口方向 -> 一条材料消息。
    records: DB_COLUMNS 元组列表；明细按 (贸易伙伴编码, 注册地编码) 排序，
    保证 contentHash 跨运行稳定。title 形如「2026年08月 其他鲜樱桃」。"""
    rows = []
    for rec in records:
        r = dict(zip(DB_COLUMNS, rec))
        rows.append({
            "trade_partner_code": r["trade_partner_code"],
            "trade_partner_name": r["trade_partner_name"],
            "register_area_code": r["register_area_code"],
            "register_area_name": r["register_area_name"],
            "first_qty": _canon_num(r["first_qty"]),
            "first_unit": r["first_unit"],
            "second_qty": _canon_num(r["second_qty"]),
            "second_unit": r["second_unit"],
            "rmb_amount": _canon_num(r["rmb_amount"]),
        })
    rows.sort(key=lambda r: (r["trade_partner_code"], r["register_area_code"]))
    content_obj = {
        "data_year_month": ym,
        "goods_code": goods_code,
        "goods_name": goods_name,
        "iEType": "进口" if str(imp_exp) == "1" else "出口",
        "currency": "人民币",
        "records": rows,
    }
    content = json.dumps(content_obj, ensure_ascii=False, separators=(",", ":"))
    title = f"{int(ym[:4])}年{int(ym[4:6]):02d}月 {goods_name or goods_code}"
    material = {
        "materialId": uuidv7(),
        "sourceId": MATERIAL_SOURCE_ID,
        "sourceKind": MATERIAL_SOURCE_KIND,
        "url": url,
        "title": title,
        "contentHash": "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "content": content,
        "publishedAt": f"{ym[:4]}-{ym[4:6]}-01T00:00:00+08:00",
        "capturedAt": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    return {"specVersion": MATERIAL_SPEC_VERSION, "material": material}


def load_sent_hashes():
    """读已发送 contentHash 集合（跨运行持久）；文件缺失/损坏返回空集。"""
    path = RUNTIME / SENT_HASH_FILE
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text(encoding="utf-8")))
    except (ValueError, OSError):
        return set()


def save_sent_hashes(hashes):
    (RUNTIME / SENT_HASH_FILE).write_text(json.dumps(sorted(hashes)), encoding="utf-8")


def make_kafka_producer():
    """建立 Kafka 生产者（SASL，惰性导入 kafka-python）。"""
    try:
        from kafka import KafkaProducer
        from kafka.serializer import Serializer
    except ImportError as exc:
        raise RuntimeError("缺少 kafka-python，请先执行: pip install kafka-python") from exc

    class _JsonSerializer(Serializer):
        def serialize(self, topic, obj):
            return json.dumps(obj, ensure_ascii=False).encode("utf-8")

    kw = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "value_serializer": _JsonSerializer(),
        "acks": KAFKA_ACKS,
        "retries": KAFKA_RETRIES,
        "enable_idempotence": True,  # 幂等生产者：网络抖动导致的重试不会在 broker 端产生重复记录
    }
    if KAFKA_USERNAME:
        kw.update({
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    return KafkaProducer(**kw)


def send_materials(producer, messages, label, sent_hashes):
    """发送一批材料消息：contentHash 已在已发送集合中的跳过（不再发 Kafka）。
    全部确认成功才返回；任一失败抛 RetryableError，已确认的哈希即时落盘。"""
    to_send = [m for m in messages if m["material"]["contentHash"] not in sent_hashes]
    dup_skipped = len(messages) - len(to_send)
    futures = [
        (m["material"]["materialId"], m["material"]["contentHash"],
         producer.send(KAFKA_TOPIC, key=m["material"]["materialId"].encode("utf-8"), value=m))
        for m in to_send
    ]
    sent = 0
    for mid, ch, fut in futures:
        try:
            fut.get(timeout=KAFKA_SEND_TIMEOUT)
            sent += 1
            sent_hashes.add(ch)
        except Exception as exc:
            save_sent_hashes(sent_hashes)
            raise RetryableError(
                f"{label} Kafka 投递失败[{type(exc).__name__}: {exc}]（已确认 {sent}/{len(futures)}）"
            ) from exc
    if sent:
        save_sent_hashes(sent_hashes)
    return sent, dup_skipped


# ============================ MySQL 入库 ============================
def get_db_conn():
    """建立 MySQL 连接（带重试）。pymysql 用惰性导入，未装时给出提示。"""
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("缺少 pymysql，请先执行: pip install pymysql") from exc
    if MYSQL_CONFIG["password"] == "请填写密码" or MYSQL_CONFIG["database"] in ("", "请填写库名"):
        raise RuntimeError("请先在文件顶部 MYSQL_CONFIG 填写数据库连接信息（host/密码/库名）")
    last = None
    for i in range(DB_RETRIES):
        try:
            return pymysql.connect(autocommit=False, **MYSQL_CONFIG)
        except Exception as exc:
            last = exc
            if i < DB_RETRIES - 1:
                logger.warning(f"MySQL 连接失败[{type(exc).__name__}: {exc}]，{DB_RETRY_SLEEP}秒后重试")
                time.sleep(DB_RETRY_SLEEP)
    raise RuntimeError(f"MySQL 连接连续失败: {last}")


# 表带唯一键 uk_business_unique(data_year_month, imp_exp_type, goods_code, trade_partner_code, register_area_code)，
# 用 ON DUPLICATE KEY UPDATE 做幂等入库：重跑/续跑不报重复键错误，直接刷新数据列
_UPSERT_COLS = [c for c in DB_COLUMNS if c not in
                ("data_year_month", "imp_exp_type", "goods_code", "trade_partner_code", "register_area_code")]
INSERT_SQL = (
        f"INSERT INTO {DB_TABLE} (" + ", ".join(DB_COLUMNS) + ") VALUES ("
        + ", ".join(["%s"] * len(DB_COLUMNS)) + ")"
        + " ON DUPLICATE KEY UPDATE "
        + ", ".join(f"{c}=VALUES({c})" for c in _UPSERT_COLS)
)


def collect_records(cells_rows, colmap, default_year, imp_exp):
    """一页单元格 -> 记录元组列表；月份解析失败的行跳过。返回 (records, skipped)。"""
    records, skipped = [], 0
    for cells in cells_rows:
        rec = build_record(cells, colmap, default_year, imp_exp)
        if rec is None:
            skipped += 1
            continue
        records.append(rec)
    return records, skipped


def insert_page_rows(conn, records, label):
    """一批记录批量入库，返回入库行数。入库失败自动重试并重连。"""
    if not records:
        return 0
    for attempt in range(DB_RETRIES):
        try:
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, records)
            conn.commit()
            return len(records)
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt < DB_RETRIES - 1:
                logger.warning(f"{label} 入库失败[{type(exc).__name__}: {exc}]，{DB_RETRY_SLEEP}秒后重试")
                time.sleep(DB_RETRY_SLEEP)
                try:
                    conn.ping(reconnect=True)
                except Exception:
                    pass
            else:
                raise RetryableError(f"{label} 入库连续 {DB_RETRIES} 次失败: {exc}") from exc
    return 0


# ============================ 单轮完整流程（含翻页） ============================
def run_once(conn, producer, sent_hashes, year, code, m1, m2, imp_exp, round_no, round_total):
    direction = "进口" if str(imp_exp) == "1" else "出口"
    logger.info(f"===== 轮次 {round_no}/{round_total} | {year}年 商品{code} {direction} 月{m1}-{m2}（分月展示） =====")
    cparams = captcha_params(year, code, m1, m2, imp_exp)
    list_url = QUERY_LIST + "?" + "&".join(f"{k}={v}" for k, v in cparams.items())
    # 材料消息里的 url 用干净的表单参数（与站点 queryDataList 查询地址一致，不含滑块页附加参数与 currentStartTime）
    _url_form = build_form(year, code, m1, m2, 1, imp_exp)
    _url_form.pop("currentStartTime", None)
    query_url = QUERY_LIST + "?" + form_body(_url_form)

    def establish_session():
        """[1]-[6] 建会话拿列表页；返回 (session, list_path, list_page, static_path, static_url, colmap, referer)"""
        # [1] 隧道代理探测
        session = make_session()
        probe = http(session, "get", HOME_URL, timeout=20, allow_redirects=False, headers=nav_headers())
        if probe.status_code not in (200, 412):
            raise RetryableError(f"隧道代理探测异常 [{probe.status_code}]")
        logger.success(f"隧道代理可用（探测 GET / -> {probe.status_code}）")

        # [2] 过瑞数 412 挑战，生成 T cookie
        challenge = http(session, "get", HOME_URL, timeout=30, allow_redirects=False, headers=nav_headers())
        if challenge.status_code != 412 or "$_ts" not in challenge.text:
            raise RetryableError(f"首页预期 412 挑战页，实际 {challenge.status_code}")
        challenge_path = write_runtime("challenge.html", challenge.text)
        static_path, static_url = fetch_static_js(session, challenge.text, HOME_URL)
        if not static_path:
            raise RetryableError(f"瑞数脚本拉取失败: {static_url}")
        cookie_info = node_rs6(session, challenge_path, HOME_URL, static_path, static_url,
                               build_form(year, code, m1, m2, 1, imp_exp), skip_xhr=True, stop_on_first_cookie=True)
        t_cookie = (cookie_info.get("cookies") or {}).get("AV7KYchI7HHaT") or ""
        if len(t_cookie) < 300:
            raise RuntimeError(
                f"T cookie 生成异常（{len(t_cookie)}位）: {'; '.join(node_stage_errors(cookie_info)) or '未知'}")
        apply_node_cookies(session, cookie_info)
        logger.success(f"T cookie 生成成功（{len(t_cookie)}位）")

        # [3] 进站建立会话（JSESSIONID）
        home = http(session, "get", HOME_URL, timeout=30, headers=nav_headers(HOME_URL))
        if home.status_code != 200:
            raise RetryableError(f"二跳首页 {home.status_code}（T cookie 未被瑞数接受）")
        home_static_path, _ = fetch_static_js(session, home.text, home.url)
        if home_static_path:
            static_path = home_static_path
        logger.success("首页 200，JSESSIONID 建立")

        # [4] 查询条件页
        query_page = http(session, "get", QUERY_PAGE, timeout=30, headers=nav_headers(HOME_URL))
        if query_page.status_code != 200:
            raise RetryableError(f"查询条件页 {query_page.status_code}")
        query_static_path, _ = fetch_static_js(session, query_page.text, query_page.url)
        if query_static_path:
            static_path = query_static_path

        # [5] 滑块页打标（服务端会话打标，滑块本身可跳过）
        captcha_url = BASE + "/queryData/toCaptchaView?" + "&".join(f"{k}={v}" for k, v in cparams.items())
        captcha_page = http(session, "get", captcha_url, timeout=30, headers=nav_headers(query_page.url))
        if captcha_page.status_code != 200:
            raise RetryableError(f"滑块页 {captcha_page.status_code}")
        logger.success("toCaptchaView 200，会话已打标")

        # [6] 列表页（拿 thead 表头 + $_ts 种子）
        list_page = http(session, "get", list_url, timeout=30, headers=nav_headers(captcha_page.url))
        if list_page.status_code != 200 or "$_ts" not in list_page.text:
            raise RetryableError(f"列表页 {list_page.status_code}（需含 $_ts 种子）")
        list_path = write_runtime("list.html", list_page.text)
        list_static_path, static_url = fetch_static_js(session, list_page.text, list_page.url)
        if list_static_path:
            static_path = list_static_path

        headers_cols = parse_thead_columns(list_page.text)
        colmap = build_column_map(headers_cols)
        logger.info(f"表头列: {headers_cols}")
        required = ["data_year_month", "goods_code", "goods_name",
                    "trade_partner_code", "trade_partner_name",
                    "register_area_code", "register_area_name"]
        missing = [c for c in required if c not in colmap]
        if missing == ["data_year_month"]:
            # 兜底：列表页表头未渲染月份列时，按"月份在行首、其余列右移一位"处理
            logger.warning("表头缺少月份列——按'月份在行首'兜底，其余列右移一位（如入库年月异常请看上方表头日志）")
            colmap = {k: (v + 1) for k, v in colmap.items()}
            colmap["data_year_month"] = 0
            missing = []
        if missing:
            raise RuntimeError(f"表头缺少关键列 {missing}——检查 outerField/monthFlag 配置（表头: {headers_cols}）")
        logger.success(f"列表页 200，表头映射 {colmap}")
        return session, list_path, list_page, static_path, static_url, colmap, captcha_page.url

    session, list_path, list_page, static_path, static_url, colmap, referer = establish_session()

    def renew_session():
        """隧道连接老化（连续 200 空响应/400）时重建会话，翻页进度不丢"""
        nonlocal session, list_path, list_page, static_path, static_url, colmap, referer
        logger.warning("会话续期：重新建立瑞数会话（新隧道连接）")
        session, list_path, list_page, static_path, static_url, colmap, referer = establish_session()

    # [7] 翻页取数入库
    def post_page(page):
        """请求第 page 页；失败时重试 PAGE_RETRIES 次：先换列表页新 $_ts，第 3 次失败后重建会话"""
        form = build_form(year, code, m1, m2, page, imp_exp)
        for retry in range(PAGE_RETRIES):
            if retry >= 1 and retry != 3:
                fresh = http(session, "get", list_url, timeout=30, headers=nav_headers(referer))
                if fresh.status_code == 200 and "$_ts" in fresh.text:
                    nonlocal list_page, list_path, colmap
                    list_page = fresh
                    list_path = write_runtime("list.html", fresh.text)
                    cols = parse_thead_columns(fresh.text)
                    if cols:
                        colmap = build_column_map(cols)
                    logger.warning(f"已重取列表页（新 $_ts）")
            elif retry == 3:
                renew_session()
            try:
                suffix_info = node_rs6(session, list_path, list_page.url, static_path, static_url, form)
                suffix = suffix_info.get("8h6a7FPl") or ""
                header_suffix = suffix_info.get("headerSuffix") or suffix
                t_send = (suffix_info.get("cookiesAtSend") or {}).get("AV7KYchI7HHaT") or ""
                if not suffix or not t_send:
                    raise RetryableError(f"动态参数生成失败（suffix={len(suffix)}位, T={len(t_send)}位）: "
                                         f"{'; '.join(node_stage_errors(suffix_info)) or '未知'}")
                api_url = suffix_info.get("url") or f"{QUERY_API}?8h6a7FPl={suffix}"
                body = suffix_info.get("body") or form_body(form)
                post_cookies = dict(suffix_info.get("cookiesAtSend") or {})
                jar = session.cookies.get_dict()
                for name in ("AV7KYchI7HHaS", "__jsluid_h", "SF_cookie_251", "JSESSIONID"):
                    if not post_cookies.get(name) and jar.get(name):
                        post_cookies[name] = jar[name]
                headers = {
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html, */*; q=0.01",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Origin": BASE,
                    "Referer": list_page.url,
                    "8h6a7FPl": header_suffix,
                    "Cookie": cookie_header_from_map(post_cookies),
                }
                resp = http(session, "post", api_url, timeout=30, data=body, headers=headers)
                if resp.status_code != 200:
                    raise RetryableError(f"第{page}页 接口返回 {resp.status_code}")
                if len(resp.text) == 0:
                    raise RetryableError(f"第{page}页 200 但响应为空（会话与后缀不同轮）")
                if "$_ts" in resp.text:
                    raise RetryableError(f"第{page}页 返回瑞数挑战页（动态参数校验未过）")
                # 把 cookie 更新到本页发送后状态，供下一页继续使用（浏览器行为）
                apply_node_cookies(session, suffix_info)
                return resp
            except RetryableError as exc:
                if retry < PAGE_RETRIES - 1:
                    wait = PAGE_RETRY_BASE_SLEEP + retry
                    logger.warning(f"第{page}页 第{retry + 1}/{PAGE_RETRIES}次失败[{exc}]，{wait}秒后重试")
                    time.sleep(wait)
                else:
                    raise

    buffered = []  # 整轮缓冲（跨页），轮尾统一入库 + 按月聚合投递
    total_rows = -1
    page = 1
    while page <= MAX_PAGES:
        resp = post_page(page)
        cells = parse_result_rows(resp.text)
        total_rows, _ = parse_result(resp.text)
        write_runtime("result.html", resp.text)  # 每页覆盖，调试用
        if not cells:
            if total_rows >= 0 and len(buffered) < total_rows:
                logger.warning(f"第{page}页为空但总数 {total_rows} > 已取 {len(buffered)}，提前停止翻页")
            break
        expected_width = max(colmap.values()) + 1 if colmap else 0
        if cells and len(cells[0]) != expected_width:
            logger.warning(f"第{page}页 行宽 {len(cells[0])} 与表头映射宽度 {expected_width} 不一致，仍按映射取值")
        records, skipped = collect_records(cells, colmap, year, imp_exp)
        buffered.extend(records)
        logger.info(f"第{page}页: {len(cells)} 行（解析 {len(records)}"
                    + (f"，跳过 {skipped}" if skipped else "")
                    + f"）累计 {len(buffered)}" + (f"/{total_rows}" if total_rows >= 0 else ""))
        if total_rows >= 0 and len(buffered) >= total_rows:
            break
        if len(cells) < int(PAGE_SIZE):
            break
        page += 1
        time.sleep(PAGE_INTERVAL)

    if page > MAX_PAGES:
        raise RetryableError(f"{year}-{code} {direction} 翻页超过安全上限 {MAX_PAGES} 页，数据可能未取完")

    label = f"{year}-{code}-{'imp' if str(imp_exp) == '1' else 'exp'} 月{m1}-{m2}"
    parts = []
    if conn is not None and buffered:
        inserted = insert_page_rows(conn, buffered, label)
        parts.append(f"MySQL 入库 {inserted}")
    if producer is not None and buffered:
        by_month = {}
        for rec in buffered:
            by_month.setdefault(rec[0], []).append(rec)
        sent = dup = 0
        for ym in sorted(by_month):
            recs = by_month[ym]
            goods_name = dict(zip(DB_COLUMNS, recs[0]))["goods_name"]
            msg = build_month_material(ym, code, goods_name, imp_exp, recs, query_url)
            s, d = send_materials(producer, [msg], f"{label} {ym}", sent_hashes)
            sent += s
            dup += d
            logger.info(f"  {ym}: {len(recs)} 条记录 -> Kafka 投递 {s}" + (f"（跳过重复 {d}）" if d else ""))
        parts.append(f"Kafka 投递 {sent}" + (f"（跳过重复 {dup}）" if dup else ""))
    logger.success(f"{year}-{code} {direction} 取数完成，解析 {len(buffered)} 行（{'，'.join(parts) or '未处理'}）")
    return len(buffered)


# ============================ 命令行参数（后台管理触发用） ============================
def parse_args(argv=None):
    """后台/命令行覆盖查询计划与输出目标；缺省时用文件顶部常量（原行为不变）。"""
    import argparse

    p = argparse.ArgumentParser(
        description="海关统计批量采集（多商品×多年份段×进出口分向），按月聚合投递 Kafka / 入库 MySQL",
    )
    p.add_argument("--goods", help="商品编码，逗号分隔（如 08106000,08052110）")
    p.add_argument("--years", help="年份段，逗号分隔，每段为 年:起月:止月（如 2026:1:8,2025:2:12）")
    p.add_argument("--sink", choices=["kafka", "mysql", "both"], help="输出目标（缺省用 SINK_MODE）")
    p.add_argument("--page-size", choices=["20", "50", "100", "200"], help="每页条数（缺省用 PAGE_SIZE）")
    p.add_argument("--proxy", help="代理 URL（空字符串 = 直连；缺省用 PROXY_URL）")
    return p.parse_args(argv)


def apply_overrides(args):
    global PAGE_SIZE, SINK_MODE, PROXY_URL, GOODS_CODES, QUERY_YEARS
    if args.page_size:
        PAGE_SIZE = args.page_size
    if args.sink:
        SINK_MODE = args.sink
    if args.proxy is not None:
        PROXY_URL = args.proxy.strip()
    if args.goods:
        codes = [c.strip() for c in args.goods.split(",") if c.strip()]
        if not codes:
            raise SystemExit("--goods 参数为空")
        GOODS_CODES = codes
    if args.years:
        segs = []
        for seg in args.years.split(","):
            seg = seg.strip()
            if not seg:
                continue
            try:
                y, m1, m2 = (int(x) for x in seg.split(":"))
            except ValueError:
                raise SystemExit(f"年份段格式错误: {seg!r}（应为 年:起月:止月，如 2026:1:8）")
            if not (1 <= m1 <= 12 and 1 <= m2 <= 12 and m1 <= m2):
                raise SystemExit(f"月份范围非法: {seg!r}")
            segs.append((y, m1, m2))
        QUERY_YEARS = segs


# ============================ 批量驱动 ============================
def main():
    apply_overrides(parse_args())
    logger.info(f"海关统计数据查询 {BASE} | Python {sys.version.split()[0]} | TLS 指纹 {FIREFOX}")
    logger.info(f"计划: {len(GOODS_CODES)} 商品 × {len(QUERY_YEARS)} 段 × 2 方向(进口/出口) = "
                f"{len(GOODS_CODES) * len(QUERY_YEARS) * 2} 轮 | pageSize={PAGE_SIZE} | 分月展示={MONTH_FLAG} | 库按年份月份自动选择(2025-01及更早历史库, 2025-02起活跃库)")

    if SINK_MODE not in ("kafka", "mysql", "both"):
        logger.error(f"SINK_MODE 非法: {SINK_MODE}，应为 kafka / mysql / both")
        sys.exit(1)
    need_kafka = SINK_MODE in ("kafka", "both")
    need_db = SINK_MODE in ("mysql", "both")

    producer = conn = None
    if need_kafka:
        try:
            producer = make_kafka_producer()
            logger.success(f"Kafka 生产者就绪 {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} "
                           f"（{KAFKA_SECURITY_PROTOCOL}/{KAFKA_SASL_MECHANISM}）")
        except RuntimeError as exc:
            logger.error(str(exc))
            sys.exit(1)
    if need_db:
        try:
            conn = get_db_conn()
            logger.success(f"MySQL {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']} 连接成功")
        except RuntimeError as exc:
            logger.error(str(exc))
            sys.exit(1)

    # 已发送 contentHash 集合：文件持久（跨运行判重）
    sent_hashes = load_sent_hashes() if need_kafka else set()
    if sent_hashes:
        logger.info(f"已发送 contentHash 集合载入: {len(sent_hashes)} 条")

    rounds = [(y, c, m1, m2, d) for y, m1, m2 in QUERY_YEARS for c in GOODS_CODES for d in ("1", "0")]
    total = len(rounds)
    ok_count = fail_count = consecutive_fail = 0
    total_rows = 0
    failed_labels = []

    for idx, (year, code, m1, m2, imp_exp) in enumerate(rounds, 1):
        direction = "进口" if imp_exp == "1" else "出口"
        label = f"{year}年{code} {direction} 月{m1}-{m2}"
        rows = None
        for attempt in range(1, MAX_RUN_ATTEMPTS + 1):
            try:
                rows = run_once(conn, producer, sent_hashes, year, code, m1, m2, imp_exp, idx, total)
                break
            except RetryableError as exc:
                if attempt < MAX_RUN_ATTEMPTS:
                    logger.warning(f"{label} 第 {attempt}/{MAX_RUN_ATTEMPTS} 次失败[{exc}]，整轮重试")
                else:
                    logger.error(f"{label} 连续 {MAX_RUN_ATTEMPTS} 次失败: {exc}")
            except RuntimeError as exc:
                logger.error(f"{label} 不可恢复错误: {exc}")
                break
        if rows is None:
            fail_count += 1
            consecutive_fail += 1
            failed_labels.append(label)
            if consecutive_fail >= CONSECUTIVE_FAIL_ABORT:
                logger.error(f"连续 {CONSECUTIVE_FAIL_ABORT} 轮失败，疑似代理/环境失效，中止后续轮次")
                break
        else:
            ok_count += 1
            consecutive_fail = 0
            total_rows += rows
        if idx < total:
            time.sleep(ROUND_INTERVAL)

    if producer is not None:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    logger.info(f"===== 汇总: 成功 {ok_count}/{total} 轮，共处理 {total_rows} 行（SINK_MODE={SINK_MODE}，"
                f"已发送哈希 {len(sent_hashes)}）=====")
    if failed_labels:
        shown = failed_labels[:20]
        logger.error(f"失败轮次({len(failed_labels)}): {', '.join(shown)}"
                     + (" ..." if len(failed_labels) > 20 else ""))
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
