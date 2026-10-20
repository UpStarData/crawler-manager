# -*- coding: utf-8 -*-
"""
海关统计数据查询平台自动查询（瑞数 RS6 + 拼图滑块链路）+ CSV 导出版

与 test.py 的区别（仅此一处逻辑变化）：
    - 第 [8] 步查询成功后，除写 result.html 外，把返回的数据行解析出来，
      结合同轮 list.html 的 <thead> 表头，输出 runtime/result.csv。
    - CSV 用 utf-8-sig（带 BOM）编码，Excel 直接打开中文不乱码；
      纯数字列自动去掉千分位逗号（如 3,073,146,591 -> 3073146591）。
    - 其余链路（过瑞数、生成动态参数、代理、重试）与 test.py 完全一致。

用法:
    python test_csv.py
    python test_csv.py --year 2026 --months 1:8 --goods 08092900 --ietype 10
    （后台管理页面以同样参数触发；不带参数时用文件顶部常量，原行为不变）

日志: loguru 风格（时间 | 级别 | 模块:函数:行号 - 消息），SUCCESS=绿色成功
重试: 单请求超时/断连就地重试 2 次；整轮失败自动重试，最多 3 轮
代理: 带认证的隧道代理（顶部 TUNNEL_* 配置），会话内连接复用保证整轮同一出口 IP
选库: selectTableState 缺省按年份月份自动选择（与批量版一致），可用 --state 覆盖
缓存: runtime/ 只保留 Node 必需文件——瑞数 JS 按文件名缓存（仅首次下载），
      挑战页/列表页 HTML 每轮覆盖，外加查询结果 result.html / result.csv
退出码: 成功 0，失败 1
"""
import csv
import json
import os
import re
import subprocess
import sys
import time
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
CSV_NAME = "result.csv"  # 每轮覆盖，与 result.html 同生命周期
BASE = "http://stats.customs.gov.cn"
HOME_URL = BASE + "/"
QUERY_PAGE = BASE + "/queryData/queryDataByWhere"
QUERY_LIST = BASE + "/queryData/queryDataList"
QUERY_API = BASE + "/queryData/getQueryDataListByWhere"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0"
# 隧道代理（带认证）；整轮必须同一出口 IP，curl 会话复用连接天然满足
TUNNEL_HOST = os.getenv("PROXY_HOST", "t130.juliangip.cc")
TUNNEL_PORT = int(os.getenv("PROXY_PORT", "13781"))
TUNNEL_AUTHKEY = os.getenv("PROXY_USERNAME", "")
TUNNEL_AUTHPWD = os.getenv("PROXY_PASSWORD", "")
PROXY_URL = os.getenv("PROXY_URL", "") or (
    f"http://{TUNNEL_AUTHKEY}:{TUNNEL_AUTHPWD}@{TUNNEL_HOST}:{TUNNEL_PORT}"
    if TUNNEL_AUTHKEY and TUNNEL_AUTHPWD else ""
)
TOTAL_STEPS = 8
MAX_RUN_ATTEMPTS = 3  # 整轮重试次数（隧道瞬断自动恢复）
REQ_RETRIES = 2  # 单请求就地重试次数
FIREFOX = getattr(curl_impersonate, "DEFAULT_FIREFOX", "firefox135")

# 查询条件：改这里即改 POST 内容（翻页改 pageNum）
FORM = {
    "pageSize": "200",  # 每页条数 10/20/50/100/200
    "pageNum": "1",  # 页码
    "iEType": "10",  # 1=进口 0=出口 10=进出口
    "currencyType": "rmb",  # rmb / usd
    "year": "2023",  # 年份
    "startMonth": "1",  # 开始月份
    "endMonth": "12",  # 结束月份
    "monthFlag": "1",  # 分月展示勾选 1
    "unitFlag": "true",
    "unitFlag1": "true",
    "codeLength": "8",  # 商品编码位数 2/4/6/8
    "outerField1": "CODE_TS",
    "outerValue1": "08052190",  # 商品编码，逗号分隔可多个
    "outerField2": "ORIGIN_COUNTRY",  # 原产国，值为ORIGIN_COUNTRY，添加且value置空即显示所有进出口国/地
    "outerValue2": "",  # 贸易伙伴（可置空），编码
    "outerField3": "",  # 贸易方式，值为TRADE_MODE，添加且value置空即显示所有贸易方式
    "outerValue3": "",  # 贸易方式（可置空）10为一般贸易
    "outerField4": "TRADE_CO_PORT",  # 收发货人注册地，添加且value置空即显示所有注册地
    "outerValue4": "",
    "orderType": "CODE ASC DEFAULT",
    "selectTableState": "3",  # 1=活跃库 2=历史库 3=两库（缺省按年份月份自动选择，见 year_state；--state 可覆盖）
    "currentStartTime": "202502",  # 平台下发值，勿改
}


def year_state(year, month):
    """库选择（2026-10-12 用户实测边界）：两库 state=3 接口返回 200 空响应，禁用；
    2025年1月及更早 = 历史库(2)；2025年2月起 = 活跃库(1)"""
    if year <= 2024:
        return "2"
    if year == 2025:
        return "2" if month <= 1 else "1"
    return "1"


# ============================ 命令行参数（后台管理触发用） ============================
def parse_args(argv=None):
    import argparse

    p = argparse.ArgumentParser(description="海关统计单轮查询（瑞数 RS6），结果导出 CSV")
    p.add_argument("--year", help="查询年份（缺省用 FORM['year']）")
    p.add_argument("--months", help="起止月 起月:止月（如 1:12）")
    p.add_argument("--goods", help="商品编码（outerValue1，逗号分隔可多个）")
    p.add_argument("--ietype", choices=["1", "0", "10"], help="1=进口 0=出口 10=进出口")
    p.add_argument("--page", help="页码")
    p.add_argument("--page-size", choices=["10", "20", "50", "100", "200"], help="每页条数")
    p.add_argument("--proxy", help="代理 URL（空字符串 = 直连；缺省用 PROXY_URL）")
    p.add_argument("--state", choices=["1", "2", "3"], help="库选择（缺省按年份月份自动选择）")
    return p.parse_args(argv)


def apply_overrides(args):
    global PROXY_URL
    if args.proxy is not None:
        PROXY_URL = args.proxy.strip()
    if args.year:
        FORM["year"] = args.year
    if args.months:
        try:
            m1, m2 = (int(x) for x in args.months.split(":"))
        except ValueError:
            raise SystemExit(f"--months 格式错误: {args.months!r}（应为 起月:止月，如 1:12）")
        if not (1 <= m1 <= 12 and 1 <= m2 <= 12 and m1 <= m2):
            raise SystemExit(f"月份范围非法: {args.months!r}")
        FORM["startMonth"] = str(m1)
        FORM["endMonth"] = str(m2)
    if args.goods:
        FORM["outerValue1"] = args.goods
    if args.ietype:
        FORM["iEType"] = args.ietype
    if args.page:
        FORM["pageNum"] = args.page
    if args.page_size:
        FORM["pageSize"] = args.page_size
    if args.state:
        FORM["selectTableState"] = args.state
    else:
        # 缺省按年份月份自动选库，避免两库(3) 200 空响应
        FORM["selectTableState"] = year_state(int(FORM["year"]), int(FORM["startMonth"]))


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


def form_body(form=None):
    """由 FORM 生成 POST body（逗号->%2C、空格->+，与站点 jQuery 序列化一致）"""
    from urllib.parse import urlencode

    return urlencode(form or FORM)


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


# ============================ 代理 ============================
def make_session():
    """隧道代理固定配置；curl 会话内连接复用，保证整轮同一出口 IP"""
    session = curl_requests.Session(impersonate=FIREFOX)
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session


# ============================ Node 瑞数 ============================
def node_rs6(session, html_path, page_url, static_path, static_url, skip_xhr=False, stop_on_first_cookie=False):
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
        "form": FORM,
        "scriptTimeoutMs": 15000,
        "body": form_body(),
    }
    if static_path:
        payload["staticSourcePath"] = rel(static_path)
    if static_url:
        payload["staticScriptSrc"] = static_url
    return call_node(payload)


def node_stage_errors(info):
    return [f"{s.get('name')}: {s.get('error')}" for s in (info.get("stages") or []) if not s.get("ok")]


def parse_result(html):
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
    m = re.search(r"共查询到\s*(\d+)\s*条数据", text)
    rows = int(m.group(1)) if m else -1
    return rows, text


# ============================ CSV 导出（相对 test.py 新增的部分） ============================
def _clean_cell(raw):
    """单元格清洗：去标签、实体反转义、压缩空白；纯数字去掉千分位逗号"""
    text = unescape(re.sub(r"<[^>]+>", "", raw or ""))
    text = re.sub(r"\s+", " ", text).strip()
    if re.fullmatch(r"\d[\d,]*", text):
        text = text.replace(",", "")
    return text


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


def parse_thead_headers(html):
    """从列表页 HTML 的 <thead> 提取表头文本（自动匹配 outerField 组合，含分月展示）"""
    m = re.search(r"<thead\b[^>]*>(.*?)</thead>", html or "", re.S | re.I)
    if not m:
        return []
    return [
        _clean_cell(h)
        for h in re.findall(
            r"""<div\b[^>]*class=["'][^"']*\btr-line\b[^"']*["'][^>]*>(.*?)</div>""",
            m.group(1),
            re.S | re.I,
        )
    ]


def export_csv(rows, headers, path):
    """写 CSV（utf-8-sig 带 BOM）；表头不足时补 列N，行长短自动对齐"""
    ncols = max((len(r) for r in rows), default=0)
    head = list(headers)
    if len(head) < ncols:
        head += [f"列{i + 1}" for i in range(len(head), ncols)]
    head = head[:ncols]
    path.parent.mkdir(exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(head)
        for row in rows:
            writer.writerow(row + [""] * (ncols - len(row)))
    return path


def write_result_csv(result_html, list_html):
    """由本轮 result.html + list.html 生成 result.csv，返回 (路径, 数据行数, 列数)"""
    rows = parse_result_rows(result_html)
    headers = parse_thead_headers(list_html)
    path = export_csv(rows, headers, RUNTIME / CSV_NAME)
    return path, len(rows), max((len(r) for r in rows), default=0)


# ============================ 单轮完整流程 ============================
def run_once(attempt):
    logger.info(f"===== 第 {attempt}/{MAX_RUN_ATTEMPTS} 轮 | 查询 {FORM['year']}年{FORM['startMonth']}月 "
                f"商品{FORM['outerValue1']} 第{FORM['pageNum']}页 =====")

    # [1] 隧道代理
    logger.info(f"[1/{TOTAL_STEPS}] 验证隧道代理 {TUNNEL_HOST}:{TUNNEL_PORT}")
    session = make_session()
    probe = http(session, "get", HOME_URL, timeout=20, allow_redirects=False, headers=nav_headers())
    if probe.status_code not in (200, 412):
        raise RetryableError(f"隧道代理探测异常 [{probe.status_code}]")
    logger.success(f"[1/{TOTAL_STEPS}] 隧道代理可用（探测 GET / -> {probe.status_code}）")

    # [2] 瑞数挑战
    logger.info(f"[2/{TOTAL_STEPS}] 过瑞数 412 挑战，生成 T cookie")
    challenge = http(session, "get", HOME_URL, timeout=30, allow_redirects=False, headers=nav_headers())
    logger.info(f"GET / -> {challenge.status_code}" + ("（瑞数挑战页，正常）" if challenge.status_code == 412 else ""))
    if challenge.status_code != 412 or "$_ts" not in challenge.text:
        raise RetryableError(f"首页预期 412 挑战页，实际 {challenge.status_code}")

    challenge_path = write_runtime("challenge.html", challenge.text)
    static_path, static_url = fetch_static_js(session, challenge.text, HOME_URL)
    if not static_path:
        raise RetryableError(f"瑞数脚本拉取失败: {static_url}")

    cookie_info = node_rs6(
        session, challenge_path, HOME_URL, static_path, static_url,
        skip_xhr=True, stop_on_first_cookie=True,
    )
    t_cookie = (cookie_info.get("cookies") or {}).get("AV7KYchI7HHaT") or ""
    if len(t_cookie) < 300:
        errs = "; ".join(node_stage_errors(cookie_info)) or "未知"
        raise RuntimeError(f"T cookie 生成异常（{len(t_cookie)}位）: {errs}")
    logger.success(f"[2/{TOTAL_STEPS}] T cookie 生成成功（{len(t_cookie)}位）")

    apply_node_cookies(session, cookie_info)
    logger.info(f"会话 cookie: {', '.join(f'{k}({len(v)})' for k, v in sorted(session.cookies.get_dict().items()))}")

    # [3] 进站建立会话
    logger.info(f"[3/{TOTAL_STEPS}] 进站建立会话（JSESSIONID）")
    home = http(session, "get", HOME_URL, timeout=30, headers=nav_headers(HOME_URL))
    if home.status_code != 200:
        raise RetryableError(f"二跳首页 {home.status_code}（T cookie 未被瑞数接受）")
    jsession = session.cookies.get_dict().get("JSESSIONID", "")
    logger.success(f"[3/{TOTAL_STEPS}] 首页 200，JSESSIONID {len(jsession)}位")
    # 注意：不要在首页后重刷/覆盖 cookie——T 对后续导航始终有效
    home_static_path, _ = fetch_static_js(session, home.text, home.url)
    if home_static_path:
        static_path = home_static_path

    # [4] 查询条件页
    logger.info(f"[4/{TOTAL_STEPS}] 查询条件页")
    query_page = http(session, "get", QUERY_PAGE, timeout=30, headers=nav_headers(HOME_URL))
    if query_page.status_code != 200:
        raise RetryableError(f"查询条件页 {query_page.status_code}")
    logger.success(f"[4/{TOTAL_STEPS}] queryDataByWhere 200（{len(query_page.text) // 1024}KB）")
    query_static_path, _ = fetch_static_js(session, query_page.text, query_page.url)
    if query_static_path:
        static_path = query_static_path

    # [5] 滑块页打标
    logger.info(f"[5/{TOTAL_STEPS}] 访问滑块页（服务端会话打标，滑块本身可跳过）")
    captcha_params = dict(FORM)
    captcha_params.update({"currentRecentTime": "202607", "currentDateBySource": "202608",
                           "currentEndTime": "202608", "unitFlag": "false", "unitFlag1": "false"})
    captcha_url = BASE + "/queryData/toCaptchaView?" + "&".join(f"{k}={v}" for k, v in captcha_params.items())
    captcha_page = http(session, "get", captcha_url, timeout=30, headers=nav_headers(query_page.url))
    if captcha_page.status_code != 200:
        raise RetryableError(f"滑块页 {captcha_page.status_code}")
    logger.success(f"[5/{TOTAL_STEPS}] toCaptchaView 200，会话已打标")

    # [6] 列表页
    logger.info(f"[6/{TOTAL_STEPS}] 列表页")
    list_url = QUERY_LIST + "?" + "&".join(f"{k}={v}" for k, v in dict(captcha_params).items())
    list_page = http(session, "get", list_url, timeout=30, headers=nav_headers(captcha_page.url))
    if list_page.status_code != 200 or "$_ts" not in list_page.text:
        raise RetryableError(f"列表页 {list_page.status_code}（需含 $_ts 种子）")
    list_path = write_runtime("list.html", list_page.text)
    logger.success(f"[6/{TOTAL_STEPS}] queryDataList 200（{len(list_page.text) // 1024}KB）")
    list_static_path, static_url = fetch_static_js(session, list_page.text, list_page.url)
    if list_static_path:
        static_path = list_static_path

    # [7] 动态参数
    logger.info(f"[7/{TOTAL_STEPS}] 生成瑞数动态参数 8h6a7FPl")
    suffix_info = node_rs6(session, list_path, list_page.url, static_path, static_url, skip_xhr=False)
    suffix = suffix_info.get("8h6a7FPl") or ""
    header_suffix = suffix_info.get("headerSuffix") or suffix
    t_send = (suffix_info.get("cookiesAtSend") or {}).get("AV7KYchI7HHaT") or ""
    if not suffix or not t_send:
        errs = "; ".join(node_stage_errors(suffix_info)) or "未知"
        raise RuntimeError(f"动态参数生成失败（suffix={len(suffix)}位, T={len(t_send)}位）: {errs}")
    logger.success(f"[7/{TOTAL_STEPS}] 动态参数生成成功")
    logger.info(f"URL 后缀({len(suffix)}位): {suffix}")
    logger.info(f"请求头后缀({len(header_suffix)}位): {header_suffix}")
    logger.info(f"同轮 T cookie: {len(t_send)}位")

    # [8] 提交查询（此步在 http() 内已有就地重试）
    logger.info(f"[8/{TOTAL_STEPS}] 提交查询 POST getQueryDataListByWhere")
    api_url = suffix_info.get("url") or f"{QUERY_API}?8h6a7FPl={suffix}"
    body = suffix_info.get("body") or form_body()
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
        raise RetryableError(f"接口返回 {resp.status_code}")
    if len(resp.text) == 0:
        raise RetryableError("接口 200 但响应为空（会话与后缀不同轮）")
    if "$_ts" in resp.text:
        raise RetryableError("接口返回瑞数挑战页（动态参数校验未过）")

    rows, plain = parse_result(resp.text)
    result_path = write_runtime("result.html", resp.text)
    # ---- CSV 导出（list.html 在同轮第 [6] 步已写入）----
    list_html = (RUNTIME / "list.html").read_text(encoding="utf-8") if (RUNTIME / "list.html").exists() else ""
    csv_path, csv_rows, csv_cols = write_result_csv(resp.text, list_html)
    logger.success(f"[8/{TOTAL_STEPS}] 查询成功 | HTTP 200 | {len(resp.text)} 字节 | "
                   f"共 {rows if rows >= 0 else '?'} 条数据")
    logger.info(f"数据预览: {plain[:120]}")
    logger.info(f"结果文件: {rel(result_path)}")
    logger.info(f"CSV 文件: {rel(csv_path)}（{csv_rows} 行 × {csv_cols} 列，表头取自同轮 list.html）")
    return True


def main():
    apply_overrides(parse_args())
    logger.info(f"海关统计数据查询 {BASE} | Python {sys.version.split()[0]} | TLS 指纹 {FIREFOX}")
    for attempt in range(1, MAX_RUN_ATTEMPTS + 1):
        try:
            run_once(attempt)
            sys.exit(0)
        except RetryableError as exc:
            if attempt < MAX_RUN_ATTEMPTS:
                logger.warning(f"第 {attempt} 轮失败[{exc}]，重试整轮（{attempt}/{MAX_RUN_ATTEMPTS}）")
            else:
                logger.error(f"连续 {MAX_RUN_ATTEMPTS} 轮失败，最后错误: {exc}")
                logger.error("排查: 隧道代理可用性/有效期，或按 README 调整 PROXY_URL")
                sys.exit(1)
        except RuntimeError as exc:
            logger.error(f"不可恢复错误: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
