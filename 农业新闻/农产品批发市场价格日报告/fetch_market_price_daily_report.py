# -*- coding: utf-8 -*-
"""
农产品批发市场价格日报抓取脚本

抓取农业农村部"全国农产品批发市场价格信息系统"的日报接口(selectDailyList),
解析每期日报的标题、来源、发布时间与正文, 所有内容汇总写入同一个 Markdown 文件。

用法:
    python fetch_market_price_daily_report.py          # 增量更新: 只抓汇总文件里缺少的新日报, 插到文件开头
    python fetch_market_price_daily_report.py --full   # 全量重抓: 忽略已有汇总文件, 重新抓全部并覆盖写入

抓取策略:
1. 每个请求优先走代理, 最多尝试 MAX_RETRIES 次; 代理仍失败则切换直连(同样最多 MAX_RETRIES 次);
2. 相邻两页之间随机休眠, 重试之间固定休眠, 避免不间断抓取;
3. pageSize 固定用 100, 减少请求次数。
"""
import html
import math
import os
import random
import re
import sys
import time

import requests

# ---------------- 可调配置 ----------------
URL = "https://ncpscxx.moa.gov.cn/product/FarmDaily/selectDailyList"
PAGE_SIZE = 100       # 服务端支持 10/20/50/100, 固定用 100 减少请求次数
MAX_RETRIES = 3       # 代理、直连各自最多尝试次数
REQUEST_TIMEOUT = 15  # 单次请求超时(秒)
SLEEP_RANGE = (2, 4)  # 相邻两页之间的随机休眠区间(秒)
RETRY_SLEEP = 2       # 单次失败重试前的休眠(秒)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "农产品批发市场价格日报汇总.md")

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
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0"),
    "sec-ch-ua": "\"Microsoft Edge\";v=\"155\", \"Chromium\";v=\"155\", \"Not(A:Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
}


def fetch_page(session, page_no):
    """请求一页数据: 先代理重试 MAX_RETRIES 次, 仍失败则直连再试 MAX_RETRIES 次。"""
    payload = {"pageSize": PAGE_SIZE, "currentPage": page_no, "startTime": "", "endTime": ""}

    for label, extra in (("代理", {"proxies": PROXIES}), ("直连", {})):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.post(URL, headers=HEADERS, json=payload,
                                    timeout=REQUEST_TIMEOUT, **extra)
                resp.raise_for_status()
                result = resp.json()
                if result.get("code") != 0 or not isinstance(result.get("data"), dict):
                    raise RuntimeError(f"接口返回异常 code={result.get('code')} msg={result.get('msg')}")
                return result["data"], label
            except Exception as exc:
                print(f"    第{page_no}页 {label}第{attempt}次尝试失败: {exc}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"第{page_no}页: 代理与直连共{MAX_RETRIES * 2}次请求均失败")


def strip_html(raw):
    """去掉 countent 里的 HTML 标签, 还原成纯文本段落。"""
    if not raw:
        return ""
    text = re.sub(r"(?i)</p\s*>|<br\s*/?>", "\n", raw)  # 段落/换行标签转为真实换行
    text = re.sub(r"<[^>]+>", "", text)                 # 去掉其余标签
    text = html.unescape(text)
    lines = [re.sub(r"[ \t\u3000]+", " ", ln).strip() for ln in text.splitlines()]
    return "\n\n".join(ln for ln in lines if ln)


def get_date(rec):
    """接口里日期字段拼写为 daylyDate, 兼容其他写法; 返回 YYYY-MM-DD。"""
    return ((rec.get("daylyDate") or rec.get("dailyDate")
             or rec.get("createDate") or ""))[:10]


def build_title(rec):
    """按网站样式生成标题, 如: 8月7日：“农产品批发价格200指数”比上周五上升0.71个点。"""
    date_str = get_date(rec)
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
    cn_date = f"{int(m.group(2))}月{int(m.group(3))}日" if m else date_str

    seg = re.search(r"农产品批发价格200指数.{0,50}?个点", rec.get("indexConclusion") or "")
    if not seg:
        return (rec.get("remark") or "").strip() or f"{cn_date} 农产品批发市场价格日报"

    # 统一引号样式并去掉具体指数值, 与网站标题样式保持一致
    s = re.sub(r'["“”]', "", seg.group(0))
    s = re.sub(r"为[\d.]+，", "", s)
    s = s.replace("农产品批发价格200指数", "“农产品批发价格200指数”", 1)
    return f"{cn_date}：{s}。"


def build_entry(rec):
    """把一条日报记录整理成 Markdown 片段(标题 + 来源行 + 正文)。"""
    title = build_title(rec)
    source = (rec.get("source") or "").strip()
    pub_date = get_date(rec)
    body = strip_html(rec.get("countent"))

    # 早期(2021年)记录没有 countent 正文, 改用结构化字段拼出等价内容
    if not body:
        segs = []
        for key in ("indexConclusion", "animalConclusion", "vegetablesConclusion",
                    "fruitsConclusion", "aquaticConclusion"):
            seg = strip_html(rec.get(key))
            if seg:
                segs.append(seg)
        body = "\n\n".join(segs)

    # 正文里正常已包含"今日, 国内鲜活农产品..."这段, 缺失时用 incOrReduRange 字段补上
    extra = strip_html(rec.get("incOrReduRange") or rec.get("inCrReduRange"))
    if extra and "国内鲜活农产品批发市场重点监测" not in body.replace(" ", ""):
        body = f"{body}\n\n{extra}" if body else extra

    lines = [f"## {title}"]
    info = "　".join(x for x in (f"来源：{source}" if source else "",
                                 f"发布时间：{pub_date}" if pub_date else "") if x)
    if info:
        lines += ["", info]
    if body:
        lines += ["", body]
    return "\n".join(lines)


def write_markdown(records, trouble):
    """所有日报直接汇总写入同一个 md 文件, 相邻两天之间空两行。"""
    parts = ["# 农产品批发市场价格日报汇总"]
    if trouble:
        parts.append(f"> 注意: {trouble}, 本次已写入 {len(records)} 期, 文件可能缺少部分日期。")
    for rec in records:
        parts.append(build_entry(rec))
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n\n\n".join(parts))
        f.write("\n")
    print(f"\n共 {len(records)} 期日报已汇总写入: {OUTPUT_FILE}")


def get_latest_date_in_md():
    """读汇总文件里所有'发布时间：YYYY-MM-DD', 返回最大日期; 文件不存在或没有日期返回 None。"""
    if not os.path.exists(OUTPUT_FILE):
        return None
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        content = f.read()
    dates = re.findall(r"发布时间：(\d{4}-\d{2}-\d{2})", content)
    return max(dates) if dates else None


def fetch_new_records(session, latest_date):
    """从第 1 页(最新在前)开始抓, 只保留日期晚于 latest_date 的记录, 直到追上已有内容。"""
    records_by_id = {}
    total_pages = None
    page_no = 1
    while True:
        data, via = fetch_page(session, page_no)
        if total_pages is None:
            total_pages = data.get("pages") or 1
        records = data.get("records") or []
        new_cnt, hit_existing = 0, False
        for rec in records:
            d = get_date(rec)
            if d and d <= latest_date:
                hit_existing = True  # 已追上文件里最新的日期, 本页之后都是旧数据
                continue
            rid = rec.get("id")
            if rid and rid not in records_by_id:
                records_by_id[rid] = rec
                new_cnt += 1
        print(f"第 {page_no}/{total_pages} 页完成({via}), 本页新增 {new_cnt} 条, 累计 {len(records_by_id)} 条")
        page_no += 1
        if hit_existing or not records or page_no > total_pages:
            break
        time.sleep(random.uniform(*SLEEP_RANGE))
    return sorted(records_by_id.values(), key=get_date, reverse=True)


def prepend_to_markdown(entries):
    """把新日报插到文件开头(文档标题之后), 保持整个文件'最新在前'的顺序。"""
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        content = f.read()
    header, sep, rest = content.partition("\n\n\n")
    if sep:
        merged = header + "\n\n\n" + "\n\n\n".join(entries) + "\n\n\n" + rest.lstrip("\n")
    else:
        merged = "\n\n\n".join(entries) + "\n\n\n" + content.lstrip("\n")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(merged if merged.endswith("\n") else merged + "\n")


def main():
    session = requests.Session()
    latest = None if "--full" in sys.argv else get_latest_date_in_md()

    if latest is None:
        crawl_all(session)
        return

    # 增量模式: 中途失败一律不动已有文件, 避免写入半截内容
    print(f"增量模式: 汇总文件中最新日报为 {latest}, 只抓取之后的更新(加 --full 可全量重抓)")
    try:
        new_records = fetch_new_records(session, latest)
    except (KeyboardInterrupt, Exception) as exc:
        print(f"增量抓取未完成({exc}), 汇总文件保持不变")
        return
    if not new_records:
        print(f"没有比 {latest} 更新的日报, 文件保持不变")
        return
    prepend_to_markdown([build_entry(rec) for rec in new_records])
    print(f"已写入 {len(new_records)} 期新日报({get_date(new_records[-1])} ~ {get_date(new_records[0])}): {OUTPUT_FILE}")


def crawl_all(session):
    """全量抓取: 从第 1 页抓到最后一页, 覆盖写入汇总文件。"""
    records_by_id = {}
    total_pages = None
    expected_total = 0
    trouble = None  # 记录中断/失败原因

    print(f"开始抓取: {URL}")
    print(f"pageSize={PAGE_SIZE}, 页间随机休眠 {SLEEP_RANGE[0]}~{SLEEP_RANGE[1]} 秒, "
          f"代理/直连各最多尝试 {MAX_RETRIES} 次")
    try:
        page_no = 1
        while True:
            data, via = fetch_page(session, page_no)
            if total_pages is None:
                expected_total = data.get("total") or 0
                size = data.get("size") or PAGE_SIZE  # 防止服务端把 pageSize 改小
                total_pages = data.get("pages") or (math.ceil(expected_total / size) if expected_total else 1)
                print(f"服务端共 {expected_total} 条记录, 每页 {size} 条, 合计 {total_pages} 页")

            records = data.get("records") or []
            new_cnt = 0
            for rec in records:
                rid = rec.get("id")
                if rid and rid not in records_by_id:
                    records_by_id[rid] = rec
                    new_cnt += 1
            print(f"第 {page_no}/{total_pages} 页完成({via}), 本页 {new_cnt} 条新记录, 累计 {len(records_by_id)} 条")

            page_no += 1
            if not records or (total_pages and page_no > total_pages):
                break
            time.sleep(random.uniform(*SLEEP_RANGE))
    except KeyboardInterrupt:
        trouble = "手动中断"
    except Exception as exc:
        trouble = f"抓取中断({exc})"

    if expected_total and len(records_by_id) < expected_total:
        print(f"警告: 预期 {expected_total} 条, 实际去重后 {len(records_by_id)} 条, 结果可能不完整")

    # 按日报日期倒序(最新在前)汇总
    records = sorted(records_by_id.values(), key=get_date, reverse=True)
    write_markdown(records, trouble)


if __name__ == "__main__":
    main()
