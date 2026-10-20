# -*- coding: utf-8 -*-
"""
全球港口数据采集：
1. 从首页 https://gangkou.bmcx.com/ 解析「按航线」区块，拿到所有航线及其链接；
2. 逐个航线页抓取港口表格，提取：港口代码、港口名称、所在国家、航线；
3. 边抓边写入 CSV 文件（utf-8-sig，Excel 可直接打开）。
"""
import csv
import os
import re
import sys
import io
import time

import requests

# 控制台输出统一用 utf-8，避免 Windows 下中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------- 代理配置 ----------
# 通过环境变量 PROXY_URL 提供完整代理地址，例如：
#   set PROXY_URL=http://user:password@host:port/
proxy_url = os.getenv("PROXY_URL", "")
proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}

BASE_URL = "https://gangkou.bmcx.com"
OUT_CSV = "港口_航线.csv"

BASE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/jxl,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "max-age=0",
    "priority": "u=0, i",
    "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not(A:Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36",
}

session = requests.Session()


def fetch(url, referer=None, retries=3):
    """带重试的 GET 请求，返回响应文本；失败返回 None。"""
    headers = dict(BASE_HEADERS)
    if referer:
        headers["referer"] = referer
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=headers, proxies=proxies, timeout=20)
            if r.status_code == 200 and r.text:
                return r.text
            print("    警告: HTTP %s, 重试 %d" % (r.status_code, attempt))
        except Exception as e:
            print("    警告: %s: %s, 重试 %d" % (type(e).__name__, e, attempt))
        time.sleep(2 * attempt)
    return None


def get_routes(home_html):
    """从首页「按航线」区块解析出 (航线名, 链接) 列表，按出现顺序去重。"""
    start = home_html.find("港口查询 - 按航线")
    end = home_html.find("港口查询 - 按国家/地区")
    if start < 0 or end < 0:
        raise RuntimeError("首页未找到「按航线」区块")
    seg = home_html[start:end]
    routes, seen = [], set()
    for href, name in re.findall(r'<a href="(/[^"]+__gangkousou/)" target="_blank">([^<]+)</a>', seg):
        if href not in seen:
            seen.add(href)
            routes.append((name.strip(), BASE_URL + href))
    return routes


# 航线页港口数据行：代码、名称、国家、航线
ROW_RE = re.compile(
    r'<tr><td bgcolor="#F5F5F5"><a href="[^"]+"[^>]*>([^<]+)</a></td>'
    r'<td bgcolor="#FFFFFF">(.*?)</td>'
    r'<td bgcolor="#FFFFFF">(.*?)</td>'
    r'<td bgcolor="#FFFFFF">(.*?)</td></tr>'
)


def parse_ports(route_html):
    """解析航线页里的港口表格，返回 [(代码, 名称, 国家, 航线), ...]。"""
    rows = []
    for code, name, country, route in ROW_RE.findall(route_html):
        rows.append((code.strip(), name.strip(), country.strip(), route.strip()))
    return rows


def main():
    print("第一步: 请求首页获取航线列表 ...")
    home_html = fetch(BASE_URL + "/")
    if not home_html:
        print("首页请求失败，退出")
        sys.exit(1)

    routes = get_routes(home_html)
    print("共获取 %d 条航线\n" % len(routes))

    total = 0
    fail_routes = []
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["港口代码", "港口名称", "所在国家", "航线"])
        for idx, (route_name, route_url) in enumerate(routes, 1):
            print("[%d/%d] %s: %s" % (idx, len(routes), route_name, route_url))
            html = fetch(route_url, referer=BASE_URL + "/")
            if not html:
                print("    失败，跳过该航线")
                fail_routes.append(route_name)
                continue
            ports = parse_ports(html)
            for row in ports:
                writer.writerow(row)
            total += len(ports)
            f.flush()
            print("    港口数: %d" % len(ports))
            time.sleep(0.5)

    print("\n完成: 共 %d 条航线, 写入 %d 行港口数据 -> %s" % (len(routes), total, OUT_CSV))
    if fail_routes:
        print("失败航线: %s" % ", ".join(fail_routes))


if __name__ == "__main__":
    main()
