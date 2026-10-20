# -*- coding: utf-8 -*-
"""坐标抽样校验：
1. 格式检查 + 同代码坐标一致性 + 按国家离群点检测；
2. 随机抽样若干行，用 Photon 独立交叉比对距离（仅对 UN/LOCODE 匹配的行有意义，
   地理编码补的行同源跳过）；
3. 打印样本供人工核对。
"""
import csv
import re
import sys
import io
import time
import random
from collections import defaultdict, Counter
from difflib import SequenceMatcher

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from port_坐标 import build_data, resolve, norm
import geo_fill

rows = list(csv.reader(open("港口_航线.csv", encoding="utf-8-sig")))
header = rows[0]
ci = header.index("坐标(纬度,经度)")
data = rows[1:]

print("== 1. 格式检查 ==")
bad_fmt = 0
for r in data:
    v = r[ci].strip()
    if not v:
        continue
    m = re.match(r"^[-]?\d+\.\d+,[-]?\d+\.\d+$", v)
    if not m:
        print("   格式异常:", r[0], r[1], repr(v))
        bad_fmt += 1
    else:
        lat, lon = map(float, v.split(","))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            print("   范围异常:", r[0], r[1], v)
            bad_fmt += 1
print("   异常数:", bad_fmt)

print("\n== 2. 同代码坐标一致性 ==")
code_coords = defaultdict(set)
for r in data:
    if r[ci].strip():
        code_coords[r[0]].add(r[ci].strip())
mismatch = {c: v for c, v in code_coords.items() if len(v) > 1}
print("   同代码不同坐标的代码数:", len(mismatch))
for c, v in list(mismatch.items())[:20]:
    print("   ", c, sorted(v))

print("\n== 3. 按国家离群点检测（质心距离 > 3倍中位偏差） ==")
by_country = defaultdict(list)
for r in data:
    if r[ci].strip():
        lat, lon = map(float, r[ci].split(","))
        by_country[r[2]].append((r[0], r[1], lat, lon))
import math

def hav(a, b):
    la1, lo1, la2, lo2 = map(math.radians, a + b)
    dlat, dlon = la2 - la1, lo2 - lo1
    h = math.sin(dlat / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin(dlon / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))

flags = 0
for country, pts in by_country.items():
    if len(pts) < 6:
        continue
    lats = [p[2] for p in pts]
    lons = [p[3] for p in pts]
    clat = sum(lats) / len(lats)
    clon = sum(lons) / len(lons)
    dists = sorted(hav((clat, clon), (la, lo)) for _, _, la, lo in pts)
    med = dists[len(dists) // 2]
    for code, name, la, lo in pts:
        d = hav((clat, clon), (la, lo))
        if med > 0 and d > med * 3 + 300:
            print("   [离群?] %s | %s | %s | %.5f,%.5f | 离质心 %.0f km" % (code, name[:30], country, la, lo, d))
            flags += 1
print("   可疑离群点数:", flags)

# == 4. 抽样交叉比对（UN/LOCODE 行 vs Photon）==
print("\n== 4. 抽样交叉比对 ==")
entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx = build_data()
valid_isos = set(by_iso.keys()) | set(iso_any_names.keys())

tiers = []
for i, r in enumerate(data):
    code = r[0].strip().upper()
    m = re.search(r"（([^（）]+)）", r[1])
    eng = m.group(1) if m else ""
    lat, lon, tier, iso = resolve(eng, code, entries, code_info, by_iso,
                                  exact_idx, token_idx, valid_isos,
                                  iso_any_names, alias_idx)
    tiers.append((i, tier))

unlocode_idx = [i for i, t in tiers if t in ("code", "name_exact", "name_fuzzy_hint",
                                             "name_exact_global", "name_fuzzy_global")]
random.seed(42)
sample = random.sample(unlocode_idx, 30)

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"}

def photon(q):
    try:
        r = requests.get("https://photon.komoot.io/api/",
                         params={"q": q, "limit": 1, "lang": "en"}, headers=UA, timeout=15)
        if r.status_code == 200:
            feats = r.json().get("features", [])
            if feats and feats[0].get("geometry", {}).get("coordinates"):
                c = feats[0]["geometry"]["coordinates"]
                return c[1], c[0], (feats[0]["properties"].get("name") or "")
        return None
    except Exception:
        return None

print("   共抽样 %d 行(UN/LOCODE 匹配)，与 Photon 交叉比对:" % len(sample))
far = 0
for i in sample:
    r = data[i]
    v = r[ci].strip()
    if not v:
        continue
    lat, lon = map(float, v.split(","))
    m = re.search(r"（([^（）]+)）", r[1])
    if not m:
        continue
    res = photon(norm(m.group(1)))
    if not res:
        print("   %s | %s | %s | %.4f,%.4f | Photon无结果" % (r[0], m.group(1)[:28], r[2], lat, lon))
        continue
    plat, plon, pname = res
    d = hav((lat, lon), (plat, plon))
    mark = "OK" if d < 60 else ("远(%dkm)" % int(d))
    if d >= 60:
        far += 1
        mark = "!! %s" % mark
    print("   %s | %-28s | %s | %.4f,%.4f | Photon: %-22s %.4f,%.4f | %s"
          % (r[0], m.group(1)[:28], r[2], lat, lon, pname[:22], plat, plon, mark))
    time.sleep(0.5)
print("   距离 >= 60km 的行数:", far)
