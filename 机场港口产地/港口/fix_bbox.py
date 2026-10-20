# -*- coding: utf-8 -*-
"""国家边界盒校验（改进版）：
- 国家映射：中文国家名 -> ISO（由已匹配行投票得到）+ 手动表 + 代码前缀 + 特殊前缀；
- 岛国/领地不参与过滤（领土分散，边界盒不可靠）；
- 大陆国家：坐标必须落在该国 UN/LOCODE 条目分布范围(+2度)内，否则删除。"""
import csv
import re
import sys
import io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from port_坐标 import build_data, resolve, HINT_EXTRA
from geo_fill import MANUAL_COUNTRY

IN_CSV = "港口_航线.csv"
COORD_HEADER = "坐标(纬度,经度)"
PAD = 2.0

# 岛国/海外领地：领土分散，不做边界盒过滤
ISLAND_ISOS = {
    "KI", "FM", "MH", "TV", "PF", "PG", "SB", "VU", "WS", "TO", "NR", "PW",
    "MP", "GU", "AS", "FJ", "MU", "SC", "MV", "KM", "ST", "CV", "BS", "AG",
    "DM", "GD", "KN", "LC", "VC", "BB", "TT", "JM", "HT", "DO", "PR", "VI",
    "VG", "BM", "KY", "TC", "MS", "AI", "AW", "CW", "BQ", "SX", "MF", "GP",
    "MQ", "RE", "YT", "NC", "WF", "NF", "NU", "CK", "PN", "FK", "GS", "SH",
    "FO", "GI", "MT", "CY", "SG", "BH", "BN", "TL",
}

entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx = build_data()
valid_isos = set(by_iso.keys()) | set(iso_any_names.keys())

# iso -> bbox
bbox = {}
for e in entries:
    iso = e["iso"]
    la, lo = e["lat"], e["lon"]
    b = bbox.get(iso)
    if b is None:
        bbox[iso] = [la, lo, la, lo]
    else:
        b[0] = min(b[0], la); b[1] = min(b[1], lo)
        b[2] = max(b[2], la); b[3] = max(b[3], lo)

rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
ci = rows[0].index(COORD_HEADER)

# 国家投票
votes = defaultdict(lambda: defaultdict(int))
for r in rows[1:]:
    code = r[0].strip().upper()
    m = re.search(r"（([^（）]+)）", r[1])
    eng = m.group(1) if m else ""
    lat, lon, tier, iso = resolve(eng, code, entries, code_info, by_iso,
                                  exact_idx, token_idx, valid_isos,
                                  iso_any_names, alias_idx)
    if iso and tier in ("code", "name_exact"):
        votes[r[2]][iso] += 1
country_iso = {}
for c, d in votes.items():
    if c not in MANUAL_COUNTRY and d:
        country_iso[c] = {max(d.items(), key=lambda x: x[1])[0]}

def hint_for(country, code):
    hint = MANUAL_COUNTRY.get(country) or country_iso.get(country)
    if hint is None and code[:2] in valid_isos:
        hint = {code[:2]}
    if hint is None:
        hint = HINT_EXTRA.get(code[:2])
    return hint

def ok(la, lo, hint):
    if not hint:
        return True
    # 岛国豁免
    for iso in hint:
        if iso in ISLAND_ISOS or iso not in bbox:
            return True
    for iso in hint:
        b = bbox.get(iso)
        if b and b[0] - PAD <= la <= b[2] + PAD and b[1] - PAD <= lo <= b[3] + PAD:
            return True
    return False

out = [rows[0]]
dropped = []
for r in rows[1:]:
    code = r[0].strip().upper()
    hint = hint_for(r[2], code)
    v = r[ci].strip()
    if v and hint:
        la, lo = map(float, v.split(","))
        if not ok(la, lo, hint):
            dropped.append((code, r[1], r[2], v))
            continue
    out.append(r)

with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
    csv.writer(f).writerows(out)

print("边界盒校验: 删除 %d 行, 剩余 %d 行" % (len(dropped), len(out) - 1))
for d in dropped:
    print("   %s | %s | %s | %s" % (d[0], d[1][:30], d[2], d[3]))
