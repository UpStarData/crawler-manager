# -*- coding: utf-8 -*-
"""
最终整理 港口_航线.csv：
1. 坐标为空的行，用 geo_cache.json 中已有的 Photon 结果补齐；
2. 对所有有值行做 Photon v2 交叉校验（按地点等级选候选），
   仅当高置信（city/town/municipality 级，或国家内唯一候选）且距离 >100km 时替换；
3. 过滤掉最终仍无坐标的行（只保留有坐标、且坐标可信的港口）；
4. 明细写 xcheck_log.txt，进度缓存 xcheck_cache.json。
"""
import csv
import json
import math
import os
import re
import sys
import io
import time
from collections import defaultdict

from port_坐标 import build_data, resolve, norm, HINT_EXTRA, fix_stdout
from geo_fill import geo_query_v2, MANUAL_COUNTRY

IN_CSV = "港口_航线.csv"
COORD_HEADER = "坐标(纬度,经度)"
XLOG = "xcheck_log.txt"

UN_TIERS = {"code", "name_exact", "name_fuzzy_hint", "name_exact_global", "name_fuzzy_global"}


def hav(a, b):
    la1, lo1, la2, lo2 = map(math.radians, a + b)
    dlat, dlon = la2 - la1, lo2 - lo1
    h = math.sin(dlat / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin(dlon / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def load_json(path):
    if os.path.exists(path):
        try:
            return json.load(open(path, encoding="utf-8"))
        except Exception:
            return {}
    return {}


def main():
    fix_stdout()
    entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx = build_data()
    valid_isos = set(by_iso.keys()) | set(iso_any_names.keys())

    rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
    header = rows[0]
    ci = header.index(COORD_HEADER)

    geo_cache = load_json("geo_cache.json")
    xcache = load_json("xcheck_cache.json")

    # 中文国家名 -> ISO 投票
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

    logf = open(XLOG, "w", encoding="utf-8")
    out_rows = [header]
    kept = geo_filled = replaced = skipped_lowconf = dropped = 0
    for r in rows[1:]:
        code = r[0].strip().upper()
        m = re.search(r"（([^（）]+)）", r[1])
        eng = m.group(1) if m else ""
        cur = r[ci].strip() if len(r) > ci else ""

        # 1) 空行先用 geo 缓存补
        if not cur and eng:
            gv = geo_cache.get(code + "|" + eng)
            if gv:
                cur = gv
                geo_filled += 1

        # 2) 所有有值行做 v2 交叉校验
        if cur and eng:
            hint = MANUAL_COUNTRY.get(r[2]) or country_iso.get(r[2])
            if hint is None and code[:2] in valid_isos:
                hint = {code[:2]}
            if hint is None:
                hint = HINT_EXTRA.get(code[:2])
            key = code + "|" + eng
            if key not in xcache:
                res = geo_query_v2(norm(eng), hint)
                xcache[key] = list(res) if res else []
                if len(xcache) % 25 == 0:
                    with open("xcheck_cache.json", "w", encoding="utf-8") as f:
                        json.dump(xcache, f, ensure_ascii=False)
                time.sleep(0.35)
            pv = xcache[key]
            if pv and len(pv) == 4:
                plat, plon, prank, pcnt = pv
                clat, clon = map(float, cur.split(","))
                d = hav((clat, clon), (plat, plon))
                confident = prank >= 4 or pcnt == 1
                if d > 100:
                    if confident:
                        logf.write("[替换] %s | %s | %s | %s -> %.5f,%.5f (%.0f km, rank=%d, cnt=%d)\n"
                                   % (code, eng, r[2], cur, plat, plon, d, prank, pcnt))
                        cur = "%.5f,%.5f" % (plat, plon)
                        replaced += 1
                    else:
                        logf.write("[跳过-低置信] %s | %s | %s | %s vs %.5f,%.5f (%.0f km, rank=%d, cnt=%d)\n"
                                   % (code, eng, r[2], cur, plat, plon, d, prank, pcnt))
                        skipped_lowconf += 1
                elif d > 40:
                    logf.write("[接近] %s | %s | %s | %s vs %.5f,%.5f (%.0f km)\n"
                               % (code, eng, r[2], cur, plat, plon, d))

        # 3) 仍无坐标 -> 丢弃
        if not cur:
            dropped += 1
            continue
        row = list(r)
        while len(row) <= ci:
            row.append("")
        row[ci] = cur
        out_rows.append(row)
        kept += 1

    with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(out_rows)
    with open("xcheck_cache.json", "w", encoding="utf-8") as f:
        json.dump(xcache, f, ensure_ascii=False)
    logf.close()

    print("\n整理完成: 保留 %d 行, 替换 %d, 低置信跳过 %d, geo缓存补 %d, 丢弃 %d"
          % (kept, replaced, skipped_lowconf, geo_filled, dropped))


if __name__ == "__main__":
    main()
