# -*- coding: utf-8 -*-
"""
对 港口_航线.csv 中坐标为空的行，用 Photon(OSM) 地理编码补齐坐标。

- 国家约束：优先用「所在国家(中文) -> ISO」映射（由已匹配行的 ISO 投票得到，
  加上少量手动修正），其次用代码前缀；查询结果按 countrycode 过滤；
- 接受条件：结果名称与查询名称共享长度>=4 的 token，或相似度 >= 0.8（无国家
  约束时 0.85）；
- 结果缓存到 geo_cache.json，可断点续跑；最后写回 CSV 坐标列。
"""
import csv
import json
import os
import re
import sys
import io
import time
from collections import defaultdict
from difflib import SequenceMatcher

import requests

from port_坐标 import (build_data, resolve, norm, HINT_EXTRA, fix_stdout)

IN_CSV = "港口_航线.csv"
CACHE = "geo_cache.json"
COORD_HEADER = "坐标(纬度,经度)"

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36",
    "Accept-Language": "en",
}

# 中文国家名 -> ISO 集合（手动修正：bmcx 前缀与国家不符、或小国家缺投票）
MANUAL_COUNTRY = {
    "中国": {"CN"}, "中国台湾": {"TW"}, "中国香港": {"HK"}, "中国澳门": {"MO"},
    "韩国": {"KR"}, "朝鲜": {"KP"},
    "莱索托": {"LS"}, "斯威士兰": {"SZ"}, "布基纳法索": {"BF"}, "赞比亚": {"ZM"},
    "津巴布韦": {"ZW"}, "马拉维": {"MW"}, "乌干达": {"UG"}, "尼日尔": {"NE"},
    "马里": {"ML"}, "马达加斯加": {"MG"}, "黑山共和国": {"ME"},
    "塞尔维亚和黑山共和国": {"RS"}, "刚果(金)": {"CD"}, "刚果(布)": {"CG"},
    "大西洋群岛": {"PT", "ES", "CV", "MF", "SX", "GP", "FK", "SH", "BM", "GL"},
    "荷属安的列斯": {"CW", "SX", "BQ", "AW"},
    "太平洋群岛": {"FM", "MH", "PW", "MP", "GU", "AS"},
    "圣皮埃尔和密克隆": {"PM"}, "福克兰群岛(马尔维纳斯)": {"FK"},
    "法属圭亚那": {"GF"}, "波多黎各": {"PR"}, "美属萨摩亚": {"AS"},
    "关岛": {"GU"}, "美属维尔京群岛": {"VI"}, "英属维尔京群岛": {"VG"},
    "特克斯和凯科斯群岛": {"TC"}, "开曼群岛": {"KY"}, "百慕大": {"BM"},
    "阿鲁巴": {"AW"}, "库拉索": {"CW"}, "格陵兰": {"GL"}, "法罗群岛": {"FO"},
    "直布罗陀": {"GI"}, "摩纳哥": {"MC"}, "圣赫勒拿": {"SH"},
    "留尼汪(法属)": {"RE"}, "瓜德罗普": {"GP"}, "马提尼克（法属）": {"MQ"},
    "新喀里多尼亚": {"NC"}, "法属波利尼西亚": {"PF"}, "瓦利斯和富图纳": {"WF"},
    "诺福克岛": {"NF"}, "纽埃": {"NU"}, "库克群岛": {"CK"}, "西撒哈拉": {"EH"},
}


def ratio(a, b):
    return SequenceMatcher(None, a, b).ratio()


def load_cache():
    if os.path.exists(CACHE):
        try:
            with open(CACHE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache):
    tmp = CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    os.replace(tmp, CACHE)


# osm_value 等级：city/town/municipality 优先于小村庄等
OSM_RANK = {"city": 6, "town": 5, "municipality": 4, "county": 3, "village": 3,
            "hamlet": 2, "suburb": 1, "neighbourhood": 1, "quarter": 1,
            "region": 0, "state": 0, "island": 0, "islet": 0}


def geo_query_v2(q, hint, limit=30):
    """改进版：候选按 (地点等级, importance) 排序，返回
    (lat, lon, rank, 候选总数) 或 None。国家过滤、名称校验同 geo_query。"""
    for attempt in range(3):
        try:
            r = requests.get("https://photon.komoot.io/api/",
                             params={"q": q, "limit": limit, "lang": "en"},
                             headers=UA, timeout=20)
            if r.status_code == 429:
                time.sleep(5)
                continue
            if r.status_code != 200:
                return None
            feats = r.json().get("features", [])
            if hint:
                hint_low = {h.lower() for h in hint}
                feats = [f for f in feats
                         if (f["properties"].get("countrycode") or "").lower() in hint_low]
            if not feats:
                return None
            # 按等级+importance 选最佳候选
            best = None
            best_key = (-1, -1)
            for f in feats:
                p = f["properties"]
                rk = OSM_RANK.get(p.get("osm_value"), 0) if p.get("osm_key") == "place" else 0
                imp = 0.0
                try:
                    imp = float(p.get("importance") or 0)
                except (TypeError, ValueError):
                    pass
                if (rk, imp) > best_key:
                    best_key = (rk, imp)
                    best = f
            p = best["properties"]
            g = best.get("geometry") or {}
            coords = g.get("coordinates")
            if not coords or len(coords) != 2:
                return None
            lat, lon = coords[1], coords[0]
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                return None
            nm = norm(" ".join(x for x in (p.get("name"), p.get("city")) if x))
            qn = norm(q)
            toks = [t for t in qn.split() if len(t) >= 4]
            ok = any(t in nm for t in toks) or ratio(qn, nm) >= (0.8 if hint else 0.85)
            if not ok:
                return None
            return lat, lon, best_key[0], len(feats)
        except Exception:
            time.sleep(2)
    return None


def geo_query(q, hint):
    """旧版（取第一名）——保留给 geo_fill 主流程，返回 'lat,lon' 或 None。"""
    for attempt in range(3):
        try:
            r = requests.get("https://photon.komoot.io/api/",
                             params={"q": q, "limit": 20, "lang": "en"},
                             headers=UA, timeout=20)
            if r.status_code == 429:
                time.sleep(5)
                continue
            if r.status_code != 200:
                return None
            feats = r.json().get("features", [])
            if hint:
                hint_low = {h.lower() for h in hint}
                feats = [f for f in feats
                         if (f["properties"].get("countrycode") or "").lower() in hint_low]
            if not feats:
                return None
            p = feats[0]["properties"]
            g = feats[0].get("geometry") or {}
            coords = g.get("coordinates")
            if not coords or len(coords) != 2:
                return None
            lat, lon = coords[1], coords[0]
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                return None
            nm = norm(" ".join(x for x in (p.get("name"), p.get("city")) if x))
            qn = norm(q)
            toks = [t for t in qn.split() if len(t) >= 4]
            ok = any(t in nm for t in toks) or ratio(qn, nm) >= (0.8 if hint else 0.85)
            if not ok:
                return None
            return "%.5f,%.5f" % (lat, lon)
        except Exception:
            time.sleep(2)
    return None


def main():
    fix_stdout()
    entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx = build_data()
    valid_isos = set(by_iso.keys()) | set(iso_any_names.keys())

    rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
    header = rows[0]
    cidx = header.index(COORD_HEADER) if COORD_HEADER in header else None

    # 1) 中文国家名 -> ISO 投票（用可靠档位的匹配结果）
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
    print("投票得到国家映射 %d 个, 手动 %d 个" % (len(country_iso), len(MANUAL_COUNTRY)))

    # 2) 补地理编码
    cache = load_cache()
    filled = failed = 0
    todo = []
    for r in rows[1:]:
        if cidx is not None and len(r) > cidx and r[cidx].strip():
            continue
        m = re.search(r"（([^（）]+)）", r[1])
        if not m:
            continue
        todo.append((r[0].strip().upper(), m.group(1), r[2]))

    print("待补坐标行数: %d (缓存已有 %d)" % (len(todo), len(cache)))
    for n, (code, eng, country) in enumerate(todo, 1):
        key = code + "|" + eng
        if cache.get(key):
            continue
        hint = MANUAL_COUNTRY.get(country) or country_iso.get(country)
        if hint is None and code[:2] in valid_isos:
            hint = {code[:2]}
        if hint is None:
            hint = HINT_EXTRA.get(code[:2])
        q = norm(eng)
        val = geo_query(q, hint)
        cache[key] = val or ""
        if val:
            filled += 1
        else:
            failed += 1
        if n % 25 == 0:
            save_cache(cache)
            print("   进度 %d/%d, 本次成功 %d, 失败 %d" % (n, len(todo), filled, failed))
            sys.stdout.flush()
        time.sleep(0.6)
    save_cache(cache)
    print("地理编码完成: 成功 %d, 失败 %d" % (filled, failed))

    # 3) 写回 CSV
    new_rows = [header]
    changed = 0
    for r in rows[1:]:
        row = list(r)
        while len(row) < len(header):
            row.append("")
        if cidx is not None and row[cidx].strip():
            new_rows.append(row)
            continue
        m = re.search(r"（([^（）]+)）", r[1])
        if m:
            key = r[0].strip().upper() + "|" + m.group(1)
            if cache.get(key):
                row[cidx] = cache[key]
                changed += 1
        new_rows.append(row)
    with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(new_rows)

    empty = sum(1 for r in new_rows[1:] if not r[cidx].strip())
    print("\n写回完成: 本次新增 %d 行坐标; 全部 %d 行中仍有 %d 行坐标为空"
          % (changed, len(new_rows) - 1, empty))


if __name__ == "__main__":
    main()
