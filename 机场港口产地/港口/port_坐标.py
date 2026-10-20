# -*- coding: utf-8 -*-
"""
给 港口_航线.csv 增加坐标列（坐标来自与港口代码同源的 UN/LOCODE 数据集）。

匹配策略（按优先级）：
1. 代码匹配：CSV 的 5 位港口代码与 UN/LOCODE 代码完全一致（并用名称相似度校验）；
2. 名称匹配：取括号内英文名，在 UN/LOCODE 中按名称匹配，
   用代码前两位（ISO 国家码）作国家约束消歧：
   精确 -> 源无坐标(留空) -> 前缀/后缀/模糊(阈值 0.88)；
3. 全局兜底（国家约束失效时，模糊仅限单词查询，阈值 0.9）。

坐标统一转换为十进制度，格式「纬度,经度」，未匹配留空。
"""
import csv
import re
import sys
import io
import tarfile
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher


def fix_stdout():
    if hasattr(sys.stdout, "buffer"):
        try:
            if sys.stdout.encoding.lower() != "utf-8":
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                              errors="replace")
        except Exception:
            pass

TARBALL = "ophpl_unlocode.tar.gz"
IN_CSV = "港口_航线.csv"
COORD_HEADER = "坐标(纬度,经度)"

LOC_RE = re.compile(r"locode:\s*'([A-Z0-9]{2}) ([A-Z0-9]{3})'")
NAMEW_RE = re.compile(r"namewodiacritics:\s*(?:'([^']*)'|(\S[^\n]*))\s*\n")
NAME_RE = re.compile(r"name:\s*(?:'([^']*)'|(\S[^\n]*))\s*\n")
COORD_RE = re.compile(r"coordinates:\s*['\"]?([^'\"\n]+?)['\"]?\s*$", re.M)

GENERIC_PREFIX = {"PUERTO", "PORT", "PUNTA", "SANTA", "CABO", "ISLA", "ISLAS",
                  "CIUDAD", "VILLA", "MONTE", "BAHIA", "SERRA", "PICO", "COSTA"}
STOPWORDS = {"DE", "LA", "LAS", "DEL", "EL", "LOS", "DA", "DAS", "DO", "DOS",
             "OF", "THE", "LE", "DU", "DES", "DI", "Y", "AND", "UND", "D"}
ABBREV = {"ST": "SAINT", "STE": "SAINTE", "MT": "MOUNT", "PT": "PORT"}

# 常见 ISO alpha-3 -> alpha-2（用于剥离名称尾部的 3 字母国家后缀）
ALPHA3 = {
    "ARG": "AR", "BRA": "BR", "URY": "UY", "PRY": "PY", "CHL": "CL", "PER": "PE",
    "ECU": "EC", "COL": "CO", "VEN": "VE", "GUY": "GY", "SUR": "SR", "PAN": "PA",
    "CRI": "CR", "NIC": "NI", "HND": "HN", "GTM": "GT", "BLZ": "BZ", "SLV": "SV",
    "CUB": "CU", "HTI": "HT", "DOM": "DO", "DMA": "DM", "JAM": "JM", "BHS": "BS",
    "BRB": "BB", "TTO": "TT", "PRI": "PR", "USA": "US", "CAN": "CA", "MEX": "MX",
    "AUS": "AU", "NZL": "NZ", "CHN": "CN", "TWN": "TW", "HKG": "HK", "MAC": "MO",
    "JPN": "JP", "KOR": "KR", "PRK": "KP", "VNM": "VN", "THA": "TH", "MYS": "MY",
    "SGP": "SG", "IDN": "ID", "PHL": "PH", "BRN": "BN", "MMR": "MM", "KHM": "KH",
    "LAO": "LA", "IND": "IN", "PAK": "PK", "BGD": "BD", "LKA": "LK", "NPL": "NP",
    "IRN": "IR", "IRQ": "IQ", "SAU": "SA", "ARE": "AE", "QAT": "QA", "KWT": "KW",
    "BHR": "BH", "OMN": "OM", "YEM": "YE", "JOR": "JO", "LBN": "LB", "SYR": "SY",
    "ISR": "IL", "TUR": "TR", "EGY": "EG", "LBY": "LY", "TUN": "TN", "DZA": "DZ",
    "MAR": "MA", "ESP": "ES", "PRT": "PT", "FRA": "FR", "DEU": "DE", "ITA": "IT",
    "NLD": "NL", "BEL": "BE", "LUX": "LU", "CHE": "CH", "AUT": "AT", "GBR": "GB",
    "IRL": "IE", "ISL": "IS", "DNK": "DK", "NOR": "NO", "SWE": "SE", "FIN": "FI",
    "POL": "PL", "CZE": "CZ", "SVK": "SK", "HUN": "HU", "ROU": "RO", "BGR": "BG",
    "GRC": "GR", "HRV": "HR", "SVN": "SI", "SRB": "RS", "MNE": "ME", "ALB": "AL",
    "UKR": "UA", "RUS": "RU", "KAZ": "KZ", "UZB": "UZ", "TKM": "TM", "KGZ": "KG",
    "TJK": "TJ", "AZE": "AZ", "ARM": "AM", "GEO": "GE", "ZAF": "ZA", "NGA": "NG",
    "KEN": "KE", "TZA": "TZ", "MOZ": "MZ", "MDG": "MG", "AGO": "AO", "COD": "CD",
    "COG": "CG", "GAB": "GA", "CMR": "CM", "SEN": "SN", "GIN": "GN", "CIV": "CI",
    "GHA": "GH", "TGO": "TG", "BEN": "BJ", "NER": "NE", "MLI": "ML", "BFA": "BF",
    "LBR": "LR", "SLE": "SL", "GMB": "GM", "GNB": "GW", "CPV": "CV", "MRT": "MR",
    "SDN": "SD", "ETH": "ET", "SOM": "SO", "DJI": "DJ", "ERI": "ER", "MWI": "MW",
    "ZMB": "ZM", "ZWE": "ZW", "BWA": "BW", "NAM": "NA", "SWZ": "SZ", "LSO": "LS",
    "FJI": "FJ", "PNG": "PG", "SLB": "SB", "VUT": "VU", "WSM": "WS", "TON": "TO",
    "TUV": "TV", "KIR": "KI", "NRU": "NR", "PLW": "PW", "FSM": "FM", "MHL": "MH",
}

# 非 ISO 前缀 -> 候选国家集合
HINT_EXTRA = {
    "AC": {"PT", "ES", "CV", "MF", "SX", "GP", "FK", "SH", "BM", "GL"},
    "AN": {"CW", "SX", "BQ", "AW"},
    "BU": {"MM"},
    "ZR": {"CD"},
    "YU": {"RS", "ME"},
    "TP": {"TL"},
    "EL": {"GR"},
}


def norm(s, drop_stop=False):
    """大写、去变音符、缩写展开、只留字母数字。"""
    s = unicodedata.normalize("NFKD", s.upper())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [ABBREV.get(t, t) for t in s.split()]
    if drop_stop:
        toks = [t for t in toks if t not in STOPWORDS]
    return " ".join(toks)


def dms_to_dec(part):
    m = re.match(r"(\d{2,3})(\d{2})(?:\.(\d+))?(\d{0,2})([NSEWnsew])", part)
    if not m:
        return None
    deg = int(m.group(1))
    minute = int(m.group(2))
    minute += float("0." + m.group(3)) if m.group(3) else 0.0
    sec = int(m.group(4)) if m.group(4) else 0
    val = deg + minute / 60.0 + sec / 3600.0
    if m.group(5) in "SWsw":
        val = -val
    return val


def parse_coord(s):
    parts = s.strip().split()
    if len(parts) != 2:
        return None
    lat, lon = dms_to_dec(parts[0]), dms_to_dec(parts[1])
    if lat is None or lon is None:
        return None
    return lat, lon


def ratio(a, b):
    return SequenceMatcher(None, a, b).ratio()


def build_data():
    """解析 YAML。返回 entries(list of dict) 及各索引(存下标)、iso_any_names、
    alias_idx(别名 -> [(iso, 条目下标)])。

    注意：坐标/名称搜索窗口以「下一个 locode:」为界，避免条目自身坐标为
    空时越过条目边界偷到相邻条目的坐标。
    """
    entries = []
    code_info = defaultdict(list)
    by_iso = defaultdict(list)
    exact_idx = defaultdict(list)
    token_idx = defaultdict(list)
    iso_any_names = defaultdict(set)
    aliases = []

    ALIAS_RE = re.compile(
        r"locode:\s*(?:'([A-Z0-9]{2})'|([A-Z0-9]{2}))\s*\n"
        r"(?:[^\n]*\n){0,3}?[ \t]*name:\s*(?:'([^']*)'|(\S[^\n]*))")

    with tarfile.open(TARBALL, "r:gz") as t:
        for member in t.getmembers():
            if not (member.isfile() and member.name.startswith("unlocode-master/data/")
                    and member.name.endswith(".yaml")):
                continue
            iso = member.name.split("/")[-1][:2].upper()
            text = t.extractfile(member).read().decode("utf-8", errors="replace")
            # 别名行：locode: DK / name: 'Copenhagen = København'
            for mm in ALIAS_RE.finditer(text):
                nm = (mm.group(3) or mm.group(4) or "").strip()
                if "=" in nm:
                    a, b = nm.split("=", 1)
                    aliases.append(((mm.group(1) or mm.group(2)).upper(), norm(a), norm(b)))
            for m in LOC_RE.finditer(text):
                # 以下一个 locode: 为边界，防止取到相邻条目的字段
                nxt = text.find("locode:", m.end())
                tail = text[m.end():nxt if nxt != -1 else m.end() + 600]
                nm = NAMEW_RE.search(tail)
                name = (nm.group(1) or nm.group(2) or "").strip() if nm else ""
                if not name:
                    nm2 = NAME_RE.search(tail)
                    name = (nm2.group(1) or nm2.group(2) or "").strip() if nm2 else ""
                if not name:
                    continue
                n = norm(name)
                n3 = norm(name, drop_stop=True)
                iso_any_names[iso].add(n)
                iso_any_names[iso].add(n3)
                cm = COORD_RE.search(tail)
                if not cm or not cm.group(1).strip():
                    continue
                c = parse_coord(cm.group(1))
                if c is None:
                    continue
                i = len(entries)
                entries.append({"iso": iso,
                                "code": (m.group(1) + m.group(2)).upper(),
                                "n": n,
                                "n3": n3,
                                "lat": c[0], "lon": c[1]})
                code_info[entries[i]["code"]].append(i)
                by_iso[iso].append(i)
                exact_idx[entries[i]["n"]].append(i)
                for tok in set(entries[i]["n"].split()):
                    if len(tok) >= 3:
                        token_idx[tok].append(i)

    # 别名 -> 规范条目（别名行自身无坐标，映射到同国家内规范名对应的条目）
    alias_idx = defaultdict(list)
    for iso, a, b in aliases:
        iso_any_names[iso].add(a)
        iso_any_names[iso].add(b)
        for side, other in ((a, b), (b, a)):
            for i in by_iso.get(iso, []):
                if entries[i]["n"] == side:
                    alias_idx[other].append((iso, i))

    print("UN/LOCODE 条目(含坐标): %d, 别名: %d" % (len(entries), len(aliases)))
    return entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx


def score(e, q, q2, q3):
    """给候选条目打分，0 表示不合格。前缀/后缀规则限制候选 token 数不超过查询 +2。"""
    n, n3 = e["n"], e["n3"]
    qt = len(q.split())
    nt = len(n.split())
    if n == q:
        return 1.0
    if q3 and n3 == q3:
        return 0.99
    if q2 and n == q2:
        return 0.98
    if q2 and q3 and n3 == q2:
        return 0.97
    if len(q) >= 4:
        if nt <= qt + 2 and n.startswith(q + " "):
            return 0.95
        if nt >= qt and n.endswith(" " + q):
            return 0.94
    if q2 and len(q2) >= 4:
        q2t = len(q2.split())
        if nt <= q2t + 2 and n.startswith(q2 + " "):
            return 0.93
        if nt >= q2t and n.endswith(" " + q2):
            return 0.92
    if q3 and len(q3) >= 4:
        q3t = len(q3.split())
        n3t = len(n3.split())
        if n3t <= q3t + 2 and n3.startswith(q3 + " "):
            return 0.91
        if n3t >= q3t and n3.endswith(" " + q3):
            return 0.90
    r = ratio(n, q)
    if q2:
        r = max(r, ratio(n, q2))
    if q3:
        r = max(r, ratio(n3, q3))
    return r if r >= 0.88 else 0.0


def pick_best(entries, cands, q, q2, q3):
    """从候选下标集合中挑最高分；返回 (score, [最高分条目])。"""
    best = 0.0
    winners = []
    for i in cands:
        e = entries[i]
        s = score(e, q, q2, q3)
        if s > best:
            best, winners = s, [e]
        elif s == best and s > 0:
            winners.append(e)
    return best, winners


def strip_generic(q):
    toks = q.split()
    if toks and toks[0] in GENERIC_PREFIX and len(toks) > 1:
        return " ".join(toks[1:])
    return ""


def eligible(e, qtok, q2tok):
    """模糊候选的 token 数约束：介于查询 token 数与 +2 之间(对 q2 同规则)。"""
    nt = len(e["n"].split())
    ok = qtok <= nt <= qtok + 2
    if q2tok:
        ok = ok or (q2tok <= nt <= q2tok + 2)
    return ok


def resolve(eng, code, entries, code_info, by_iso, exact_idx, token_idx,
            valid_isos, iso_any_names, alias_idx=None):
    """返回 (lat, lon, tier, iso) 或 (None, None, tier, None)。"""
    alias_idx = alias_idx or {}
    q = norm(eng)
    if not q:
        return None, None, "no_eng", None

    toks = q.split()
    extra_hint = set()
    # 尾部 ",AR" 之类 2 字母国家后缀
    if len(toks) >= 2 and len(toks[-1]) == 2 and toks[-1] in valid_isos:
        extra_hint.add(toks[-1])
        toks = toks[:-1]
    # 尾部 3 字母 ISO 国家后缀，如 SANTA CRUZ(ARG.)
    elif len(toks) >= 3 and toks[-1] in ALPHA3 and len(toks) - 1 >= 2:
        extra_hint.add(ALPHA3[toks[-1]])
        toks = toks[:-1]
    q = " ".join(toks)

    q2 = strip_generic(q)
    q3_toks = [t for t in toks if t not in STOPWORDS]
    q3 = " ".join(q3_toks)
    if q3 == q:
        q3 = ""

    # 1) 代码匹配 + 名称相似度校验
    if code in code_info:
        for i in code_info[code]:
            e = entries[i]
            sim = ratio(e["n"], q)
            if q2:
                sim = max(sim, ratio(e["n"], q2))
            if q3:
                sim = max(sim, ratio(e["n3"], q3))
            if sim >= 0.72:
                return e["lat"], e["lon"], "code", e["iso"]

    # 2) 国家约束内匹配
    hint = set()
    if code[:2] in valid_isos:
        hint = {code[:2]}
    hint |= HINT_EXTRA.get(code[:2], set())
    hint |= extra_hint

    if hint:
        pool = set()
        for iso in hint:
            pool.update(by_iso.get(iso, []))
        # 精确
        exacts = set()
        for nm in (q, q3, q2):
            if nm:
                exacts.update(exact_idx.get(nm, []))
        exacts &= pool
        if exacts:
            best, wins = pick_best(entries, exacts, q, q2, q3)
            if len({(e["lat"], e["lon"]) for e in wins}) == 1:
                return wins[0]["lat"], wins[0]["lon"], "name_exact", wins[0]["iso"]
            return None, None, "ambiguous", None
        # 别名精确匹配（如 Copenhagen -> København）
        for nm in (q, q3, q2):
            if nm:
                for aiso, ai in alias_idx.get(nm, []):
                    if aiso in hint:
                        e = entries[ai]
                        return e["lat"], e["lon"], "name_exact", e["iso"]
        # 名称在约束国家内存在但源数据没有坐标 -> 留空，不再跨国家找同名
        if any(nm in iso_any_names.get(iso, set()) for iso in hint
               for nm in (q, q3, q2) if nm):
            return None, None, "no_coord", None
        # token 预筛 + 打分（含 token 数约束）
        cands = set()
        for tok in set(q.split()):
            if len(tok) >= 3:
                cands.update(token_idx.get(tok, []))
        cands &= pool
        if cands:
            qtok = len(q.split())
            q2tok = len(q2.split()) if q2 else 0
            cands = {i for i in cands if eligible(entries[i], qtok, q2tok)}
            best, wins = pick_best(entries, cands, q, q2, q3)
            if best >= 0.88 and len({(e["lat"], e["lon"]) for e in wins}) == 1:
                return wins[0]["lat"], wins[0]["lon"], "name_fuzzy_hint", wins[0]["iso"]
            if best >= 0.88:
                return None, None, "ambiguous", None
        # hint 范围内全量打分（无公共 token 时兜底）
        best, wins = pick_best(entries, pool, q, q2, q3)
        if best >= 0.88 and len({(e["lat"], e["lon"]) for e in wins}) == 1:
            return wins[0]["lat"], wins[0]["lon"], "name_fuzzy_hint", wins[0]["iso"]

    # 3) 全局兜底（仅当名称在约束国家内完全不存在时才启用）
    exacts = set()
    for nm in (q, q3, q2):
        if nm:
            exacts.update(exact_idx.get(nm, []))
    if exacts:
        best, wins = pick_best(entries, exacts, q, q2, q3)
        if len({(e["lat"], e["lon"]) for e in wins}) == 1:
            return wins[0]["lat"], wins[0]["lon"], "name_exact_global", wins[0]["iso"]
        return None, None, "ambiguous", None
    if len(q.split()) == 1:
        cands = set()
        for tok in set(q.split()):
            if len(tok) >= 3:
                cands.update(token_idx.get(tok, []))
        if cands:
            best, wins = pick_best(entries, cands, q, q2, q3)
            if best >= 0.9 and len({(e["lat"], e["lon"]) for e in wins}) == 1:
                return wins[0]["lat"], wins[0]["lon"], "name_fuzzy_global", wins[0]["iso"]
    return None, None, "blank", None


def main():
    fix_stdout()
    entries, code_info, by_iso, exact_idx, token_idx, iso_any_names, alias_idx = build_data()
    valid_isos = set(by_iso.keys()) | set(iso_any_names.keys())

    rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
    header = rows[0]
    while COORD_HEADER in header:
        idx = header.index(COORD_HEADER)
        header = header[:idx] + header[idx + 1:]
        for r in rows[1:]:
            r.pop(idx)

    tier_cnt = defaultdict(int)
    blanks = []
    fuzzies = []
    out_rows = [header + [COORD_HEADER]]

    for r in rows[1:]:
        code = r[0].strip().upper()
        eng = ""
        m = re.search(r"（([^（）]+)）", r[1])
        if m:
            eng = m.group(1)
        lat, lon, tier, iso = resolve(eng, code, entries, code_info, by_iso,
                                      exact_idx, token_idx, valid_isos,
                                      iso_any_names, alias_idx)
        if lat is not None:
            out_rows.append(r + ["%.5f,%.5f" % (lat, lon)])
        else:
            out_rows.append(r + [""])
            blanks.append((tier, code, eng, r[2], r[3]))
        tier_cnt[tier] += 1
        if tier.startswith("name_fuzzy") and lat is not None:
            fuzzies.append((code, eng, "%.5f,%.5f" % (lat, lon), r[2]))

    # 全部计算完成后再写文件，避免中途出错截断输入
    with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(out_rows)

    total = len(rows) - 1
    filled = total - len(blanks)
    print("\n===== 结果 =====")
    print("总行数 %d, 已填坐标 %d (%.1f%%), 未填 %d"
          % (total, filled, 100.0 * filled / total, total - filled))
    print("各档统计: %s" % dict(tier_cnt))
    print("\n未填行数: %d" % len(blanks))
    for tier, code, eng, country, route in blanks:
        print("   [%s] %s | %s | %s | %s" % (tier, code, eng, country, route))
    print("\n模糊匹配样本(前40):")
    for row in fuzzies[:40]:
        print("   ", row)


if __name__ == "__main__":
    main()
