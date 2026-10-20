# -*- coding: utf-8 -*-
"""人工复核修正：
- REVERT: 交叉校验把原本正确的值换错了，恢复原值（原值取自 xcheck_log.txt）；
- SET: 两个值都不可信的行，填入人工核实的正确坐标；
- DROP: 两个值都不可信且无法核实的行，直接删除（保证正确性优先）。
"""
import csv
import re
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

IN_CSV = "港口_航线.csv"
XLOG = "xcheck_log.txt"
CI = 4  # 坐标列下标

REVERT = [
    ("BRMPA", "MACAPA"), ("BRNAT", "NATAL"), ("BRSUA", "SUAPE"),
    ("MYTJM", "TANJONG MANI"), ("MYKPA", "KUALA PAHANG"), ("PHIBL", "ISABEL"),
    ("USPSJ", "PORT ST.JOE"), ("USHUR", "HURON"), ("USNOB", "NORTH BEN"),
    ("VNVIN", "VINH"), ("CNFAN", "FANGCHENG"), ("CNSDG", "SHUIDONG"),
    ("CNTGU", "TANGGU"), ("CNSHD", "SHIDAO"), ("ECLLD", "LA LIBERTAD,EC"),
    ("ECSAL", "SALINAS"), ("AUWSP", "WESTERN PORT"), ("JPSU", "SHIMOTSU"),
    ("CALOU", "LOUISBURG"), ("ESALC", "ALCUDIA"), ("INMAH", "MAHE"),
    ("DKHAS", "HASLE"), ("SELID", "LIDKOPING"), ("GRYER", "YERAKINI"),
    ("TRERE", "EREGLI"), ("YUZEL", "ZELENIKA"), ("CLSVE", "SAN VICENTE"),
    ("DENHS", "NEUHAUS"), ("MZANE", "ANTONIO ENES"), ("JPIRA", "IRAKO"),
]

SET = {
    ("ARCON", "CONCEPCION"): "-32.48500,-58.22800",        # Concepción del Uruguay
    ("ARSFO", "SAN FERNANDO,AR"): "-34.44719,-58.57015",   # San Fernando (Buenos Aires)
    ("BRATU", "ARATU"): "-12.80000,-38.48000",             # Aratu, Bahia
    ("BRSTA", "SANTANA"): "-0.06000,-51.17000",            # Santana, Amapá
    ("COPCO", "PUERTO COLOMBIA"): "10.98913,-74.95541",    # Puerto Colombia, Atlántico
    ("MXSRO", "SANTA ROSALIA"): "27.33907,-112.26775",     # Santa Rosalía, BCS
    ("IDTEL", "TELUKBETUNG"): "-5.45000,105.26667",        # Bandar Lampung
    ("USFER", "FERNDALE"): "48.85000,-122.59000",          # Ferndale, WA
    ("USHOM", "HOME"): "47.27482,-122.76375",              # Home, WA
    ("CNYGS", "YANGSHAN"): "30.64000,122.06000",           # 洋山港
    ("PKQAS", "QASIM"): "24.78000,67.35000",               # Port Qasim
    ("CNSDU", "SANDU"): "26.66000,119.67000",              # 三都澳
    ("CAVAL", "VALLEYFIELD"): "45.25333,-74.13000",        # Valleyfield, QC
}

DROP = [
    ("BRTRO", "TROMBETAS"), ("VEGUA", "GUARAGUAO"), ("VEPMI", "PUERTO MIRANDA"),
    ("CNDQH", "DAQINGHE"), ("CNZGG", "ZHANGGANG"), ("CNZJB", "ZHANGJIABU"),
    ("CNZWN", "ZHANGWAN"), ("DENST", "NEUSTADT"), ("VNPHU", "PHUOC LONG"),
    ("PESNI", "SAN NICOLAS,PE"), ("FRLGU", "LE GUIDO"), ("USBCN", "BUCHANAN,US"),
    ("UZAKA", "AKALTYN"), ("JPYAW", "YAWATA"),
]

# 从日志解析原值
old_val = {}
for line in open(XLOG, encoding="utf-8"):
    m = re.match(r"\[替换\] (\S+) \| ([^|]+) \| [^|]+ \| (\S+) -> ", line)
    if m:
        old_val[(m.group(1), m.group(2).strip())] = m.group(3)

rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
out = [rows[0]]
n_revert = n_set = n_drop = 0
for r in rows[1:]:
    m = re.search(r"（([^（）]+)）", r[1])
    eng = m.group(1) if m else ""
    key = (r[0].strip().upper(), eng)
    if key in REVERT and key in old_val:
        r[CI] = old_val[key]
        n_revert += 1
    elif key in SET:
        r[CI] = SET[key]
        n_set += 1
    elif key in DROP:
        n_drop += 1
        continue
    out.append(r)

with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
    csv.writer(f).writerows(out)
print("修正完成: 恢复原值 %d, 填入核实坐标 %d, 删除 %d, 最终行数 %d"
      % (n_revert, n_set, n_drop, len(out) - 1))
