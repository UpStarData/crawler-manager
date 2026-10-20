# -*- coding: utf-8 -*-
"""恢复边界盒过滤误删的正确行（坐标正确，仅 bmcx 代码前缀误导了国家映射）。"""
import csv
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

IN_CSV = "港口_航线.csv"

RESTORE = [
    ("PKGWA", "瓜德尔（GWADAR）", "巴基斯坦", "印巴线", "25.14798,62.32546"),
    ("FRFUT", "福图纳（FUTUNA ）", "法国", "欧洲内陆点", "-14.27685,-178.14235"),
    ("ARYER", "埃里温 （YEREVAN）", "亚美尼亚", "欧洲内陆点", "40.18333,44.51667"),
    ("MOCHI", "基希讷乌（CHISINAU）", "摩尔多瓦", "欧洲内陆点", "47.01667,28.83333"),
    ("PEPIS", "皮萨瓜（PISAGUA）", "秘鲁", "南美西", "-19.60000,-70.20000"),
    ("MGAHA", "安塔拉哈（ANTALAHA）", "马达加斯加", "东非线", "-14.90123,50.28104"),
    ("SOALU", "阿鲁拉（ALULA）", "索马里", "东非线", "11.96643,50.75715"),
    ("SOBER", "柏培拉（BERBERA）", "索马里", "东非线", "10.43862,45.01340"),
    ("SOBOS", "博萨索（BOSASO）", "索马里", "东非线", "11.27848,49.17995"),
    ("SOKIS", "基斯马尤（KISMAYU）", "索马里", "东非线", "-0.36024,42.54090"),
    ("MEMEL", "梅利利亚（MELILLA）", "梅利利亚", "地西线", "35.31667,-2.88333"),
]

rows = list(csv.reader(open(IN_CSV, encoding="utf-8-sig")))
existing = {(r[0].strip().upper(), r[1]) for r in rows[1:]}
added = 0
for code, name, country, route, coord in RESTORE:
    if (code, name) in existing:
        continue
    rows.append([code, name, country, route, coord])
    added += 1
with open(IN_CSV, "w", newline="", encoding="utf-8-sig") as f:
    csv.writer(f).writerows(rows)
print("恢复 %d 行, 总行数 %d" % (added, len(rows) - 1))
