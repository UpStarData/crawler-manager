# -*- coding: utf-8 -*-
"""最终 CSV 校验：行数、航线统计、已知港口坐标抽查、格式检查。"""
import csv
import sys
import io
import re
from collections import Counter

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

rows = list(csv.reader(open("港口_航线.csv", encoding="utf-8-sig")))
header = rows[0]
print("表头:", header)
ci = header.index("坐标(纬度,经度)")
data = rows[1:]

print("\n== 格式检查 ==")
bad = 0
for r in data:
    v = r[ci].strip()
    if not re.match(r"^[-]?\d+\.\d+,[-]?\d+\.\d+$", v):
        print("   格式异常:", r[0], r[1], repr(v))
        bad += 1
    else:
        lat, lon = map(float, v.split(","))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            print("   范围异常:", r[0], r[1], v)
            bad += 1
print("   异常数:", bad)

print("\n== 航线统计 ==")
cnt = Counter(r[3] for r in data)
for rt, n in sorted(cnt.items(), key=lambda x: -x[1]):
    print("   %-12s %d" % (rt, n))
print("   总行数: %d, 覆盖航线: %d" % (len(data), len(cnt)))

print("\n== 已知港口抽查 ==")
known = {
    "CNSHA": (30.5, 32.5), "NLRTM": (51.5, 52.5), "DEHAM": (53.0, 54.0),
    "GBLON": (51.0, 52.0), "AUMEL": (-38.5, -37.0), "JPYOK": (34.5, 36.5),
    "HKHKG": (21.5, 23.0), "ZADUR": (-30.5, -29.0), "SGSIN": (0.5, 2.0),
    "USNYC": (40.0, 41.5), "BRSSZ": (-24.5, -23.0), "ARENA": (-35.5, -33.5),
    "JPNAG": (34.5, 36.0), "KEMMB": (-5.0, -3.0), "MYLPK": (2.5, 4.5),
    "THBKK": (13.0, 14.5), "CNTXG": (38.5, 39.5), "RULED": (59.0, 61.0),
    "USDAL": (32.0, 33.5), "JPKUR": (33.5, 35.0), "CNYAN": (37.0, 38.5),
    "AZBAK": (40.0, 41.0), "DKCOP": (55.0, 56.5), "DEMUN": (47.5, 49.0),
    "NZWHA": (-36.5, -34.5), "PHSUR": (9.0, 10.5), "USKOD": (57.0, 58.5),
    "EGSUE": (29.0, 30.5), "LBSOU": (32.5, 34.0), "ZMLUN": (-16.0, -14.5),
}
for code, (lo, hi) in known.items():
    hit = [r for r in data if r[0] == code]
    if not hit:
        print("   %s: 已被过滤(无坐标)" % code)
        continue
    v = hit[0][ci]
    lat = float(v.split(",")[0])
    flag = "OK" if lo <= lat <= hi else "!!超出范围"
    print("   %s %-20s %s %s" % (code, hit[0][1][:20], v, flag))
