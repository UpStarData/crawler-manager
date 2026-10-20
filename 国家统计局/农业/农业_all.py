import requests
import csv
import json
import os
import time
from collections import OrderedDict

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://data.stats.gov.cn",
    "Referer": "https://data.stats.gov.cn/dg/website/page.html",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36 Edg/153.0.0.0",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"153\", \"Not_A Brand\";v=\"8\", \"Chromium\";v=\"153\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}
COOKIES = {
    "client_info": "eyJkZXZpY2UiOiJQQyIsImxhbmd1YWdlIjoiemhfQ04iLCJlbmdpbmUiOiJCbGluayIsImJyb3dzZXIiOiJFZGdlIiwib3MiOiJXaW5kb3dzIiwib3NWZXJzaW9uIjoiMTAuMCIsInBsYXRmb3JtIjoiV2luMzIiLCJpc1dlYnZpZXciOmZhbHNlLCJpc0JvdCI6ZmFsc2UsInZlcnNpb24iOiIxNTMuMC4wLjAiLCJjb3JlIjoiQ2hyb21lIiwiY29yZVZlcnNpb24iOiIxNTMuMC4wLjAifQ==",
    "JSESSIONID": "3652C23A3AD8CAC6B05C7F51D3B194DA"
}
URL = 'https://data.stats.gov.cn/dg/website/publicrelease/web/external/stream/esData'
ROOT_ID = "884c062607104a91967b22742537f44f"

PROGRESS_FILE = "progress.json"
INPUT_CSV = "农业子目录cid及指标id.csv"
OUTPUT_DIR = "output"
COMBINED_FILE = "农业数据汇总.csv"


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def parse_catalog(csv_path):
    groups = OrderedDict()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("名称") or "").strip()
            cid = (row.get("cid") or "").strip()
            iid = (row.get("指标id") or "").strip()
            if not name or not cid or not iid:
                continue
            if name not in groups:
                groups[name] = {"cid": cid, "indicatorIds": []}
            if iid not in groups[name]["indicatorIds"]:
                groups[name]["indicatorIds"].append(iid)
    return groups


def fetch_data(cid, indicator_ids, max_retries=5, backoff=2, timeout=30):
    payload = {
        "cid": cid,
        "indicatorIds": indicator_ids,
        "daCatalogId": "",
        "das": [{"text": "全国", "value": "000000000000"}],
        "showType": "1",
        "dts": "",
        "rootId": ROOT_ID
    }
    body_data = json.dumps(payload, ensure_ascii=False).encode('utf-8', 'surrogateescape')

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(URL, headers=HEADERS, cookies=COOKIES,
                                 data=body_data, timeout=timeout)
            resp.raise_for_status()
            res_json = resp.json()
            if res_json.get("success") is not True:
                raise Exception(f"接口业务失败: {res_json}")
            return res_json
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                break
            wait = backoff * (2 ** (attempt - 1))
            print(f"    请求失败，第 {attempt} 次重试，等待 {wait}s，错误：{e}")
            time.sleep(wait)
    raise last_err


def build_rows(resp_json):
    """返回 (header, rows)。header = ['指标', '2018年', '2017年', ...]"""
    data_list = resp_json.get("data", []) or []
    if not data_list:
        return [], []
    header = ["指标"] + [item.get("name", "") for item in data_list]

    indicator_value_map = OrderedDict()
    for year_item in data_list:
        year_name = year_item.get("name", "")
        for val in year_item.get("values", []) or []:
            i_showname = val.get("i_showname", "")
            v = val.get("value", "")
            indicator_value_map.setdefault(i_showname, {})[year_name] = v

    rows = []
    for indicator_name, year_dict in indicator_value_map.items():
        row = [indicator_name] + [year_dict.get(h, "") for h in header[1:]]
        rows.append(row)
    return header, rows


def safe_filename(name):
    return "".join(c if c not in r'\/:*?"<>|' else "_" for c in name)


def save_type_csv(name, header, rows, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{safe_filename(name)}.csv")
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def year_sort_key(y):
    """把 '2018年' 排序成 2018；无法解析的放最后"""
    try:
        return int(str(y).replace("年", "").strip())
    except Exception:
        return -1


def main():
    catalog = parse_catalog(INPUT_CSV)
    print(f"共解析出 {len(catalog)} 个数据类别")

    progress = load_progress()
    done_set = set(progress["done"])
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 用于合并大文件的数据缓冲：每个类型记录 (类别名, header, rows)
    # 关键改动：不强求统一 header，先全部收集，最后做年份并集对齐
    collected = []  # list of (name, header, rows)

    total = len(catalog)
    for idx, (name, info) in enumerate(catalog.items(), 1):
        cid = info["cid"]
        indicator_ids = info["indicatorIds"]
        print(f"\n[{idx}/{total}] 处理：{name}  (cid={cid}, 指标数={len(indicator_ids)})")

        single_path = os.path.join(OUTPUT_DIR, f"{safe_filename(name)}.csv")

        if name in done_set and os.path.exists(single_path):
            print(f"    已完成，跳过（断点续爬）")
            # 从已有单类型 CSV 读回，参与最终合并
            with open(single_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = list(csv.reader(f))
            if reader:
                collected.append((name, reader[0], reader[1:]))
            continue

        try:
            resp_json = fetch_data(cid, indicator_ids)
        except Exception as e:
            print(f"    × 抓取失败（已重试到上限）：{e}")
            continue

        os.makedirs(os.path.join(OUTPUT_DIR, "raw"), exist_ok=True)
        raw_path = os.path.join(OUTPUT_DIR, "raw", f"{safe_filename(name)}.json")
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(resp_json, f, ensure_ascii=False, indent=2)

        header, rows = build_rows(resp_json)
        if not header:
            print(f"    ! 返回数据为空，跳过写出：{name}")
            done_set.add(name)
            progress["done"] = sorted(done_set)
            save_progress(progress)
            continue

        save_type_csv(name, header, rows, OUTPUT_DIR)
        print(f"    √ 已保存：{single_path}  (指标行数={len(rows)})")
        collected.append((name, header, rows))

        done_set.add(name)
        progress["done"] = sorted(done_set)
        save_progress(progress)
        time.sleep(0.5)

    # ============ 合并：动态对齐年份列 ============
    if collected:
        # 1. 收集所有出现过的年份列（去掉首列"指标"）
        all_years = set()
        for _, header, _ in collected:
            for h in header[1:]:
                if h:
                    all_years.add(h)
        # 2. 按年份从大到小排序（没有年份特征的放最后）
        sorted_years = sorted(all_years, key=year_sort_key, reverse=True)
        combined_header = ["类别", "指标"] + sorted_years

        # 3. 逐行写出，缺失年份填 ""
        with open(COMBINED_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(combined_header)
            for name, header, rows in collected:
                # 该类型的 年份 -> 列索引 映射
                year_to_idx = {h: i for i, h in enumerate(header) if i > 0}
                for row in rows:
                    indicator_name = row[0] if row else ""
                    new_row = [name, indicator_name]
                    for y in sorted_years:
                        if y in year_to_idx:
                            idx_val = year_to_idx[y]
                            new_row.append(row[idx_val] if idx_val < len(row) else "")
                        else:
                            new_row.append("")
                    writer.writerow(new_row)

        print(f"\n合并 CSV 已保存：{COMBINED_FILE}")
        print(f"  共 {len(collected)} 个类别，{len(sorted_years)} 个年份列")
    else:
        print("\n没有可写出的数据。")

    print(f"\n完成进度：{len(done_set)}/{total}")


if __name__ == "__main__":
    main()
