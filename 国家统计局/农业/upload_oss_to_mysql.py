# -*- coding: utf-8 -*-
"""把 农业数据汇总.csv 上传到阿里云 OSS,并把得到的 URL 写入 MySQL 表 crawl_data。

独立脚本,不修改原抓取脚本(农业_all.py)。
用法:
    python upload_oss_to_mysql.py --dry-run   # 预演(不真正上传、不写库)
    python upload_oss_to_mysql.py             # 正式上传到 OSS 并写入 MySQL

入库规则:
    只上传汇总文件一个,写入 crawl_data 一行:
        source     = '统计局'
        crawl_time = 汇总文件最后修改时间(即抓取完成时间)
        url        = 统计局数据接口原始链接
        raw_json   = {"农业数据汇总.csv": "<oss url>"}  (key 为文件名称,值为 url)
"""
import os
import sys
import json
import datetime

import pymysql
import oss2

# ==================== OSS 配置 ====================
OSS_CONFIG = {
    "endpoint": "oss-cn-shenzhen.aliyuncs.com",
    "bucket": "apxs-datacenter-public",
    "access_key_id": os.getenv("OSS_ACCESS_KEY_ID", ""),
    "access_key_secret": os.getenv("OSS_ACCESS_KEY_SECRET", ""),
    # 上传到 OSS 后的对象路径(可自行修改目录层级)
    "object_key": "农业/农业数据汇总.csv",
}

# ==================== MySQL 配置 ====================
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "ap_dev",
    "table": "crawl_data",
}

# ==================== 任务参数 ====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE = os.path.join(SCRIPT_DIR, "农业数据汇总.csv")
SOURCE = "统计局"
CRAWL_URL = "https://data.stats.gov.cn/dg/website/publicrelease/web/external/stream/esData"

# bucket 若为私有读,OSS 标准 URL 无法直接访问,可改为 True 使用带签名的 URL(默认有效期 10 年)
USE_SIGNED_URL = False
SIGNED_URL_EXPIRES = 10 * 365 * 24 * 3600


def check_oss_config():
    """OSS 参数仍为占位符时提示并退出。"""
    placeholders = ("your-bucket-name", "your-AccessKeyId", "your-AccessKeySecret")
    missing = [k for k, v in OSS_CONFIG.items()
               if isinstance(v, str) and (not v.strip() or v.strip() in placeholders)]
    if missing:
        print("× OSS 参数未补充完整,请先填写 OSS_CONFIG 中的占位项:", ", ".join(missing))
        sys.exit(1)


def get_crawl_time():
    """以汇总文件的最后修改时间作为抓取完成时间。"""
    mtime = os.path.getmtime(LOCAL_FILE)
    return datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")


def upload_to_oss():
    auth = oss2.Auth(OSS_CONFIG["access_key_id"], OSS_CONFIG["access_key_secret"])
    bucket = oss2.Bucket(auth, OSS_CONFIG["endpoint"], OSS_CONFIG["bucket"])
    key = OSS_CONFIG["object_key"]

    bucket.put_object_from_file(key, LOCAL_FILE)

    if USE_SIGNED_URL:
        url = bucket.sign_url("GET", key, SIGNED_URL_EXPIRES, slash_safe=True)
    else:
        url = f"https://{OSS_CONFIG['bucket']}.{OSS_CONFIG['endpoint']}/{key}"
    return url


def row_exists(conn, crawl_time):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM {table} WHERE source=%s AND crawl_time=%s LIMIT 1".format(
                table=MYSQL_CONFIG["table"]),
            (SOURCE, crawl_time),
        )
        return cur.fetchone() is not None


def insert_row(conn, url, crawl_time):
    raw_json = json.dumps({os.path.basename(LOCAL_FILE): url}, ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO {table} (source, crawl_time, url, raw_json) "
            "VALUES (%s, %s, %s, %s)".format(table=MYSQL_CONFIG["table"]),
            (SOURCE, crawl_time, CRAWL_URL, raw_json),
        )
    conn.commit()


def main():
    dry_run = "--dry-run" in sys.argv

    if not dry_run:
        check_oss_config()
    if not os.path.exists(LOCAL_FILE):
        print(f"× 未找到本地文件:{LOCAL_FILE}")
        sys.exit(1)

    crawl_time = get_crawl_time()
    print(f"本地文件:{LOCAL_FILE}")
    print(f"抓取时间(crawl_time):{crawl_time}")
    print(f"上传对象:oss://{OSS_CONFIG['bucket']}/{OSS_CONFIG['object_key']}")

    if dry_run:
        url = f"https://{OSS_CONFIG['bucket']}.{OSS_CONFIG['endpoint']}/{OSS_CONFIG['object_key']}"
        raw_json = json.dumps({os.path.basename(LOCAL_FILE): url}, ensure_ascii=False)
        print("预演 raw_json:" + raw_json)
        print("预演 SQL:INSERT INTO crawl_data (source, crawl_time, url, raw_json) "
              f"VALUES ('{SOURCE}', '{crawl_time}', '{CRAWL_URL}', <raw_json>)")
        print("\n[--dry-run] 未执行上传与入库,仅预演。")
        return

    url = upload_to_oss()
    print(f"√ 上传成功,URL:{url}")

    conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        charset="utf8mb4",
    )
    try:
        if row_exists(conn, crawl_time):
            print("! 表中已存在相同 source+crawl_time 的记录,跳过插入(避免重复)。")
            return
        insert_row(conn, url, crawl_time)
        print("√ 已写入 MySQL 表 crawl_data")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
