# -*- coding: utf-8 -*-
"""
扫描 MySQL ap_dev.customs_data，按「材料导入 Kafka 消息契约 specVersion 0.1」逐行打包成
材料消息并投递 Kafka 主题 crawl.backend.material.create（SASL 账号 app）。

契约要点（均已实现）：
    - 一条消息 = 一个 (年月 × 商品) 分组：title 相同的多行数据聚合进同一个 content（records 数组）
    - materialId 为 UUIDv7（RFC 9562，每条消息唯一）
    - 发送前按 contentHash 判重：已在"已发送哈希集合"中的内容不再投递 Kafka
    - 不含 sourceName（按需求移除；注意契约 0.1 原文将其列为必填）
    - url = 抓取该组数据的海关统计列表页 URL（queryDataList 查询参数按采集脚本还原）
    - publishedAt 由数据年月推导（该月 1 日 00:00:00+08:00，ISO 8601 带偏移）
    - capturedAt 带时区偏移（ISO 8601）；信封层与 material 段不夹带未登记字段

去重基线：仅本脚本自己的已发送记录（runtime/sent_content_hashes.json，跨运行持久）。
    注意：Kafka 曾被清空过（2026-10-13），历史发送记录不可作为跳过依据，因此不并入
    老采集脚本的哈希文件、也不从 customs_data_v2 播种——全表内容一律重新投递。
    若以后 Kafka 再被清空，删除 runtime/sent_content_hashes.json 即可全量重发。

用法:
    python customs_to_kafka.py --dry-run     # 只统计不发送（不连 Kafka）
    python customs_to_kafka.py --limit 5     # 测试：只发前 5 条，不记录已发送哈希
    python customs_to_kafka.py               # 正式扫描并发送

依赖: pymysql、kafka-python（仅标准库 + 这两个包）
"""
import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

# ============================ 日志配置 ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("customs_to_kafka")

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"

# ============================ MySQL 配置 ============================
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "ap_dev",
    "charset": "utf8mb4",
}
DB_TABLE = "customs_data"        # 本次扫描的表
# 字段顺序与采集脚本一致（content JSON 的键序由此决定，保证 contentHash 可复现）
DB_COLUMNS = [
    "data_year_month",  # 数据年月 202501
    "goods_code",  # 商品编码
    "goods_name",  # 商品名称
    "trade_partner_code",  # 贸易伙伴编码
    "trade_partner_name",  # 贸易伙伴名称
    "register_area_code",  # 注册地编码
    "register_area_name",  # 注册地名称
    "first_qty",  # 第一数量
    "first_unit",  # 第一计量单位
    "second_qty",  # 第二数量
    "second_unit",  # 第二计量单位
    "rmb_amount",  # 人民币金额
]
NUMERIC_COLS = {"first_qty", "second_qty", "rmb_amount"}
# records 内单行字段：不含与分组头重复的 data_year_month / goods_code / goods_name，
# 也不含 second_qty / second_unit（按需求移除）
RECORD_COLUMNS = [
    "trade_partner_code",
    "trade_partner_name",
    "register_area_code",
    "register_area_name",
    "first_qty",
    "first_unit",
    "rmb_amount",
]

# ============================ Kafka 配置 ============================
KAFKA_BOOTSTRAP = ["192.168.1.154:9092"]
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "app")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_TOPIC = "crawl.backend.material.create"
KAFKA_ACKS = "all"
KAFKA_RETRIES = 3
KAFKA_SEND_TIMEOUT = 30  # 单条发送确认超时（秒）
CHUNK_SIZE = 500  # 分批确认/落盘粒度

# ============================ 材料元数据 ============================
MATERIAL_SOURCE_ID = "customs-trade-stats"
MATERIAL_SOURCE_KIND = "statistics"
MATERIAL_SPEC_VERSION = "0.1"

# 海关统计查询平台列表页（抓取页），URL 按该组查询条件还原
STATS_BASE = "http://stats.customs.gov.cn"
STATS_LIST = STATS_BASE + "/queryData/queryDataList"

SENT_HASH_FILE = RUNTIME / "sent_content_hashes.json"

# 契约 0.1 登记的字段（用于未知字段校验）
ENVELOPE_FIELDS = {"specVersion", "material"}
MATERIAL_FIELDS = {
    "materialId", "sourceId", "sourceName", "sourceKind", "url", "title",
    "contentHash", "content", "publishedAt", "capturedAt",
}
MATERIAL_REQUIRED = {"materialId", "sourceId", "contentHash", "content", "capturedAt"}


# ============================ 基础工具 ============================
def uuidv7():
    """生成 UUIDv7（RFC 9562）：48 位毫秒时间戳 + 随机位，如 0193c3f5-7a1b-7000-8000-xxxxxxxxxxxx"""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = os.urandom(10)
    b = bytearray(16)
    b[0:6] = ms.to_bytes(6, "big")
    b[6] = 0x70 | (rand[0] & 0x0F)  # version 7
    b[7] = rand[1]
    b[8] = 0x80 | (rand[2] & 0x3F)  # variant 10
    b[9:] = rand[3:]
    return str(uuid.UUID(bytes=bytes(b)))


def canon_num(v):
    """数量/金额 -> 规范化数值：整数值取 int，否则 float；兼容 Decimal。
    与采集脚本规则一致，保证 contentHash 可复现。"""
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else v
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return 0


def group_to_content(group):
    """分组 -> (content JSON 字符串, contentHash)。
    records 内每行只含与分组头不重复的字段（RECORD_COLUMNS 顺序），保证 contentHash 可复现。"""
    obj = {
        "data_year_month": group["ym"],
        "goods_code": group["goods_code"],
        "goods_name": group["goods_name"],
        "iEType": "进出口",  # 与采集脚本一致：进出口合计
        "currency": "人民币",  # currencyType=rmb
        "records": [
            {k: canon_num(r.get(k)) if k in NUMERIC_COLS else r.get(k) for k in RECORD_COLUMNS}
            for r in group["records"]
        ],
    }
    content = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return content, "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def year_state(year, month):
    """库选择（与采集脚本一致）：2025年1月及更早 = 历史库(2)；2025年2月起 = 活跃库(1)"""
    if year <= 2024:
        return "2"
    if year == 2025:
        return "2" if month <= 1 else "1"
    return "1"


def build_source_url(ym, goods_code):
    """按该组查询条件还原抓取列表页 URL（与采集脚本 build_form 一致）"""
    year, month = ym[:4], str(int(ym[4:6]))
    params = {
        "pageSize": "200",
        "pageNum": "1",
        "iEType": "10",
        "currencyType": "rmb",
        "year": year,
        "startMonth": month,
        "endMonth": month,
        "monthFlag": "1",
        "unitFlag": "true",
        "unitFlag1": "true",
        "codeLength": "8",
        "outerField1": "CODE_TS",
        "outerValue1": goods_code,
        "outerField2": "ORIGIN_COUNTRY",
        "outerValue2": "",
        "outerField3": "TRADE_CO_PORT",
        "outerValue3": "",
        "outerField4": "",
        "outerValue4": "",
        "orderType": "CODE ASC DEFAULT",
        "selectTableState": year_state(int(year), int(month)),
    }
    return STATS_LIST + "?" + urlencode(params)


def build_material_message(group, content, content_hash):
    """分组 -> 契约信封（specVersion 0.1）。不含 sourceName；url/publishedAt 按需求新增。"""
    ym = group["ym"]
    goods = group["goods_name"] or group["goods_code"] or ""
    material = {
        "materialId": uuidv7(),
        "sourceId": MATERIAL_SOURCE_ID,
        "sourceKind": MATERIAL_SOURCE_KIND,
        "url": build_source_url(ym, group["goods_code"]),
        "title": f"{ym} {goods} 海关进出口统计",
        "contentHash": content_hash,
        "content": content,
        "publishedAt": f"{ym[:4]}-{ym[4:]}-01T00:00:00+08:00",
        "capturedAt": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    return {"specVersion": MATERIAL_SPEC_VERSION, "material": material}


_ISO_OFFSET_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$")


def validate_message(msg):
    """按契约校验清单检查一条消息；返回错误列表（空 = 通过）"""
    errors = []
    if msg.get("specVersion") != MATERIAL_SPEC_VERSION:
        errors.append(f"specVersion 非法: {msg.get('specVersion')!r}")
    unknown_env = set(msg) - ENVELOPE_FIELDS
    if unknown_env:
        errors.append(f"信封层含未登记字段: {sorted(unknown_env)}")
    material = msg.get("material")
    if not isinstance(material, dict):
        errors.append("material 段缺失或非对象")
        return errors
    unknown_mat = set(material) - MATERIAL_FIELDS
    if unknown_mat:
        errors.append(f"material 段含未登记字段: {sorted(unknown_mat)}")
    for field in MATERIAL_REQUIRED:
        if not material.get(field):
            errors.append(f"material.{field} 必填且非空")
    for field in ("capturedAt", "publishedAt"):
        v = material.get(field)
        if v and not _ISO_OFFSET_RE.match(v):
            errors.append(f"material.{field} 非带时区偏移的 ISO 8601: {v!r}")
    return errors


# ============================ 已发送哈希集合 ============================
def load_hash_set(path):
    """读 JSON 数组形式的哈希集合；文件缺失/损坏返回空集"""
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text(encoding="utf-8")))
    except (ValueError, OSError) as exc:
        logger.warning(f"哈希文件读取失败（按空集处理）: {path} [{exc}]")
        return set()


def save_hash_set(path, hashes):
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(sorted(hashes)), encoding="utf-8")


def build_baseline():
    """去重基线 = 本脚本自己的已发送哈希文件（跨运行持久）。
    Kafka 曾被清空过，历史记录（老采集脚本哈希文件 / v2 表）不作为跳过依据。"""
    sent = load_hash_set(SENT_HASH_FILE)
    logger.info(f"去重基线载入 {len(sent)} 条（本脚本已发送记录）")
    return sent


# ============================ Kafka 生产者 ============================
def make_kafka_producer():
    from kafka import KafkaProducer
    from kafka.serializer import Serializer

    class _JsonSerializer(Serializer):
        def serialize(self, topic, headers, obj):
            return json.dumps(obj, ensure_ascii=False).encode("utf-8")

    class _Utf8Serializer(Serializer):
        def serialize(self, topic, headers, data):
            return data.encode("utf-8")

    kw = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "value_serializer": _JsonSerializer(),
        "key_serializer": _Utf8Serializer(),
        "acks": KAFKA_ACKS,
        "retries": KAFKA_RETRIES,
    }
    if KAFKA_USERNAME:
        kw.update({
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    return KafkaProducer(**kw)


def wait_topic_ready(producer, timeout=30):
    """等生产者拿到主题分区元数据（确认主题存在且账号有权限）"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            parts = producer.partitions_for(KAFKA_TOPIC)
            if parts:
                logger.info(f"Kafka 主题 {KAFKA_TOPIC} 分区: {sorted(parts)}")
                return True
        except Exception as exc:
            logger.warning(f"获取主题元数据异常: {type(exc).__name__}: {exc}")
        time.sleep(2)
    return False


# ============================ 主流程 ============================
def scan_and_build(conn, sent):
    """扫描 customs_data：按 (年月, 商品编码) 聚合——title 相同的行并入同一个 content。
    contentHash 已在基线中的分组跳过。返回 (to_send, 总行数, 分组数, 跳过组数)"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(DB_COLUMNS)} FROM {DB_TABLE} ORDER BY id")
        rows = cur.fetchall()
    groups, order = {}, []
    for row in rows:
        rec = dict(zip(DB_COLUMNS, row))
        key = (rec["data_year_month"], rec["goods_code"])
        if key not in groups:
            groups[key] = {"ym": key[0], "goods_code": key[1],
                           "goods_name": rec.get("goods_name") or "", "records": []}
            order.append(key)
        groups[key]["records"].append(rec)
    to_send, seen_this_run = [], set()
    for key in order:
        group = groups[key]
        content, ch = group_to_content(group)
        if ch in sent or ch in seen_this_run:
            continue
        seen_this_run.add(ch)
        to_send.append((build_material_message(group, content, ch), ch))
    return to_send, len(rows), len(groups), len(groups) - len(to_send)


def send_all(producer, to_send, sent, persist=True):
    """分批投递，persist 时每批确认后即时把哈希落盘；返回 (成功数, 失败数)"""
    sent_count = failed = 0
    total = len(to_send)
    for start in range(0, total, CHUNK_SIZE):
        chunk = to_send[start:start + CHUNK_SIZE]
        futures = [
            (msg["material"]["materialId"], ch,
             producer.send(KAFKA_TOPIC, key=msg["material"]["materialId"], value=msg))
            for msg, ch in chunk
        ]
        chunk_ok = 0
        for mid, ch, fut in futures:
            try:
                fut.get(timeout=KAFKA_SEND_TIMEOUT)
                if persist:
                    sent.add(ch)
                sent_count += 1
                chunk_ok += 1
            except Exception as exc:
                failed += 1
                logger.error(f"Kafka 投递失败 materialId={mid} [{type(exc).__name__}: {exc}]")
        if chunk_ok and persist:
            save_hash_set(SENT_HASH_FILE, sent)
        logger.info(f"进度: 已投递 {sent_count}/{total}（本批 {chunk_ok}，失败累计 {failed}）")
    return sent_count, failed


def main():
    parser = argparse.ArgumentParser(description="扫描 customs_data 并按契约投递 Kafka")
    parser.add_argument("--dry-run", action="store_true", help="只统计不发送（不连接 Kafka）")
    parser.add_argument("--limit", type=int, default=0,
                        help="测试模式：只发送前 N 条，不更新已发送哈希记录")
    args = parser.parse_args()

    mode = "【试运行，不发送】" if args.dry_run else (
        f"【测试模式，仅发前 {args.limit} 条】" if args.limit else "")
    logger.info(f"扫描表 {DB_TABLE} -> Kafka {KAFKA_TOPIC}（{KAFKA_SECURITY_PROTOCOL}/{KAFKA_SASL_MECHANISM}）{mode}")

    import pymysql
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        sent = set() if args.limit else build_baseline()
        to_send, total_rows, total_groups, skipped = scan_and_build(conn, sent)
    finally:
        conn.close()

    logger.info(f"扫描结果: 共 {total_rows} 行 → {total_groups} 个分组（同 title 聚合），"
                f"跳过重复 {skipped} 组，待发送 {len(to_send)} 组")

    if args.dry_run:
        for msg, _ in to_send[:3]:
            errors = validate_message(msg)
            status = "校验通过" if not errors else f"校验失败: {errors}"
            print(json.dumps(msg, ensure_ascii=False, indent=2))
            print(f"--- {status}")
        return

    if not to_send:
        logger.info("无待发送消息，结束")
        return

    # 发送前逐条过一遍契约校验（未知字段 / 必填 / 时间格式）
    invalid = []
    for msg, _ in to_send:
        errors = validate_message(msg)
        if errors:
            invalid.append((msg["material"]["materialId"], errors))
    if invalid:
        for mid, errors in invalid[:5]:
            logger.error(f"消息 {mid} 未通过契约校验: {errors}")
        logger.error(f"共 {len(invalid)} 条未通过校验，中止发送")
        sys.exit(1)

    if args.limit:
        to_send = to_send[:args.limit]
        logger.warning(f"测试模式: 仅发送前 {len(to_send)} 条，不记录已发送哈希")
        for msg, _ in to_send:
            print(json.dumps(msg, ensure_ascii=False, indent=2))

    producer = make_kafka_producer()
    try:
        if not wait_topic_ready(producer):
            logger.error(f"30 秒内未取得主题 {KAFKA_TOPIC} 元数据，仍尝试发送（可能失败）")
        sent_count, failed = send_all(producer, to_send, sent, persist=not args.limit)
        producer.flush()
    finally:
        producer.close()

    if args.limit:
        logger.info(f"===== 测试完成: 发送成功 {sent_count}，失败 {failed}（未更新已发送哈希记录）=====")
    else:
        logger.info(f"===== 完成: 发送成功 {sent_count}，失败 {failed}，跳过重复 {skipped}，"
                    f"已发送哈希集合现共 {len(sent)} 条 =====")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
