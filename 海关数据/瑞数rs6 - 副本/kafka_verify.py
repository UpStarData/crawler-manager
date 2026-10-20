# -*- coding: utf-8 -*-
"""
Kafka 材料导入主题的投递验证工具（配合 test_db.py 使用）。

用法:
    python kafka_verify.py before   # 记录主题当前末端 offset（写到 runtime/kafka_end_offsets.json）
    python test_db.py               # 跑采集
    python kafka_verify.py after    # 从记录的 offset 起消费，统计本跑新投的消息并抽样展示

连接参数与 test_db.py 顶部 KAFKA_* 保持一致（SASL_PLAINTEXT / PLAIN）。
"""
import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.serializer import Deserializer


class _JsonDeserializer(Deserializer):
    def deserialize(self, topic, data):
        return json.loads(data.decode("utf-8"))

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"
TOPIC = "crawl.backend.material.create"
BOOTSTRAP = ["192.168.1.154:9092"]
SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "PLAIN"
USERNAME = os.getenv("KAFKA_USERNAME", "app")
PASSWORD = os.getenv("KAFKA_PASSWORD", "")
OFFSET_FILE = RUNTIME / "kafka_end_offsets.json"


def make_consumer(**kw):
    base = {
        "bootstrap_servers": BOOTSTRAP,
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": USERNAME,
        "sasl_plain_password": PASSWORD,
    }
    base.update(kw)
    return KafkaConsumer(**base)


def partitions():
    c = make_consumer()
    try:
        parts = sorted(c.partitions_for_topic(TOPIC))
    finally:
        c.close()
    return [TopicPartition(TOPIC, p) for p in parts]


def cmd_before():
    c = make_consumer()
    try:
        tps = partitions()
        ends = c.end_offsets(tps)
    finally:
        c.close()
    snapshot = {str(t.partition): ends[t] for t in tps}
    RUNTIME.mkdir(exist_ok=True)
    OFFSET_FILE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    print(f"已记录 {TOPIC} 末端 offset -> {OFFSET_FILE}")
    print(json.dumps(snapshot, indent=2))


def cmd_after():
    if not OFFSET_FILE.exists():
        print("未找到 offset 快照，请先运行: python kafka_verify.py before")
        sys.exit(1)
    snapshot = {int(k): v for k, v in json.loads(OFFSET_FILE.read_text(encoding="utf-8")).items()}
    c = make_consumer(
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        value_deserializer=_JsonDeserializer(),
    )
    try:
        tps = [tp for tp in partitions() if tp.partition in snapshot and snapshot[tp.partition] > 0]
        c.assign(tps)
        for tp in tps:
            c.seek(tp, snapshot[tp.partition])
        total = 0
        samples = []
        for msg in c:
            v = msg.value or {}
            m = v.get("material") or {}
            total += 1
            if len(samples) < 3:
                samples.append((msg.partition, msg.offset, m.get("materialId"), m.get("title")))
        # 0 偏移起读的分区（跑之前主题为空的情况）单独统计
        for tp in partitions():
            if tp.partition in snapshot and snapshot[tp.partition] == 0:
                c2 = make_consumer(
                    auto_offset_reset="earliest", enable_auto_commit=False,
                    consumer_timeout_ms=10000,
                    value_deserializer=_JsonDeserializer(),
                )
                try:
                    c2.assign([tp])
                    c2.seek_to_beginning(tp)
                    n = 0
                    for msg in c2:
                        n += 1
                        if len(samples) < 3:
                            v = msg.value or {}
                            m = v.get("material") or {}
                            samples.append((msg.partition, msg.offset, m.get("materialId"), m.get("title")))
                    total += n
                    print(f"分区 {tp.partition}（快照 offset=0，从头读）: {n} 条")
                finally:
                    c2.close()
    finally:
        c.close()
    print(f"本次消费到新消息合计: {total} 条")
    for p, off, mid, title in samples:
        print(f"  示例 partition={p} offset={off} materialId={mid} title={title}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "after"
    {"before": cmd_before, "after": cmd_after}[cmd]()
