# -*- coding: utf-8 -*-
"""
海关数据采集后台管理（Flask）：网页上手动点击触发 test_csv.py / test_db.py 取数，
实时看日志、停止任务、下载 CSV。

为什么不用 Scrapy：瑞数 RS6 链路依赖 curl_cffi 的 TLS 指纹模拟 + 在特定步骤调 Node
子进程补环境生成动态参数（cookie 与 XHR 后缀需按发送瞬间取值），Scrapy 的 Twisted
下载器无法复用 curl_cffi 会话，迁移等于重写整个反爬链路且无收益。本后台不改采集
逻辑，以子进程方式运行现有脚本，只负责参数下发、任务排队、日志与结果管理。

并发模型：同一时间只跑一个采集任务（脚本共享 runtime/ 工作文件与判重哈希文件，
并发会互相踩踏），其余任务按提交顺序排队；支持停止运行中任务（连带杀掉 Node 子进程）。

用法:
    python admin_server.py [--host 127.0.0.1] [--port 8000]

依赖: pip install flask
"""
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file

ROOT = Path(__file__).resolve().parent
ADMIN_DIR = ROOT / "admin_runtime"
LOG_DIR = ADMIN_DIR / "logs"
TASK_DIR = ADMIN_DIR / "tasks"
STATE_FILE = ADMIN_DIR / "tasks.json"

# 读取两个入口脚本的默认配置（仅读配置，采集时以子进程运行，互不干扰）
try:
    import test_csv as csvmod  # noqa: E402
    import test_db as dbmod  # noqa: E402
except Exception as exc:  # 依赖缺失时后台仍可启动，配置区显示为空
    print(f"[admin] 警告: 导入采集脚本失败，页面配置为空: {exc}")
    csvmod = dbmod = None

GOODS_NAMES = {
    "08106000": "鲜榴莲",
    "08052110": "焦柑",
    "08052190": "其他柑橘（包括小蜜橘及萨摩蜜柑橘）",
    # "08052200": "克里曼丁橘",
    "08052900": "韦尔金橘及类似的杂交柑橘",
    "08059000": "未列名柑桔属水果",
    "08081000": "鲜苹果",
    "08092100": "鲜欧洲酸樱桃",
    "08092900": "其他鲜樱桃",
    "07096000": "鲜或冷藏的辣椒属及多香果属的果实",
}
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

app = Flask(__name__)
LOCK = threading.RLock()
TASKS = {}           # task_id -> task dict（与 tasks.json 同步）
QUEUE = queue.Queue()  # 待执行任务 id 队列

STATUS_TEXT = {
    "queued": "排队中", "running": "运行中", "succeeded": "成功",
    "failed": "失败", "stopped": "已停止",
}
ACTIVE_STATUS = ("queued", "running")


# ============================ 任务持久化 ============================
def save_state():
    with LOCK:
        try:
            (ADMIN_DIR / "tasks.json").write_text(
                json.dumps(TASKS, ensure_ascii=False, indent=1), encoding="utf-8")
        except OSError:
            pass


def load_state():
    if not STATE_FILE.exists():
        return
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return
    for tid, t in data.items():
        if t.get("status") in ACTIVE_STATUS:
            t["status"] = "failed"
            t["summary"] = "服务重启，任务中断"
            t["finished_at"] = now_iso()
        TASKS[tid] = t


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def new_task_id():
    return f"t{datetime.now():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"


def put_task(task):
    with LOCK:
        TASKS[task["id"]] = task
        save_state()
    QUEUE.put(task["id"])


def get_task(tid):
    with LOCK:
        return TASKS.get(tid)


# ============================ 任务命令与执行 ============================
def norm_proxy(raw):
    """表单代理字段 -> --proxy 参数：
    None/空串 = 不传（用脚本内置隧道）；'direct' = 直连；其余 = 自定义代理 URL"""
    if raw is None or not str(raw).strip():
        return None
    raw = str(raw).strip()
    return "" if raw.lower() == "direct" else raw


def parse_csv_params(data):
    err = None
    try:
        year = int(str(data.get("year", "")).strip())
    except (TypeError, ValueError):
        year = -1
    if not 2000 <= year <= 2100:
        err = "年份必须是 2000-2100 的整数"
    months = str(data.get("months", "")).strip()
    m1 = m2 = 0
    if ":" in months:
        try:
            m1, m2 = (int(x) for x in months.split(":"))
        except ValueError:
            err = "月份格式应为 起月:止月，如 1:12"
    else:
        err = "月份格式应为 起月:止月，如 1:12"
    if not err and not (1 <= m1 <= 12 and 1 <= m2 <= 12 and m1 <= m2):
        err = "月份范围非法（1-12 且起月 <= 止月）"
    goods = ",".join(c.strip() for c in str(data.get("goods", "")).split(",") if c.strip())
    if not goods:
        err = "请填写商品编码"
    ietype = str(data.get("ietype", "10"))
    if ietype not in ("1", "0", "10"):
        err = "iEType 非法"
    page = str(data.get("page", "1")).strip() or "1"
    if not page.isdigit() or int(page) < 1:
        err = "页码必须为正整数"
    page_size = str(data.get("page_size", "200")).strip() or "200"
    if page_size not in ("10", "20", "50", "100", "200"):
        err = "每页条数非法"
    proxy = norm_proxy(data.get("proxy"))
    if err:
        return None, err
    params = {
        "year": str(year), "months": f"{m1}:{m2}", "goods": goods,
        "ietype": ietype, "page": page, "page_size": page_size,
    }
    if proxy is not None:
        params["proxy"] = proxy
    direction = {"1": "进口", "0": "出口", "10": "进出口"}[ietype]
    label = f"单轮查询 {year}年{m1}-{m2}月 {direction} 商品 {goods} 第{page}页"
    return params, label


def parse_db_params(data):
    err = None
    raw_goods = data.get("goods") or []
    if isinstance(raw_goods, str):
        raw_goods = [c.strip() for c in raw_goods.split(",") if c.strip()]
    goods = [c for c in raw_goods if str(c).strip()]
    if not goods:
        err = "至少选择一个商品编码"
    years_text = str(data.get("years", "")).replace("\n", ",")
    segs = []
    for seg in years_text.split(","):
        seg = seg.strip()
        if not seg:
            continue
        try:
            y, m1, m2 = (int(x) for x in seg.split(":"))
        except ValueError:
            err = f"年份段格式错误: {seg!r}（应为 年:起月:止月）"
            break
        if not (1 <= m1 <= 12 and 1 <= m2 <= 12 and m1 <= m2):
            err = f"月份范围非法: {seg!r}"
            break
        segs.append((y, m1, m2))
    if not err and not segs:
        err = "请填写至少一个年份段"
    sink = str(data.get("sink", "both"))
    if sink not in ("kafka", "mysql", "both"):
        err = "SINK_MODE 非法"
    page_size = str(data.get("page_size", "200")).strip() or "200"
    if page_size not in ("20", "50", "100", "200"):
        err = "每页条数非法"
    proxy = norm_proxy(data.get("proxy"))
    if err:
        return None, err
    params = {
        "goods": ",".join(goods),
        "years": ",".join(f"{y}:{m1}:{m2}" for y, m1, m2 in segs),
        "sink": sink, "page_size": page_size,
    }
    if proxy is not None:
        params["proxy"] = proxy
    label = f"批量采集 {len(goods)} 商品 × {len(segs)} 段 × 2 方向（{sink}）"
    return params, label


def build_cmd(kind, params):
    cmd = [sys.executable]
    if kind == "csv":
        cmd += ["test_csv.py"]
        for key in ("year", "months", "goods", "ietype", "page", "page_size"):
            if params.get(key):
                cmd += [f"--{key.replace('_', '-')}", str(params[key])]
    else:
        cmd += ["test_db.py"]
        for key in ("goods", "years", "sink", "page_size"):
            if params.get(key):
                cmd += [f"--{key.replace('_', '-')}", str(params[key])]
    if "proxy" in params:
        cmd += ["--proxy", params["proxy"]]
    return cmd


def summarize(kind, log_text):
    """从日志提取结果摘要（用于任务列表展示）。"""
    text = ANSI_RE.sub("", log_text)
    if kind == "csv":
        m = re.search(r"共\s*(\d+)\s*条数据", text)
        if m:
            return f"查询成功，共 {m.group(1)} 条"
        m = re.search(r"CSV 文件:.*?（(\d+) 行", text)
        if m:
            return f"查询成功，导出 {m.group(1)} 行"
    else:
        m = re.search(r"汇总: 成功 (\d+)/(\d+) 轮，共处理 (\d+) 行", text)
        if m:
            return f"成功 {m.group(1)}/{m.group(2)} 轮，共 {m.group(3)} 行"
        m = re.search(r"失败轮次\((\d+)\)", text)
        if m:
            return f"有 {m.group(1)} 轮失败（详见日志）"
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return lines[-1][:200] if lines else ""


def run_task(tid):
    """工作线程：执行一个任务（串行，同一时间只有一个）。"""
    task = get_task(tid)
    if not task or task["status"] != "queued":
        return
    cmd = build_cmd(task["kind"], task["params"])
    log_path = Path(task["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    task["status"] = "running"
    task["started_at"] = now_iso()
    task["cmd"] = " ".join(cmd)
    save_state()

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    proc = None
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            proc = subprocess.Popen(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT, env=env)
            task["pid"] = proc.pid
            save_state()
            proc.wait()
    except Exception as exc:
        task["summary"] = f"启动失败: {exc}"
        task["status"] = "failed"
    else:
        task["exit_code"] = proc.returncode if proc else -1
        if task["status"] == "stopped":
            task["summary"] = task.get("summary") or "已手动停止"
        elif task["exit_code"] == 0:
            task["status"] = "succeeded"
        else:
            task["status"] = "failed"
    finally:
        task["pid"] = None
        task["finished_at"] = now_iso()
        log_text = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
        if task.get("summary") in (None, ""):
            task["summary"] = summarize(task["kind"], log_text)
            if task["status"] == "failed" and task.get("exit_code") not in (None, 0):
                task["summary"] = f"退出码 {task['exit_code']} | {task['summary']}"
        if task["kind"] == "csv" and task["status"] == "succeeded":
            src = ROOT / "runtime" / "result.csv"
            dst = Path(task["dir"]) / "result.csv"
            if src.exists():
                shutil.copy(src, dst)
                task["csv"] = str(dst)
        save_state()


def worker_loop():
    while True:
        tid = QUEUE.get()
        try:
            run_task(tid)
        except Exception as exc:  # 兜底：任何异常都不能让工作线程死掉
            task = get_task(tid)
            if task:
                task["status"] = "failed"
                task["summary"] = f"内部错误: {exc}"
                task["finished_at"] = now_iso()
                save_state()
        finally:
            QUEUE.task_done()


def stop_process_tree(pid):
    """杀掉进程树（Windows 下连带 Node 子进程）。返回是否真的杀掉了进程。"""
    if os.name == "nt":
        r = subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True, check=False,
        )
        return r.returncode == 0
    import signal
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        return True
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except ProcessLookupError:
            return False


# ============================ 页面与 API ============================
def node_version():
    try:
        out = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except Exception:
        return None


def page_config():
    cfg = {
        "goods_names": GOODS_NAMES,
        "python": sys.version.split()[0],
        "node": node_version(),
        "kafka": None, "mysql": None,
        "csv_defaults": {}, "db_defaults": {},
    }
    if dbmod is not None:
        cfg["db_defaults"] = {
            "goods": list(dbmod.GOODS_CODES),
            "years": "\n".join(f"{y}:{m1}:{m2}" for y, m1, m2 in dbmod.QUERY_YEARS),
            "sink": dbmod.SINK_MODE,
            "page_size": dbmod.PAGE_SIZE,
            "proxy": dbmod.PROXY_URL,
        }
        cfg["kafka"] = {
            "bootstrap": dbmod.KAFKA_BOOTSTRAP,
            "topic": dbmod.KAFKA_TOPIC,
            "user": dbmod.KAFKA_USERNAME,
        }
        cfg["mysql"] = {
            "host": dbmod.MYSQL_CONFIG["host"],
            "port": dbmod.MYSQL_CONFIG["port"],
            "database": dbmod.MYSQL_CONFIG["database"],
            "table": dbmod.DB_TABLE,
        }
    if csvmod is not None:
        f = csvmod.FORM
        cfg["csv_defaults"] = {
            "year": f["year"],
            "months": f"{f['startMonth']}:{f['endMonth']}",
            "goods": f["outerValue1"],
            "ietype": f["iEType"],
            "page": f["pageNum"],
            "page_size": f["pageSize"],
            "proxy": csvmod.PROXY_URL,
        }
    return cfg


@app.get("/")
def index():
    return render_template("index.html", cfg=page_config(), status_text=STATUS_TEXT)


@app.get("/api/config")
def api_config():
    return jsonify(page_config())


@app.get("/api/tasks")
def api_tasks():
    items = []
    with LOCK:
        for t in sorted(TASKS.values(), key=lambda x: x["created_at"], reverse=True):
            items.append({
                "id": t["id"], "kind": t["kind"], "label": t["label"],
                "status": t["status"], "created_at": t["created_at"],
                "started_at": t.get("started_at"), "finished_at": t.get("finished_at"),
                "exit_code": t.get("exit_code"), "summary": t.get("summary", ""),
                "cmd": t.get("cmd", ""), "csv": bool(t.get("csv")),
                "params": t.get("params", {}),
            })
    return jsonify({"tasks": items, "running": current_running()})


@app.post("/api/tasks")
def api_create_task():
    data = request.get_json(silent=True) or {}
    kind = data.get("kind")
    if kind == "csv":
        params, label = parse_csv_params(data)
    elif kind == "db":
        params, label = parse_db_params(data)
    else:
        return jsonify({"error": "kind 必须是 csv 或 db"}), 400
    if params is None:
        return jsonify({"error": label}), 400

    # 相同参数的任务已在排队/运行中 -> 拒绝重复提交
    sig = json.dumps([kind, params], sort_keys=True, ensure_ascii=False)
    with LOCK:
        for t in TASKS.values():
            if t["status"] in ACTIVE_STATUS and t.get("sig") == sig:
                return jsonify({"error": f"相同参数的任务 {t['id']} 已在 {STATUS_TEXT[t['status']]}"}), 409

    tid = new_task_id()
    task = {
        "id": tid, "kind": kind, "label": label, "params": params, "sig": sig,
        "status": "queued", "created_at": now_iso(),
        "started_at": None, "finished_at": None, "exit_code": None,
        "pid": None, "summary": "", "cmd": " ".join(build_cmd(kind, params)),
        "dir": str(TASK_DIR / tid),
        "log_path": str(LOG_DIR / f"{tid}.log"),
        "csv": None,
    }
    (TASK_DIR / tid).mkdir(parents=True, exist_ok=True)
    put_task(task)
    return jsonify({"id": tid, "status": "queued", "label": label}), 202


@app.get("/api/tasks/<tid>")
def api_task_detail(tid):
    t = get_task(tid)
    if not t:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify(t)


@app.get("/api/tasks/<tid>/log")
def api_task_log(tid):
    t = get_task(tid)
    if not t:
        return jsonify({"error": "任务不存在"}), 404
    path = Path(t["log_path"])
    text = ""
    if path.exists():
        text = path.read_text(encoding="utf-8", errors="replace")
    offset = max(0, int(request.args.get("offset", 0)))
    return jsonify({
        "text": text[offset:],
        "offset": len(text),
        "status": t["status"],
    })


@app.post("/api/tasks/<tid>/stop")
def api_task_stop(tid):
    t = get_task(tid)
    if not t:
        return jsonify({"error": "任务不存在"}), 404
    if t["status"] not in ACTIVE_STATUS:
        return jsonify({"error": f"任务已结束（{STATUS_TEXT.get(t['status'], t['status'])}）"}), 400
    if t["status"] == "queued":
        t["status"] = "stopped"
        t["summary"] = "已取消排队"
        t["finished_at"] = now_iso()
        save_state()
        return jsonify({"id": tid, "status": "stopped"})
    # running: 杀进程树，工作线程 wait() 返回后按 stopped 收尾
    t["summary"] = "已手动停止"
    if t.get("pid") and stop_process_tree(t["pid"]):
        t["status"] = "stopped"
    elif not t.get("pid"):
        t["status"] = "stopped"
    # 进程已自然退出（taskkill 未找到进程）时不改状态，交给工作线程按真实结果收尾
    return jsonify({"id": tid, "status": "stopping"})


@app.get("/api/tasks/<tid>/csv")
def api_task_csv(tid):
    t = get_task(tid)
    if not t:
        return jsonify({"error": "任务不存在"}), 404
    path = t.get("csv")
    if not path or not Path(path).exists():
        return jsonify({"error": "该任务没有 CSV 结果（仅成功的单轮查询任务会保留）"}), 404
    return send_file(path, as_attachment=True,
                     download_name=f"海关数据_{t['id']}.csv")


@app.get("/api/stats")
def api_stats():
    sent = 0
    hf = ROOT / "runtime" / "sent_content_hashes.json"
    if hf.exists():
        try:
            sent = len(json.loads(hf.read_text(encoding="utf-8")))
        except (ValueError, OSError):
            pass
    csvf = ROOT / "runtime" / "result.csv"
    csv_info = None
    if csvf.exists():
        st = csvf.stat()
        rows = None
        try:
            with open(csvf, encoding="utf-8-sig", errors="replace") as f:
                rows = max(0, sum(1 for _ in f) - 1)
        except OSError:
            pass
        csv_info = {
            "size": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
            "rows": rows,
        }
    db_rows = None
    if dbmod is not None:
        try:
            import pymysql
            conn = pymysql.connect(connect_timeout=3, **dbmod.MYSQL_CONFIG)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {dbmod.DB_TABLE}")
                    db_rows = int(cur.fetchone()[0])
            finally:
                conn.close()
        except Exception:
            pass
    return jsonify({
        "sent_hashes": sent, "result_csv": csv_info, "db_rows": db_rows,
        "running": current_running(), "queued": QUEUE.qsize(),
    })


def current_running():
    with LOCK:
        for t in TASKS.values():
            if t["status"] == "running":
                return t["id"]
    return None


def main():
    import argparse
    ap = argparse.ArgumentParser(description="海关数据采集后台管理")
    ap.add_argument("--host", default="127.0.0.1", help="监听地址（局域网访问用 0.0.0.0）")
    ap.add_argument("--port", type=int, default=8000, help="监听端口")
    args = ap.parse_args()

    load_state()
    threading.Thread(target=worker_loop, daemon=True, name="crawl-worker").start()
    print(f"[admin] 后台管理已启动: http://{args.host}:{args.port}")
    print(f"[admin] 工作目录: {ROOT}")
    app.run(host=args.host, port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
