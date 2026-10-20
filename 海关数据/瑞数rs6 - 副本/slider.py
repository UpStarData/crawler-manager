# -*- coding: utf-8 -*-
"""
海关统计平台拼图滑块（/js/test.js）的自算实现。

test.js 关键逻辑（与站点源码一致）：
    l=42, r=10, w=310, h=155, L=l+r*2=62
    缺口 x = Math.round(rand*(238-72)+72)          # [72, 238]
    缺口 y = Math.round(rand*(83-30)+30)            # [30, 83]
    moveX 约束: 0 <= moveX <= 271                   # moveX+38 < 310
    blockLeft = (310-40-20)/(310-40)*moveX = 250/270*moveX
    校验: spliced = |blockLeft - x| < 10
          TuringTest = mean(trail_y) !== stddev(trail_y)
    通过后仅 $("#msg").val("1")，无任何服务端提交。

本模块职责：
    1. 移植 verify() 判定（含站点 originY 未赋值导致 trail 为 NaN 的忠实复现）
    2. 生成仿人滑动轨迹（缓动 + 回撤 + y 抖动），使 blockLeft 落入缺口 ±10px
    3. solve() 输出：缺口 x、目标 moveX、轨迹点序列、校验结果
"""
import math
import random

# ---- 站点常量（test.js）----
L_SIDE = 42      # 拼图边长 l
R_HOLE = 10      # 圆弧半径 r
W_CANVAS = 310   # 画布宽
H_CANVAS = 155   # 画布高
L_FULL = L_SIDE + R_HOLE * 2          # 62
X_MIN = L_FULL + 10                   # 72
X_MAX = W_CANVAS - (L_FULL + 10)      # 238
Y_MIN = 10 + R_HOLE * 2               # 30
Y_MAX = H_CANVAS - (L_FULL + 10)      # 83
MOVE_X_MAX = W_CANVAS - 38 - 1        # 271（moveX+38 >= w 则拒绝）
BLOCK_RATIO = (W_CANVAS - 40 - 20) / (W_CANVAS - 40)   # 250/270


def block_left(move_x):
    """moveX -> 拼图块实际 left（test.js mousemove 内公式）"""
    return BLOCK_RATIO * move_x


def site_get_random(start, end, rnd=None):
    """移植 getRandomNumberByRange: Math.round(rand*(end-start)+start)"""
    r = rnd() if rnd else random.random()
    return round(r * (end - start) + start)


def verify(trail_y, left, x):
    """
    移植 test.js verify()。
    trail_y 为每次 mousemove push 的 moveY 列表（真实站点因 originY 未赋值全是 NaN）。
    """
    arr = list(trail_y)
    if not arr:
        return {"spliced": False, "TuringTest": False}
    try:
        average = sum(arr) / len(arr)
        deviations = [v - average for v in arr]
        stddev = math.sqrt(sum(d * d for d in deviations))
    except TypeError:
        average = float("nan")
        stddev = float("nan")
    return {
        "spliced": abs(left - x) < 10,
        "TuringTest": average != stddev,   # NaN !== NaN 在 JS/Python 均为 True
    }


def gen_trajectory(target_dx, rnd=None):
    """
    生成仿人滑动轨迹：[(dx, dy, dt_ms), ...]，终点 dx 精确等于 target_dx。
    阶段：缓动推进到 ~92% -> 冲过头 3~6px -> 回拉微调 -> 停稳。
    """
    rnd = rnd or random.random
    points = []
    x = 0.0
    y = 0.0
    t = 0
    fast_end = target_dx * (0.88 + rnd() * 0.07)
    # 阶段1：easeInOut 快推
    steps = random.randint(18, 26)
    for i in range(1, steps + 1):
        p = i / steps
        ease = 0.5 - 0.5 * math.cos(math.pi * p)
        nx = fast_end * ease
        y += rnd() * 2.4 - 1.2
        t += random.randint(8, 22)
        points.append((round(nx - x, 1), round(rnd() * 3 - 1.5, 1), t))
        x = nx
    # 阶段2：冲过头
    overshoot = target_dx + random.uniform(3, 6)
    for _ in range(random.randint(2, 4)):
        nx = x + (overshoot - x) * 0.6
        y += rnd() * 2 - 1
        t += random.randint(12, 26)
        points.append((round(nx - x, 1), round(rnd() * 2 - 1, 1), t))
        x = nx
    # 阶段3：回拉停稳到精确目标
    while abs(x - target_dx) > 0.4:
        nx = x + (target_dx - x) * 0.55
        t += random.randint(15, 35)
        points.append((round(nx - x, 1), round(rnd() * 1.2 - 0.6, 1), t))
        x = nx
    points.append((round(target_dx - x, 1), 0.0, t + random.randint(40, 90)))
    return points


def solve(x=None, seed=None):
    """
    求解一次滑块。
    x: 缺口位置（浏览器侧由 Math.random 生成；补环境/自动化场景可 hook 后传入）。
       缺省时按站点同款随机区间自行生成。
    返回 dict：x / moveX / blockLeft / 轨迹 / verify 结果 / 是否通过。
    """
    rnd = random.Random(seed).random if seed is not None else random.random
    if x is None:
        x = site_get_random(X_MIN, X_MAX, rnd)
    x = int(x)
    if not (X_MIN - 10 <= x <= X_MAX + 10):
        raise ValueError(f"x 超出站点生成范围: {x}")
    # blockLeft = 250/270*moveX 需落入 (x-10, x+10) -> moveX = x*270/250
    move_x = round(x / BLOCK_RATIO)
    move_x = max(0, min(MOVE_X_MAX, move_x))
    trail = gen_trajectory(move_x, rnd)
    # 站点真实 trail_y：originY 未赋值 -> 每次都 push NaN；这里两种口径都验
    trail_y_nan = [float("nan")] * len(trail)
    trail_y_real = [dy for _, dy, _ in trail]
    left = block_left(move_x)
    v_nan = verify(trail_y_nan, left, x)
    v_real = verify(trail_y_real, left, x)
    return {
        "x": x,
        "moveX": move_x,
        "blockLeft": round(left, 2),
        "offset": round(left - x, 2),
        "steps": len(trail),
        "trail": trail,
        "verify_site_nan_trail": v_nan,
        "verify_real_y_trail": v_real,
        "passed": v_nan["spliced"] and v_nan["TuringTest"],
    }


def demo(rounds=20):
    ok = 0
    for i in range(rounds):
        r = solve(seed=i)
        ok += 1 if r["passed"] else 0
        if i < 3:
            print(
                f"[{i}] x={r['x']} moveX={r['moveX']} blockLeft={r['blockLeft']} "
                f"offset={r['offset']:+.2f} steps={r['steps']} "
                f"spliced={r['verify_site_nan_trail']['spliced']} "
                f"turing={r['verify_site_nan_trail']['TuringTest']}"
            )
    # 边界
    for x in (X_MIN, X_MAX):
        r = solve(x=x)
        ok += 1 if r["passed"] else 0
        print(f"[edge] x={x} offset={r['offset']:+.2f} passed={r['passed']}")
    print(f"pass {ok}/{rounds + 2}")


if __name__ == "__main__":
    demo()
