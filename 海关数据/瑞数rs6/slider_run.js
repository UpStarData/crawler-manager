/**
 * 海关统计平台拼图滑块：在补环境里跑【真实站点 site_captcha.js】，自动通过。
 *
 * 原理：
 *   1. 构造 site_captcha.js 触达的最小 DOM/BOM（createElement/classList/style/canvas/$ ...）
 *   2. hook Math.random 记录随机序列 -> 第 2 个随机数即缺口 x（站点用 Math.random 生成缺口）
 *   3. 按 site_captcha.js 公式反解 moveX = x*270/250（blockLeft=250/270*moveX 需落入 x±10）
 *   4. 生成仿人轨迹（缓动+冲过头+回拉），向 document 派发 mousedown/mousemove/mouseup
 *   5. 站点自身 verify() 判定通过 -> success 回调 -> $("#msg").val("1") -> PASS
 *
 * 用法：node slider_run.js [轮数]
 */
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const ROUNDS = Math.max(1, parseInt(process.argv[2] || "8", 10) || 8);
const TEST_JS = path.join(__dirname, "site_captcha.js");

// ---------- Math.random hook（可复现）----------
const randCalls = [];
let seedState = 0x2545f491;
Math.random = function () {
  seedState = (seedState * 1103515245 + 12345) & 0x7fffffff;
  const v = (seedState & 0xffff) / 0x10000;
  randCalls.push(v);
  return v;
};

// ---------- 最小 DOM 环境 ----------
const allElements = [];
const docHandlers = {};

function makeClassList(el) {
  const set = new Set();
  return {
    add(c) {
      set.add(c);
      el.className = [...set].join(" ");
    },
    remove(c) {
      set.delete(c);
      el.className = [...set].join(" ");
    },
    contains(c) {
      return set.has(c);
    },
  };
}

function makeCtx() {
  return {
    fillStyle: "#000000",
    globalCompositeOperation: "source-over",
    beginPath() {},
    moveTo() {},
    lineTo() {},
    arc() {},
    fill() {},
    clip() {},
    clearRect() {},
    drawImage() {},
    getImageData(x, y, w, h) {
      return { data: new Uint8ClampedArray(Math.max(1, w * h * 4)), width: w, height: h };
    },
    putImageData() {},
  };
}

function makeElement(tag) {
  const el = {
    tagName: String(tag).toUpperCase(),
    className: "",
    id: "",
    innerHTML: "",
    value: "0",
    width: 0,
    height: 0,
    style: {},
    children: [],
    parentNode: null,
    ownerDocument: null,
    _listeners: {},
    classList: null,
    getContext(type) {
      return String(type).toLowerCase() === "2d" ? (el._ctx || (el._ctx = makeCtx())) : null;
    },
    appendChild(child) {
      child.parentNode = el;
      el.children.push(child);
      return child;
    },
    removeChild(child) {
      el.children = el.children.filter((c) => c !== child);
      return child;
    },
    cloneNode() {
      return makeElement(el.tagName.toLowerCase());
    },
    addEventListener(type, fn) {
      (el._listeners[type] || (el._listeners[type] = [])).push(fn);
    },
    removeEventListener() {},
    querySelector() {
      return null;
    },
    getElementsByTagName() {
      return [];
    },
  };
  el.classList = makeClassList(el);
  el.ownerDocument = document;
  if (el.tagName === "IMG") {
    let src = "";
    Object.defineProperty(el, "src", {
      configurable: true,
      get() {
        return src;
      },
      set(v) {
        src = String(v);
        // 真实浏览器图片异步加载；同步触发会踩 initImg 里 const img 的 TDZ
        Promise.resolve().then(() => {
          if (typeof el.onload === "function") el.onload({ type: "load", target: el });
        });
      },
    });
  }
  allElements.push(el);
  return el;
}

const document = {
  createElement: makeElement,
  getElementById(id) {
    if (id === "captcha") return captchaEl;
    return allElements.find((e) => e.id === id) || null;
  },
  addEventListener(type, fn) {
    (docHandlers[type] || (docHandlers[type] = [])).push(fn);
  },
  removeEventListener() {},
};

function docFire(type, event) {
  for (const fn of docHandlers[type] || []) fn(event);
}

const captchaEl = makeElement("div");

// jQuery 桩：只用到 $("#msg").val("1")
const jq = { val: "0" };
function $(sel) {
  return {
    val(v) {
      if (v !== undefined) jq.val = String(v);
      return jq.val;
    },
    click() {},
  };
}

// site_captcha.js 的 IIFE 会执行 window.jigsaw = {...}，底部又裸调 jigsaw.init(...)
// 所以 window 必须就是全局对象，否则裸标识符解析不到
global.window = global;
global.document = document;
global.$ = $;

// ---------- 运行真实 site_captcha.js ----------
const source = fs.readFileSync(TEST_JS, "utf8");
vm.runInThisContext(source, { filename: "site_captcha.js" });
if (typeof global.jigsaw !== "object" || !global.jigsaw.init) {
  throw new Error("site_captcha.js 未暴露 window.jigsaw");
}

// ---------- 轨迹生成（与 slider.py 同款三阶段）----------
function genTrajectory(targetDx) {
  const pts = [];
  let x = 0;
  let t = 0;
  const fastEnd = targetDx * (0.88 + Math.random() * 0.07);
  const steps = 18 + Math.floor(Math.random() * 9);
  for (let i = 1; i <= steps; i++) {
    const p = i / steps;
    const ease = 0.5 - 0.5 * Math.cos(Math.PI * p);
    const nx = fastEnd * ease;
    t += 8 + Math.floor(Math.random() * 15);
    pts.push({ dx: nx - x, dy: Math.random() * 3 - 1.5, t });
    x = nx;
  }
  const overshoot = targetDx + 3 + Math.random() * 3;
  for (let i = 0; i < 2 + Math.floor(Math.random() * 3); i++) {
    const nx = x + (overshoot - x) * 0.6;
    t += 12 + Math.floor(Math.random() * 15);
    pts.push({ dx: nx - x, dy: Math.random() * 2 - 1, t });
    x = nx;
  }
  while (Math.abs(x - targetDx) > 0.4) {
    const nx = x + (targetDx - x) * 0.55;
    t += 15 + Math.floor(Math.random() * 20);
    pts.push({ dx: nx - x, dy: Math.random() * 1.2 - 0.6, t });
    x = nx;
  }
  pts.push({ dx: targetDx - x, dy: 0, t: t + 40 + Math.floor(Math.random() * 50) });
  return pts;
}

// ---------- 单轮求解 ----------
function runRound(idx) {
  const randBefore = randCalls.length;
  const elCountBefore = allElements.length;
  const el = makeElement("div");
  jq.val = "0";
  window.jigsaw.init(el, function () {
    document.getElementById("msg").innerHTML = "验证通过！";
    $("#msg").val("1");
  });

  // rand 顺序：[0]背景图号 [1]缺口x [2]缺口y
  const rx = randCalls[randBefore + 1];
  const x = Math.round(rx * (238 - 72) + 72);
  const moveX = Math.max(0, Math.min(271, Math.round((x * 270) / 250)));

  // 只在【本轮】新建元素里找 slider，避免命中历史实例
  const slider = allElements.slice(elCountBefore).find(
    (e) => e.className === "slider" && e._listeners.mousedown && e._listeners.mousedown.length,
  );
  if (!slider) throw new Error("未找到 slider 元素");

  const x0 = 50;
  const y0 = 30;
  slider._listeners.mousedown[0].call(slider, { x: x0, y: y0 });
  const trail = genTrajectory(moveX);
  // moveX = e.x - originx 是累计偏移，必须发送累计坐标而非每步增量
  let acc = 0;
  for (const p of trail) {
    acc += p.dx;
    docFire("mousemove", { x: x0 + acc, y: y0 + p.dy });
  }
  docFire("mouseup", { x: x0 + acc, y: y0 });

  const blockEl = allElements.slice(elCountBefore).find((e) => e.className === "block");
  if (process.env.SLIDER_DEBUG) {
    console.log(
      `  debug: docMove=${(docHandlers.mousemove || []).length} docUp=${(docHandlers.mouseup || []).length}`,
      `slider.left=${slider.style.left} block.left=${blockEl && blockEl.style.left}`,
      `mask.width=${allElements.slice(elCountBefore).find((e) => e.className.includes("sliderMask") || e.className === "sliderContainer") ? "" : ""}`,
    );
  }

  const passed = jq.val === "1";
  const blockLeft = (250 / 270) * moveX;
  return { idx, x, moveX, blockLeft: blockLeft.toFixed(2), offset: (blockLeft - x).toFixed(2), steps: trail.length, passed };
}

// ---------- 批量验证 ----------
const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const CYAN = "\x1b[36m\x1b[1m";
const RESET = "\x1b[0m";
let ok = 0;
const rows = [];
for (let i = 0; i < ROUNDS; i++) {
  const r = runRound(i);
  if (r.passed) ok++;
  rows.push(r);
}
console.log(`${CYAN}---- 滑块自动求解（真实站点 site_captcha.js）----${RESET}`);
for (const r of rows) {
  console.log(
    `[${r.idx}] x=${r.x} moveX=${r.moveX} blockLeft=${r.blockLeft} offset=${r.offset} steps=${r.steps} -> ${r.passed ? GREEN + "PASS" : RED + "FAIL"}${RESET}`,
  );
}
const allPass = ok === ROUNDS;
console.log(`${allPass ? GREEN : RED}${allPass ? "✅" : "❌"} 通过率: ${ok}/${ROUNDS}${RESET}`);
process.exitCode = allPass ? 0 : 1;
