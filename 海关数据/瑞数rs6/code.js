const fs = require("fs");
const path = require("path");
const vm = require("vm");
const nodeCrypto = require("crypto");
const NODE_LEAKS = [
  "process",
  "Buffer",
  "require",
  "module",
  "exports",
  "__dirname",
  "__filename",
  "setImmediate",
  "clearImmediate",
  "GLOBAL",
  "root",
  "gc",
  "global",
];

const hiddenNodeDescriptors = [];

function hideNodeLeaks() {
  hiddenNodeDescriptors.length = 0;
  for (const name of NODE_LEAKS) {
    try {
      const desc = Object.getOwnPropertyDescriptor(global, name);
      if (!desc) continue;
      hiddenNodeDescriptors.push([name, desc]);
      delete global[name];
    } catch (err) {}
  }
}

function restoreNodeLeaks() {
  for (const [name, desc] of hiddenNodeDescriptors) {
    try {
      Object.defineProperty(global, name, desc);
    } catch (err) {
      try {
        global[name] = desc.value;
      } catch (inner) {}
    }
  }
  hiddenNodeDescriptors.length = 0;
}

const ROOT = __dirname;
const RTPROXY_PATH = path.join(ROOT, "rtproxy.js");
const HTTP_PACKET_DIR = path.join(ROOT, "ruyitrace", "http_packet");
const SITE_ORIGIN = "http://stats.customs.gov.cn";
const DEFAULT_PAGE_URL = SITE_ORIGIN + "/";
const DEFAULT_TARGET_PATH = "/queryData/getQueryDataListByWhere";
const QUERY_NAME = "8h6a7FPl";
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0";
const HTTPONLY_COOKIES = new Set(["AV7KYchI7HHaS", "__jsluid_h", "JSESSIONID"]);

function readText(file) {
  return fs.readFileSync(file, "utf8");
}

function findPacketFile(prefix) {
  if (!fs.existsSync(HTTP_PACKET_DIR)) {
    throw new Error("packet dir missing, pass pageHtmlPath/staticSourcePath");
  }
  const file = fs.readdirSync(HTTP_PACKET_DIR).find((name) => name.startsWith(prefix));
  if (!file) {
    throw new Error("packet not found: " + prefix);
  }
  return path.join(HTTP_PACKET_DIR, file);
}

function readPacket(prefix) {
  return JSON.parse(readText(findPacketFile(prefix)));
}

function packetText(prefix) {
  const packet = readPacket(prefix);
  return packet.responseBody && packet.responseBody.text ? packet.responseBody.text : "";
}

const rtproxySource = readText(RTPROXY_PATH);
const rtproxyPrefix = rtproxySource.split("// 目标js")[0];
vm.runInThisContext(rtproxyPrefix, { filename: RTPROXY_PATH });

const nativeList = [];
const rtLogRing = [];
const baseRtLog = rt_log;
rt_log = function rtLogCapture() {
  const args = Array.from(arguments).map((item) => {
    if (typeof item === "function") return "[function " + (item.name || "anonymous") + "]";
    if (item && typeof item === "object") {
      if (item.tagName) return "[element " + item.tagName + "]";
      if (item.type) return "[event " + item.type + "]";
      return "[object]";
    }
    return item;
  });
  rtLogRing.push(args);
  if (rtLogRing.length > 1200) rtLogRing.shift();
  return baseRtLog.apply(this, arguments);
};

function protect(fn) {
  if (typeof fn === "function") {
    fun_to_native(fn);
    nativeList.push(fn.name || "(anonymous)");
  }
  return fn;
}

function Node() {}
function Element() {}
function HTMLElement() {}
function HTMLHtmlElement() {}
function HTMLHeadElement() {}
function HTMLBodyElement() {}
function HTMLMetaElement() {}
function HTMLScriptElement() {}
function HTMLAnchorElement() {}
function HTMLFormElement() {}
function HTMLInputElement() {}
function HTMLDivElement() {}
function HTMLCanvasElement() {}
function HTMLIFrameElement() {}
function HTMLAudioElement() {}
function HTMLVideoElement() {}
function CanvasRenderingContext2D() {}
function BarProp() {}
function IDBFactory() {}
function IDBRequest() {}
function IDBOpenDBRequest() {}
function IDBDatabase() {}
function IDBTransaction() {}
function IDBObjectStore() {}
function DOMStringList() {}
function External() {}
function LockManager() {}

for (const ctor of [
  Node,
  Element,
  HTMLElement,
  HTMLHtmlElement,
  HTMLHeadElement,
  HTMLBodyElement,
  HTMLMetaElement,
  HTMLScriptElement,
  HTMLAnchorElement,
  HTMLFormElement,
  HTMLInputElement,
  HTMLDivElement,
  HTMLCanvasElement,
  HTMLIFrameElement,
  HTMLAudioElement,
  HTMLVideoElement,
  CanvasRenderingContext2D,
  BarProp,
  IDBFactory,
  IDBRequest,
  IDBOpenDBRequest,
  IDBDatabase,
  IDBTransaction,
  IDBObjectStore,
  DOMStringList,
  External,
  LockManager,
]) {
  protect(ctor);
}
Object.setPrototypeOf(Element.prototype, Node.prototype);
Object.setPrototypeOf(HTMLElement.prototype, Element.prototype);
for (const ctor of [
  HTMLHtmlElement,
  HTMLHeadElement,
  HTMLBodyElement,
  HTMLMetaElement,
  HTMLScriptElement,
  HTMLAnchorElement,
  HTMLFormElement,
  HTMLInputElement,
  HTMLDivElement,
  HTMLCanvasElement,
  HTMLIFrameElement,
  HTMLAudioElement,
  HTMLVideoElement,
]) {
  Object.setPrototypeOf(ctor.prototype, HTMLElement.prototype);
  Object.defineProperty(ctor.prototype, Symbol.toStringTag, { value: ctor.name });
}
Object.defineProperty(CanvasRenderingContext2D.prototype, Symbol.toStringTag, { value: "CanvasRenderingContext2D" });
Object.defineProperty(Node.prototype, Symbol.toStringTag, { value: "Node" });
Object.defineProperty(Element.prototype, Symbol.toStringTag, { value: "Element" });
Object.defineProperty(HTMLElement.prototype, Symbol.toStringTag, { value: "HTMLElement" });
Object.defineProperty(BarProp.prototype, Symbol.toStringTag, { value: "BarProp" });
Object.setPrototypeOf(IDBOpenDBRequest.prototype, IDBRequest.prototype);
for (const ctor of [IDBFactory, IDBRequest, IDBOpenDBRequest, IDBDatabase, IDBTransaction, IDBObjectStore, DOMStringList, External, LockManager]) {
  Object.defineProperty(ctor.prototype, Symbol.toStringTag, { value: ctor.name });
}

const ELEMENT_CTORS = {
  HTML: HTMLHtmlElement,
  HEAD: HTMLHeadElement,
  BODY: HTMLBodyElement,
  META: HTMLMetaElement,
  SCRIPT: HTMLScriptElement,
  A: HTMLAnchorElement,
  FORM: HTMLFormElement,
  INPUT: HTMLInputElement,
  DIV: HTMLDivElement,
  CANVAS: HTMLCanvasElement,
  IFRAME: HTMLIFrameElement,
  AUDIO: HTMLAudioElement,
  VIDEO: HTMLVideoElement,
};

function parseCookieHeader(header) {
  const jar = Object.create(null);
  if (!header) return jar;
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const name = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (name) jar[name] = value;
  }
  return jar;
}

function getHeader(packet, name) {
  const headers = packet.requestHeaders || [];
  const hit = headers.find((item) => item.name.toLowerCase() === name.toLowerCase());
  return hit ? hit.value : "";
}

function parseCookieSet(raw) {
  const first = String(raw || "").split(";")[0];
  const idx = first.indexOf("=");
  if (idx <= 0) return null;
  return {
    name: first.slice(0, idx).trim(),
    value: first.slice(idx + 1).trim(),
  };
}

function serializeCookies(visibleJar, httpOnlyJar) {
  const merged = Object.assign(Object.create(null), httpOnlyJar || {}, visibleJar || {});
  return Object.keys(merged)
    .filter((key) => merged[key] !== undefined && merged[key] !== null)
    .map((key) => key + "=" + merged[key])
    .join("; ");
}

function parseMetaElements(html) {
  const metas = [];
  const re = /<meta\b[^>]*>/gi;
  let match;
  while ((match = re.exec(html))) {
    const tag = match[0];
    const attrs = Object.create(null);
    const attrRe = /([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g;
    let attr;
    while ((attr = attrRe.exec(tag))) {
      attrs[attr[1]] = attr[2] !== undefined ? attr[2] : attr[3] !== undefined ? attr[3] : attr[4];
    }
    metas.push(makeElement("meta", attrs));
  }
  return metas;
}

function getDynamicMetaContent(metas) {
  const hit = metas.find((item) => item.id === "U07cUYgw9lbI" || (item.getAttribute && item.getAttribute("r") === "m"));
  return hit ? hit.content || "" : "";
}

function extractRsScripts(html) {
  const scripts = [];
  const cleaned = String(html || "").replace(/<!--[\s\S]*?-->/g, "");
  const re = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
  let match;
  while ((match = re.exec(cleaned))) {
    const attrs = match[1] || "";
    if (!/\br\s*=\s*['"]m['"]/i.test(attrs)) continue;
    const srcMatch = /\bsrc\s*=\s*['"]([^'"]+)['"]/i.exec(attrs);
    if (srcMatch) {
      scripts.push({ type: "external", src: srcMatch[1] });
    } else {
      scripts.push({ type: "inline", source: match[2] || "" });
    }
  }
  return scripts;
}

function extractStaticScriptSrc(html) {
  const hit = extractRsScripts(html).find((item) => item.type === "external");
  return hit ? hit.src : "";
}

function resolveUrl(url) {
  return new URL(url, SITE_ORIGIN + "/");
}

function makeStyle() {
  return rtwatch(
    {
      getPropertyValue: protect(function getPropertyValue(name) {
        rt_log("CSSStyleDeclaration.getPropertyValue", name);
        return "";
      }),
      setProperty: protect(function setProperty(name, value) {
        rt_log("CSSStyleDeclaration.setProperty", name, value);
      }),
      removeProperty: protect(function removeProperty(name) {
        rt_log("CSSStyleDeclaration.removeProperty", name);
        return "";
      }),
    },
    "CSSStyleDeclaration"
  );
}

function makeElement(tagName, attrs = {}) {
  const upper = String(tagName || "").toUpperCase();
  const listeners = Object.create(null);
  const element = {
    nodeType: 1,
    nodeName: upper,
    tagName: upper,
    localName: String(tagName || "").toLowerCase(),
    id: attrs.id || "",
    className: attrs.class || "",
    content: attrs.content || "",
    name: attrs.name || "",
    type: attrs.type || "",
    rel: attrs.rel || "",
    href: attrs.href || "",
    src: attrs.src || "",
    charset: attrs.charset || "",
    async: true,
    defer: false,
    style: makeStyle(),
    children: [],
    childNodes: [],
    parentNode: null,
    ownerDocument: null,
    getAttribute: protect(function getAttribute(name) {
      rt_log("element.getAttribute", upper, name);
      const key = String(name);
      return attrs[key] !== undefined ? attrs[key] : element[key] !== undefined ? String(element[key]) : null;
    }),
    setAttribute: protect(function setAttribute(name, value) {
      rt_log("element.setAttribute", upper, name, value);
      const key = String(name);
      attrs[key] = String(value);
      element[key] = String(value);
    }),
    removeAttribute: protect(function removeAttribute(name) {
      rt_log("element.removeAttribute", upper, name);
      delete attrs[String(name)];
    }),
    hasAttribute: protect(function hasAttribute(name) {
      rt_log("element.hasAttribute", upper, name);
      return attrs[String(name)] !== undefined;
    }),
    appendChild: protect(function appendChild(child) {
      rt_log("element.appendChild", upper, child && child.tagName);
      if (child) {
        child.parentNode = element;
        element.children.push(child);
        element.childNodes.push(child);
      }
      if (child && typeof child.onload === "function") {
        child.onload({ type: "load", target: child });
      }
      return child;
    }),
    removeChild: protect(function removeChild(child) {
      rt_log("element.removeChild", upper, child && child.tagName);
      element.children = element.children.filter((item) => item !== child);
      element.childNodes = element.childNodes.filter((item) => item !== child);
      return child;
    }),
    getElementsByTagName: protect(function getElementsByTagName(name) {
      rt_log("element.getElementsByTagName", upper, name);
      const tag = String(name).toUpperCase();
      const hits = tag === "*" ? element.children.slice() : element.children.filter((child) => child.tagName === tag);
      return rtwatch(hits, upper.toLowerCase() + ".getElementsByTagName");
    }),
    addEventListener: protect(function addEventListener(type, listener) {
      rt_log("element.addEventListener", upper, type);
      listeners[type] = listeners[type] || [];
      listeners[type].push(listener);
    }),
    removeEventListener: protect(function removeEventListener(type, listener) {
      rt_log("element.removeEventListener", upper, type);
      listeners[type] = (listeners[type] || []).filter((item) => item !== listener);
    }),
    dispatchEvent: protect(function dispatchEvent(event) {
      rt_log("element.dispatchEvent", upper, event && event.type);
      for (const listener of listeners[(event && event.type) || ""] || []) {
        listener.call(element, event);
      }
      return true;
    }),
    getBoundingClientRect: protect(function getBoundingClientRect() {
      rt_log("element.getBoundingClientRect", upper);
      return { x: 0, y: 0, width: 0, height: 0, top: 0, left: 0, right: 0, bottom: 0 };
    }),
  };
  if (upper === "CANVAS") {
    element.width = attrs.width ? Number(attrs.width) : 300;
    element.height = attrs.height ? Number(attrs.height) : 150;
    const ctx = Object.create(CanvasRenderingContext2D.prototype);
    ctx.canvas = element;
    ctx.fillStyle = "#000000";
    ctx.strokeStyle = "#000000";
    ctx.font = "10px sans-serif";
    ctx.textBaseline = "alphabetic";
    ctx.globalCompositeOperation = "source-over";
    ctx.fillRect = protect(function fillRect(x, y, w, h) {
      rt_log("canvas.fillRect", x, y, w, h);
    });
    ctx.fillText = protect(function fillText(text, x, y) {
      rt_log("canvas.fillText", text, x, y);
    });
    ctx.strokeText = protect(function strokeText(text, x, y) {
      rt_log("canvas.strokeText", text, x, y);
    });
    ctx.measureText = protect(function measureText(text) {
      rt_log("canvas.measureText", text);
      return { width: String(text).length * 6 };
    });
    ctx.getImageData = protect(function getImageData(x, y, w, h) {
      rt_log("canvas.getImageData", x, y, w, h);
      const data = new Uint8ClampedArray(Math.max(1, Number(w) * Number(h) * 4));
      return { data, width: Number(w) || 0, height: Number(h) || 0 };
    });
    ctx.beginPath = protect(function beginPath() {
      rt_log("canvas.beginPath");
    });
    ctx.rect = protect(function rect() {
      rt_log("canvas.rect");
    });
    ctx.arc = protect(function arc() {
      rt_log("canvas.arc");
    });
    ctx.closePath = protect(function closePath() {
      rt_log("canvas.closePath");
    });
    ctx.fill = protect(function fill() {
      rt_log("canvas.fill");
    });
    ctx.stroke = protect(function stroke() {
      rt_log("canvas.stroke");
    });
    const watchedCtx = rtwatch(ctx, "canvas2d");
    element.getContext = protect(function getContext(type) {
      rt_log("canvas.getContext", type);
      return String(type).toLowerCase() === "2d" ? watchedCtx : null;
    });
    element.toDataURL = protect(function toDataURL(type) {
      rt_log("canvas.toDataURL", type);
      return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    });
  }
  if (upper === "IFRAME") {
    element.src = attrs.src || "";
    element.onload = null;
    Object.defineProperty(element, "contentWindow", {
      configurable: true,
      enumerable: true,
      get: protect(function contentWindow() {
        rt_log("iframe.contentWindow.get");
        return typeof window !== "undefined" ? window : null;
      }),
    });
    Object.defineProperty(element, "contentDocument", {
      configurable: true,
      enumerable: true,
      get: protect(function contentDocument() {
        rt_log("iframe.contentDocument.get");
        return typeof document !== "undefined" ? document : null;
      }),
    });
  }
  if (upper === "AUDIO" || upper === "VIDEO") {
    element.canPlayType = protect(function canPlayType(mime) {
      rt_log((upper === "AUDIO" ? "audio" : "video") + ".canPlayType", mime);
      const value = String(mime || "").toLowerCase();
      if (value.includes("mp4") || value.includes("mpeg") || value.includes("mp3") || value.includes("wav") || value.includes("ogg")) {
        return "probably";
      }
      return "";
    });
  }
  Object.setPrototypeOf(element, (ELEMENT_CTORS[upper] || HTMLElement).prototype);
  if (upper === "A") {
    let anchorUrl = attrs.href ? new URL(String(attrs.href), (global.location && global.location.href) || SITE_ORIGIN + "/") : null;
    function setAnchorUrl(value) {
      rt_log("anchor.href.set", value);
      attrs.href = String(value);
      anchorUrl = new URL(String(value), (global.location && global.location.href) || SITE_ORIGIN + "/");
    }
    function getAnchorPart(part) {
      rt_log("anchor." + part + ".get");
      return anchorUrl ? anchorUrl[part] : "";
    }
    protect(setAnchorUrl);
    protect(getAnchorPart);
    Object.defineProperties(element, {
      href: {
        configurable: true,
        enumerable: true,
        get: protect(function href() {
          return getAnchorPart("href");
        }),
        set: protect(function href(value) {
          setAnchorUrl(value);
        }),
      },
      protocol: { configurable: true, enumerable: true, get: protect(function protocol() { return getAnchorPart("protocol"); }) },
      host: { configurable: true, enumerable: true, get: protect(function host() { return getAnchorPart("host"); }) },
      hostname: { configurable: true, enumerable: true, get: protect(function hostname() { return getAnchorPart("hostname"); }) },
      port: { configurable: true, enumerable: true, get: protect(function port() { return getAnchorPart("port"); }) },
      pathname: { configurable: true, enumerable: true, get: protect(function pathname() { return getAnchorPart("pathname"); }) },
      search: { configurable: true, enumerable: true, get: protect(function search() { return getAnchorPart("search"); }) },
      hash: { configurable: true, enumerable: true, get: protect(function hash() { return getAnchorPart("hash"); }) },
      origin: { configurable: true, enumerable: true, get: protect(function origin() { return getAnchorPart("origin"); }) },
    });
  }
  return rtwatch(element, upper.toLowerCase());
}

function makeStorage(name) {
  const store = Object.create(null);
  const storage = {
    get length() {
      return Object.keys(store).length;
    },
    key: protect(function key(index) {
      rt_log(name + ".key", index);
      return Object.keys(store)[index] || null;
    }),
    getItem: protect(function getItem(keyName) {
      rt_log(name + ".getItem", keyName);
      keyName = String(keyName);
      return Object.prototype.hasOwnProperty.call(store, keyName) ? store[keyName] : null;
    }),
    setItem: protect(function setItem(keyName, value) {
      rt_log(name + ".setItem", keyName, value);
      store[String(keyName)] = String(value);
    }),
    removeItem: protect(function removeItem(keyName) {
      rt_log(name + ".removeItem", keyName);
      delete store[String(keyName)];
    }),
    clear: protect(function clear() {
      rt_log(name + ".clear");
      for (const keyName of Object.keys(store)) delete store[keyName];
    }),
  };
  return rtwatch(storage, name);
}

function makeMimeTypes() {
  const pdfPlugin = rtwatch(
    {
      name: "PDF Viewer",
      filename: "internal-pdf-viewer",
      description: "Portable Document Format",
    },
    "navigator.plugins.0"
  );
  const mimeTypes = [
    { type: "application/pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: pdfPlugin },
    { type: "text/pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: pdfPlugin },
  ];
  mimeTypes.item = protect(function item(index) {
    rt_log("navigator.mimeTypes.item", index);
    return mimeTypes[index] || null;
  });
  mimeTypes.namedItem = protect(function namedItem(name) {
    rt_log("navigator.mimeTypes.namedItem", name);
    return mimeTypes.find((item) => item.type === name) || null;
  });
  Object.defineProperty(mimeTypes, Symbol.toStringTag, { value: "MimeTypeArray" });
  const plugins = [pdfPlugin];
  plugins.item = protect(function item(index) {
    rt_log("navigator.plugins.item", index);
    return plugins[index] || null;
  });
  plugins.namedItem = protect(function namedItem(name) {
    rt_log("navigator.plugins.namedItem", name);
    return plugins.find((item) => item.name === name) || null;
  });
  Object.defineProperty(plugins, Symbol.toStringTag, { value: "PluginArray" });
  return {
    mimeTypes: rtwatch(mimeTypes, "navigator.mimeTypes"),
    plugins: rtwatch(plugins, "navigator.plugins"),
  };
}

function makeLockManager() {
  const locks = Object.create(LockManager.prototype);
  locks.request = protect(function request(name, options, callback) {
    rt_log("navigator.locks.request", name);
    if (typeof options === "function") return Promise.resolve(options({ name: String(name), mode: "exclusive" }));
    if (typeof callback === "function") return Promise.resolve(callback({ name: String(name), mode: (options && options.mode) || "exclusive" }));
    return Promise.resolve(undefined);
  });
  locks.query = protect(function query() {
    rt_log("navigator.locks.query");
    return Promise.resolve({ held: [], pending: [] });
  });
  return rtwatch(locks, "navigator.locks");
}

function makeIndexedDB(enqueueTimer) {
  const stores = { EkcP: Object.create(null) };

  function queueSuccess(request) {
    enqueueTimer(() => {
      request.readyState = "done";
      const event = { type: "success", target: request, currentTarget: request };
      if (typeof request.onsuccess === "function") request.onsuccess.call(request, event);
    });
  }

  function makeRequest(result, label, openRequest) {
    const request = Object.create(openRequest ? IDBOpenDBRequest.prototype : IDBRequest.prototype);
    request.result = result;
    request.error = null;
    request.source = null;
    request.transaction = null;
    request.readyState = "pending";
    request.onsuccess = null;
    request.onerror = null;
    if (openRequest) request.onupgradeneeded = null;
    return rtwatch(request, label);
  }

  function makeDOMStringList(items) {
    const list = Object.create(DOMStringList.prototype);
    items.forEach((item, index) => {
      list[index] = item;
    });
    list.length = items.length;
    list.contains = protect(function contains(name) {
      rt_log("DOMStringList.contains", name);
      return items.includes(String(name));
    });
    list.item = protect(function item(index) {
      rt_log("DOMStringList.item", index);
      return items[index] || null;
    });
    return rtwatch(list, "DOMStringList");
  }

  function makeObjectStore(dbName, storeName) {
    const store = Object.create(IDBObjectStore.prototype);
    store.name = storeName;
    store.keyPath = "name";
    store.get = protect(function get(key) {
      rt_log("IDBObjectStore.get", key);
      const bucket = stores[dbName] || (stores[dbName] = Object.create(null));
      const result = bucket[String(key)] || { name: String(key), value: null };
      const request = makeRequest(result, "IDBRequest", false);
      queueSuccess(request);
      return request;
    });
    store.put = protect(function put(value) {
      rt_log("IDBObjectStore.put", value && value.name);
      const bucket = stores[dbName] || (stores[dbName] = Object.create(null));
      if (value && value.name !== undefined) bucket[String(value.name)] = value;
      const request = makeRequest(value && value.name !== undefined ? String(value.name) : undefined, "IDBRequest", false);
      queueSuccess(request);
      return request;
    });
    return rtwatch(store, "IDBObjectStore");
  }

  function makeTransaction(dbName, mode) {
    const transaction = Object.create(IDBTransaction.prototype);
    transaction.mode = mode || "readonly";
    transaction.db = null;
    transaction.error = null;
    transaction.oncomplete = null;
    transaction.onerror = null;
    transaction.onabort = null;
    transaction.objectStore = protect(function objectStore(name) {
      rt_log("IDBTransaction.objectStore", name);
      return makeObjectStore(dbName, String(name));
    });
    return rtwatch(transaction, "IDBTransaction");
  }

  function makeDatabase(name) {
    const db = Object.create(IDBDatabase.prototype);
    db.name = name;
    db.version = 1;
    db.objectStoreNames = makeDOMStringList([name]);
    db.createObjectStore = protect(function createObjectStore(storeName) {
      rt_log("IDBDatabase.createObjectStore", storeName);
      stores[String(storeName)] = stores[String(storeName)] || Object.create(null);
      return makeObjectStore(name, String(storeName));
    });
    db.transaction = protect(function transaction(storeNames, mode) {
      rt_log("IDBDatabase.transaction", storeNames, mode);
      return makeTransaction(name, mode);
    });
    db.close = protect(function close() {
      rt_log("IDBDatabase.close");
    });
    return rtwatch(db, "IDBDatabase");
  }

  const factory = Object.create(IDBFactory.prototype);
  factory.open = protect(function open(name, version) {
    rt_log("IDBFactory.open", name, version);
    const dbName = String(name);
    stores[dbName] = stores[dbName] || Object.create(null);
    const request = makeRequest(makeDatabase(dbName), "IDBOpenDBRequest", true);
    queueSuccess(request);
    return request;
  });
  factory.deleteDatabase = protect(function deleteDatabase(name) {
    rt_log("IDBFactory.deleteDatabase", name);
    delete stores[String(name)];
    const request = makeRequest(undefined, "IDBOpenDBRequest", true);
    queueSuccess(request);
    return request;
  });
  factory.cmp = protect(function cmp(first, second) {
    rt_log("IDBFactory.cmp", first, second);
    return first === second ? 0 : first > second ? 1 : -1;
  });
  return rtwatch(factory, "indexedDB");
}

function makeLocation(url, envState) {
  let current = resolveUrl(url);
  const locationRaw = {
    get href() {
      return current.href;
    },
    set href(value) {
      rt_log("location.href.set", value);
      current = resolveUrl(value);
      envState.stopped = true;
    },
    get origin() {
      return current.origin;
    },
    get protocol() {
      return current.protocol;
    },
    get host() {
      return current.host;
    },
    get hostname() {
      return current.hostname;
    },
    get port() {
      return current.port;
    },
    get pathname() {
      return current.pathname;
    },
    get search() {
      return current.search;
    },
    get hash() {
      return current.hash;
    },
    assign: protect(function assign(urlValue) {
      rt_log("location.assign", urlValue);
      current = resolveUrl(urlValue);
      envState.stopped = true;
    }),
    replace: protect(function replace(urlValue) {
      rt_log("location.replace", urlValue);
      current = resolveUrl(urlValue);
      envState.stopped = true;
    }),
    reload: protect(function reload() {
      rt_log("location.reload");
      envState.stopped = true;
    }),
    toString: protect(function toString() {
      return current.href;
    }),
  };
  return rtwatch(locationRaw, "location");
}

function buildBody(input) {
  const fields = Object.assign(
    {
      pageSize: "10",
      pageNum: "1",
      iEType: "1",
      currencyType: "rmb",
      year: "2026",
      startMonth: "8",
      endMonth: "8",
      monthFlag: "",
      unitFlag: "true",
      unitFlag1: "true",
      codeLength: "8",
      outerField1: "CODE_TS",
      outerField2: "ORIGIN_COUNTRY",
      outerField3: "TRADE_MODE",
      outerField4: "",
      outerValue1: "08106000",
      outerValue2: "136,141",
      outerValue3: "10",
      outerValue4: "",
      orderType: "CODE ASC DEFAULT",
      selectTableState: "1",
      currentStartTime: "202502",
    },
    input.form || {}
  );
  return new URLSearchParams(fields).toString();
}

function installEnvironment(input = {}) {
  const envState = { stopped: false };
  const pagePacketPrefix = input.pagePacket || "000056";
  const pagePacket = input.pageHtml || input.pageHtmlPath ? null : readPacket(pagePacketPrefix);
  const pageHtml = input.pageHtmlPath
    ? readText(path.resolve(ROOT, input.pageHtmlPath))
    : input.pageHtml !== undefined
      ? String(input.pageHtml)
      : pagePacket.responseBody && pagePacket.responseBody.text
        ? pagePacket.responseBody.text
        : "";
  const pageUrl = input.pageUrl || (pagePacket && pagePacket.url) || DEFAULT_PAGE_URL;
  const requestCookie = input.requestCookie || (pagePacket ? getHeader(pagePacket, "Cookie") : "");
  const allCookies = Object.assign(parseCookieHeader(requestCookie), input.cookies || {});
  const visibleJar = Object.create(null);
  const httpOnlyJar = Object.create(null);

  for (const [name, value] of Object.entries(allCookies)) {
    if (HTTPONLY_COOKIES.has(name)) {
      httpOnlyJar[name] = value;
    } else {
      visibleJar[name] = value;
    }
  }

  const timerQueue = [];
  let timerId = 1;
  const windowListeners = Object.create(null);
  const captured = [];
  const metas = parseMetaElements(pageHtml);
  const dynamicMetaContent = getDynamicMetaContent(metas);
  const documentElement = makeElement("html");
  const head = makeElement("head");
  const body = makeElement("body");
  const currentScript = makeElement("script", {
    src: input.staticScriptSrc || extractStaticScriptSrc(pageHtml) || "/Qy6JDI4LYvbD/YWB5qmnxo45M.2437991.js",
  });
  const documentAll = protect(function HTMLAllCollection() {
    rt_log("document.all.call");
    return null;
  });
  documentAll.item = protect(function item(index) {
    rt_log("document.all.item", index);
    return null;
  });
  documentAll.namedItem = protect(function namedItem(name) {
    rt_log("document.all.namedItem", name);
    return null;
  });
  Object.defineProperty(documentAll, Symbol.toStringTag, { value: "HTMLAllCollection" });
  head.parentNode = documentElement;
  body.parentNode = documentElement;
  documentElement.children.push(head, body);
  documentElement.childNodes.push(head, body);
  for (const meta of metas) {
    meta.parentNode = head;
    head.children.push(meta);
    head.childNodes.push(meta);
  }
  currentScript.parentNode = head;
  head.children.push(currentScript);
  head.childNodes.push(currentScript);

  function enqueueTimer(fn) {
    if (envState.stopped) return;
    if (typeof fn === "function") {
      timerQueue.push(fn);
    }
  }

  const indexedDB = makeIndexedDB(enqueueTimer);
  const external = rtwatch(Object.create(External.prototype), "external");
  const locationObj = makeLocation(pageUrl, envState);

  const documentRaw = {
    nodeType: 9,
    nodeName: "#document",
    URL: pageUrl,
    documentURI: pageUrl,
    compatMode: "CSS1Compat",
    characterSet: "UTF-8",
    charset: "UTF-8",
    inputEncoding: "UTF-8",
    contentType: "text/html",
    domain: "stats.customs.gov.cn",
    referrer: input.referrer || "",
    readyState: "complete",
    title: "海关统计数据查询平台",
    visibilityState: "visible",
    hidden: false,
    onselectionchange: null,
    onmousemove: null,
    documentElement,
    head,
    body,
    all: documentAll,
    scrollingElement: documentElement,
    children: [documentElement],
    childNodes: [documentElement],
    defaultView: null,
    currentScript,
    scripts: [currentScript],
    location: locationObj,
    appendChild: protect(function appendChild(child) {
      rt_log("document.appendChild", child && child.tagName);
      if (child) {
        child.parentNode = document;
        documentRaw.children.push(child);
        documentRaw.childNodes.push(child);
      }
      return child;
    }),
    removeChild: protect(function removeChild(child) {
      rt_log("document.removeChild", child && child.tagName);
      documentRaw.children = documentRaw.children.filter((item) => item !== child);
      documentRaw.childNodes = documentRaw.childNodes.filter((item) => item !== child);
      return child;
    }),
    createElement: protect(function createElement(tagName) {
      rt_log("document.createElement", tagName);
      const element = makeElement(tagName);
      element.ownerDocument = document;
      return element;
    }),
    createElementNS: protect(function createElementNS(ns, tagName) {
      rt_log("document.createElementNS", ns, tagName);
      const element = makeElement(tagName);
      element.namespaceURI = ns;
      element.ownerDocument = document;
      return element;
    }),
    createTextNode: protect(function createTextNode(text) {
      rt_log("document.createTextNode", text);
      return rtwatch({ nodeType: 3, nodeName: "#text", textContent: String(text), nodeValue: String(text) }, "text");
    }),
    createEvent: protect(function createEvent(type) {
      rt_log("document.createEvent", type);
      return rtwatch(
        {
          type: "",
          bubbles: false,
          cancelable: false,
          initEvent: protect(function initEvent(name, bubbles, cancelable) {
            rt_log("event.initEvent", name, bubbles, cancelable);
            this.type = String(name);
            this.bubbles = !!bubbles;
            this.cancelable = !!cancelable;
          }),
        },
        "event"
      );
    }),
    getElementById: protect(function getElementById(id) {
      rt_log("document.getElementById", id);
      return metas.find((item) => item.id === id) || null;
    }),
    getElementsByTagName: protect(function getElementsByTagName(tagName) {
      rt_log("document.getElementsByTagName", tagName);
      const tag = String(tagName).toLowerCase();
      if (tag === "meta") return rtwatch(metas, "document.metaList");
      if (tag === "head") return rtwatch([head], "document.headList");
      if (tag === "body") return rtwatch([body], "document.bodyList");
      if (tag === "script") return rtwatch(documentRaw.scripts, "document.scriptList");
      return rtwatch([], "document." + tag + "List");
    }),
    querySelector: protect(function querySelector(selector) {
      rt_log("document.querySelector", selector);
      const m = /^#(.+)$/.exec(String(selector));
      if (m) return documentRaw.getElementById(m[1]);
      if (String(selector).toLowerCase() === "meta") return metas[0] || null;
      if (String(selector).toLowerCase() === "head") return head;
      if (String(selector).toLowerCase() === "body") return body;
      return null;
    }),
    querySelectorAll: protect(function querySelectorAll(selector) {
      rt_log("document.querySelectorAll", selector);
      if (String(selector).toLowerCase() === "meta") return rtwatch(metas, "document.querySelectorAll.meta");
      const one = documentRaw.querySelector(selector);
      return rtwatch(one ? [one] : [], "document.querySelectorAll");
    }),
    addEventListener: protect(function addEventListener(type, listener) {
      rt_log("document.addEventListener", type);
      windowListeners["document:" + type] = windowListeners["document:" + type] || [];
      windowListeners["document:" + type].push(listener);
      if (type === "DOMContentLoaded" || type === "readystatechange") enqueueTimer(() => listener.call(document, { type, target: document }));
    }),
    removeEventListener: protect(function removeEventListener(type, listener) {
      rt_log("document.removeEventListener", type);
      const key = "document:" + type;
      windowListeners[key] = (windowListeners[key] || []).filter((item) => item !== listener);
    }),
    dispatchEvent: protect(function dispatchEvent(event) {
      rt_log("document.dispatchEvent", event && event.type);
      for (const listener of windowListeners["document:" + ((event && event.type) || "")] || []) listener.call(document, event);
      return true;
    }),
    hasFocus: protect(function hasFocus() {
      rt_log("document.hasFocus");
      return true;
    }),
  };

  Object.defineProperty(documentRaw, "cookie", {
    configurable: true,
    enumerable: true,
    get: protect(function cookie() {
      rt_log("document.cookie.get");
      return serializeCookies(visibleJar, null);
    }),
    set: protect(function cookie(value) {
      rt_log("document.cookie.set", value);
      const parsed = parseCookieSet(value);
      if (parsed) {
        visibleJar[parsed.name] = parsed.value;
        if (parsed.name === "AV7KYchI7HHaT" && input.stopOnFirstCookie) {
          envState.stopped = true;
        }
      }
    }),
  });

  document = rtwatch(documentRaw, "document");
  location = locationObj;

  function Navigator() {}
  protect(Navigator);
  const mimeStore = makeMimeTypes();
  const navigatorValues = {
    vendorSub: "",
    productSub: "20100101",
    vendor: "",
    maxTouchPoints: 0,
    hardwareConcurrency: 12,
    cookieEnabled: true,
    appCodeName: "Mozilla",
    appName: "Netscape",
    appVersion: "5.0 (Windows)",
    platform: "Win32",
    product: "Gecko",
    userAgent: input.userAgent || USER_AGENT,
    language: "en-US",
    languages: ["en-US", "en"],
    buildID: "20181001000000",
    onLine: true,
    webdriver: false,
    mimeTypes: mimeStore.mimeTypes,
    plugins: mimeStore.plugins,
    pdfViewerEnabled: true,
    doNotTrack: "unspecified",
    oscpu: "Windows NT 10.0; Win64; x64",
    locks: makeLockManager(),
    permissions: rtwatch(
      {
        query: protect(function query(permission) {
          rt_log("navigator.permissions.query", permission && permission.name);
          return Promise.resolve({ state: "prompt", onchange: null });
        }),
      },
      "navigator.permissions"
    ),
  };
  for (const [key, value] of Object.entries(navigatorValues)) {
    Object.defineProperty(Navigator.prototype, key, {
      configurable: true,
      enumerable: true,
      get: protect(function navigatorGetter() {
        rt_log("navigator." + key + ".get");
        return value;
      }),
    });
  }
  Navigator.prototype.javaEnabled = protect(function javaEnabled() {
    rt_log("navigator.javaEnabled");
    return false;
  });
  Navigator.prototype.sendBeacon = protect(function sendBeacon(url, data) {
    rt_log("navigator.sendBeacon", url, data && data.length);
    return true;
  });
  Object.defineProperty(Navigator.prototype, Symbol.toStringTag, { value: "Navigator" });
  const navigatorRaw = Object.create(Navigator.prototype);
  navigator = rtwatch(navigatorRaw, "navigator");

  screen = rtwatch(
    {
      width: 1920,
      height: 1080,
      availWidth: 1920,
      availHeight: 1040,
      colorDepth: 24,
      pixelDepth: 24,
      orientation: rtwatch({ type: "landscape-primary", angle: 0 }, "screen.orientation"),
    },
    "screen"
  );

  history = rtwatch(
    {
      length: 2,
      state: null,
      back: protect(function back() {
        rt_log("history.back");
      }),
      forward: protect(function forward() {
        rt_log("history.forward");
      }),
      go: protect(function go(delta) {
        rt_log("history.go", delta);
      }),
      pushState: protect(function pushState(state, title, url) {
        rt_log("history.pushState", state, title, url);
      }),
      replaceState: protect(function replaceState(state, title, url) {
        rt_log("history.replaceState", state, title, url);
      }),
    },
    "history"
  );

  localStorage = makeStorage("localStorage");
  sessionStorage = makeStorage("sessionStorage");

  performance = rtwatch(
    {
      timeOrigin: Date.now() - 1000,
      now: protect(function now() {
        rt_log("performance.now");
        return Date.now() - this.timeOrigin;
      }),
      getEntries: protect(function getEntries() {
        rt_log("performance.getEntries");
        return [];
      }),
      getEntriesByType: protect(function getEntriesByType(type) {
        rt_log("performance.getEntriesByType", type);
        return [];
      }),
      getEntriesByName: protect(function getEntriesByName(name, type) {
        rt_log("performance.getEntriesByName", name, type);
        return [];
      }),
      mark: protect(function mark(name) {
        rt_log("performance.mark", name);
      }),
      measure: protect(function measure(name) {
        rt_log("performance.measure", name);
      }),
    },
    "performance"
  );

  function Event(type, init = {}) {
    this.type = type;
    this.bubbles = !!init.bubbles;
    this.cancelable = !!init.cancelable;
    this.target = init.target || null;
    this.currentTarget = null;
    this.timeStamp = Date.now();
  }
  protect(Event);

  function XMLHttpRequest() {
    this.readyState = 0;
    this.status = 0;
    this.responseText = "";
    this.response = "";
    this.onreadystatechange = null;
    this.onload = null;
    this.onerror = null;
    this._headers = Object.create(null);
  }
  XMLHttpRequest.prototype.open = protect(function open(method, url, async, user, password) {
    rt_log("XMLHttpRequest.open", method, url, async, user, password);
    this._method = method;
    this._url = new URL(String(url), location.href).href;
    captured.push({ type: "open", method, url: this._url });
    this.readyState = 1;
  });
  XMLHttpRequest.prototype.setRequestHeader = protect(function setRequestHeader(name, value) {
    rt_log("XMLHttpRequest.setRequestHeader", name, value);
    this._headers[String(name)] = String(value);
    captured.push({ type: "header", name: String(name), value: String(value), url: this._url, method: this._method });
  });
  XMLHttpRequest.prototype.send = protect(function send(bodyValue) {
    rt_log("XMLHttpRequest.send", bodyValue);
    captured.push({ type: "send", method: this._method, url: this._url, body: bodyValue, headers: Object.assign({}, this._headers) });
    this.readyState = 4;
    this.status = 200;
    if (typeof this.onreadystatechange === "function") this.onreadystatechange();
    if (typeof this.onload === "function") this.onload({ type: "load", target: this });
  });
  XMLHttpRequest.prototype.abort = protect(function abort() {
    rt_log("XMLHttpRequest.abort");
  });
  XMLHttpRequest.prototype.addEventListener = protect(function addEventListener(type, listener) {
    rt_log("XMLHttpRequest.addEventListener", type);
    this["_on" + type] = listener;
  });
  XMLHttpRequest.prototype.removeEventListener = protect(function removeEventListener(type) {
    rt_log("XMLHttpRequest.removeEventListener", type);
  });
  XMLHttpRequest.prototype.getResponseHeader = protect(function getResponseHeader(name) {
    rt_log("XMLHttpRequest.getResponseHeader", name);
    return null;
  });
  XMLHttpRequest.prototype.getAllResponseHeaders = protect(function getAllResponseHeaders() {
    rt_log("XMLHttpRequest.getAllResponseHeaders");
    return "";
  });
  protect(XMLHttpRequest);
  const originalXhrOpen = XMLHttpRequest.prototype.open;

  function setTimeoutShim(callback, delay) {
    rt_log("setTimeout", delay);
    const id = timerId++;
    enqueueTimer(callback);
    return id;
  }
  function setIntervalShim(callback, delay) {
    rt_log("setInterval", delay);
    return timerId++;
  }
  function clearTimerShim(id) {
    rt_log("clearTimer", id);
  }
  function requestAnimationFrameShim(callback) {
    rt_log("requestAnimationFrame");
    enqueueTimer(() => callback(Date.now()));
    return timerId++;
  }
  function fetchShim(url, options) {
    rt_log("fetch", url, options && options.method);
    captured.push({ type: "fetch", url: new URL(String(url), location.href).href, options });
    return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(""), json: () => Promise.resolve({}) });
  }
  function getComputedStyleShim(element) {
    rt_log("getComputedStyle", element && element.tagName);
    return makeStyle();
  }

  function makeBarProp(name) {
    const bar = Object.create(BarProp.prototype);
    bar.visible = true;
    return rtwatch(bar, name);
  }

  function mediaQueryMatches(query) {
    const normalized = String(query).toLowerCase().replace(/\s+/g, " ");
    if (/\(\s*(any-)?pointer\s*:\s*fine\s*\)/.test(normalized)) return true;
    if (/\(\s*(any-)?pointer\s*:\s*coarse\s*\)/.test(normalized)) return false;
    if (/\(\s*(any-)?pointer\s*:\s*none\s*\)/.test(normalized)) return false;
    if (/\(\s*(any-)?pointer\s*\)/.test(normalized)) return true;
    if (/\(\s*(any-)?hover\s*:\s*hover\s*\)/.test(normalized)) return true;
    if (/\(\s*(any-)?hover\s*:\s*on-demand\s*\)/.test(normalized)) return false;
    if (/\(\s*(any-)?hover\s*:\s*none\s*\)/.test(normalized)) return false;
    if (/\(\s*(any-)?hover\s*\)/.test(normalized)) return true;
    if (/\(\s*color-gamut\s*:\s*srgb\s*\)/.test(normalized)) return true;
    if (/\(\s*color-gamut\s*:\s*p3\s*\)/.test(normalized)) return false;
    if (/\(\s*color-gamut\s*:\s*rec2020\s*\)/.test(normalized)) return false;
    if (/\(\s*color-gamut\s*\)/.test(normalized)) return true;
    return false;
  }

  function matchMedia(query) {
    rt_log("matchMedia", query);
    const media = String(query);
    return rtwatch(
      {
        matches: mediaQueryMatches(media),
        media,
        onchange: null,
        addListener: protect(function addListener(listener) {
          rt_log("MediaQueryList.addListener", listener);
        }),
        removeListener: protect(function removeListener(listener) {
          rt_log("MediaQueryList.removeListener", listener);
        }),
        addEventListener: protect(function addEventListener(type, listener) {
          rt_log("MediaQueryList.addEventListener", type, listener);
        }),
        removeEventListener: protect(function removeEventListener(type, listener) {
          rt_log("MediaQueryList.removeEventListener", type, listener);
        }),
        dispatchEvent: protect(function dispatchEvent(event) {
          rt_log("MediaQueryList.dispatchEvent", event && event.type);
          return true;
        }),
      },
      "MediaQueryList"
    );
  }

  for (const fn of [setTimeoutShim, setIntervalShim, clearTimerShim, requestAnimationFrameShim, fetchShim, getComputedStyleShim, matchMedia]) {
    protect(fn);
  }

  function defineGlobal(name, value) {
    try {
      Object.defineProperty(global, name, {
        configurable: true,
        enumerable: true,
        writable: true,
        value,
      });
    } catch (err) {
      try {
        global[name] = value;
      } catch (inner) {
        rt_log("defineGlobal.fail", name, inner && inner.message);
      }
    }
  }

  const globalProps = {
    document,
    location,
    navigator,
    screen,
    history,
    localStorage,
    sessionStorage,
    performance,
    XMLHttpRequest,
    Node,
    Element,
    HTMLElement,
    HTMLHtmlElement,
    HTMLHeadElement,
    HTMLBodyElement,
    HTMLMetaElement,
    HTMLScriptElement,
    HTMLAnchorElement,
    HTMLFormElement,
    HTMLInputElement,
    HTMLDivElement,
    HTMLCanvasElement,
    HTMLIFrameElement,
    HTMLAudioElement,
    HTMLVideoElement,
    CanvasRenderingContext2D,
    BarProp,
    IDBFactory,
    IDBRequest,
    IDBOpenDBRequest,
    IDBDatabase,
    IDBTransaction,
    IDBObjectStore,
    DOMStringList,
    External,
    LockManager,
    Navigator,
    Event,
    setTimeout: setTimeoutShim,
    setInterval: setIntervalShim,
    clearTimeout: clearTimerShim,
    clearInterval: clearTimerShim,
    requestAnimationFrame: requestAnimationFrameShim,
    cancelAnimationFrame: clearTimerShim,
    fetch: fetchShim,
    getComputedStyle: getComputedStyleShim,
    matchMedia,
    open: protect(function open(url, target, features) {
      rt_log("window.open", url, target, features);
      return null;
    }),
    postMessage: protect(function postMessage(message, targetOrigin) {
      rt_log("window.postMessage", message, targetOrigin);
    }),
    crypto: rtwatch(
      {
        getRandomValues: protect(function getRandomValues(typedArray) {
          rt_log("crypto.getRandomValues", typedArray && typedArray.length);
          if (nodeCrypto.webcrypto && nodeCrypto.webcrypto.getRandomValues) {
            return nodeCrypto.webcrypto.getRandomValues(typedArray);
          }
          const bytes = nodeCrypto.randomBytes(typedArray.length);
          typedArray.set(bytes);
          return typedArray;
        }),
        subtle: undefined,
      },
      "crypto"
    ),
    indexedDB,
    external,
    ActiveXObject: undefined,
    innerWidth: 1280,
    innerHeight: 649,
    outerWidth: 1294,
    outerHeight: 741,
    screenLeft: 3,
    screenTop: 3,
    screenX: 3,
    screenY: 3,
    devicePixelRatio: 1,
    pageXOffset: 0,
    pageYOffset: 0,
    scrollX: 0,
    scrollY: 0,
    name: "",
    status: "",
    closed: false,
    length: 0,
    opener: null,
    frameElement: null,
    onmessage: null,
    onerror: null,
    origin: SITE_ORIGIN,
    isSecureContext: false,
    crossOriginIsolated: false,
    clientInformation: navigator,
    locationbar: makeBarProp("locationbar"),
    menubar: makeBarProp("menubar"),
    personalbar: makeBarProp("personalbar"),
    scrollbars: makeBarProp("scrollbars"),
    statusbar: makeBarProp("statusbar"),
    toolbar: makeBarProp("toolbar"),
    dolphinmeta: input.dolphinmeta !== undefined ? String(input.dolphinmeta) : dynamicMetaContent,
  };
  for (const [name, value] of Object.entries(globalProps)) {
    defineGlobal(name, value);
  }

  window = rtwatch(global, "window");
  for (const [name, value] of Object.entries({
    window,
    self: window,
    top: window,
    parent: window,
    frames: window,
    document,
    location,
    navigator,
    screen,
    history,
  })) {
    defineGlobal(name, value);
  }
  documentRaw.defaultView = window;

  window.addEventListener = protect(function addEventListener(type, listener) {
    rt_log("window.addEventListener", type);
    windowListeners[type] = windowListeners[type] || [];
    windowListeners[type].push(listener);
    if (type === "load") enqueueTimer(() => listener.call(window, { type: "load", target: window }));
  });
  window.removeEventListener = protect(function removeEventListener(type, listener) {
    rt_log("window.removeEventListener", type);
    windowListeners[type] = (windowListeners[type] || []).filter((item) => item !== listener);
  });
  window.dispatchEvent = protect(function dispatchEvent(event) {
    rt_log("window.dispatchEvent", event && event.type);
    for (const listener of windowListeners[(event && event.type) || ""] || []) listener.call(window, event);
    return true;
  });

  function flushTimers(limit = 80) {
    let count = 0;
    while (timerQueue.length && count < limit && !envState.stopped) {
      const fn = timerQueue.shift();
      count += 1;
      try {
        fn.call(window);
      } catch (err) {
        captured.push({ type: "timerError", message: err && err.message, stack: err && err.stack });
      }
    }
    return count;
  }

  return {
    pageHtml,
    pageUrl,
    visibleJar,
    httpOnlyJar,
    captured,
    originalXhrOpen,
    flushTimers,
    envState,
  };
}

function runScript(source, filename, timeout = 8000) {
  return vm.runInThisContext("(function(){\n" + source + "\n}).call(window);", { filename, timeout });
}

// 目标js

function loadStaticRsScript(input = {}) {
  if (input.staticSourcePath) {
    return readText(path.resolve(ROOT, input.staticSourcePath));
  }
  if (input.staticSource !== undefined) {
    return String(input.staticSource);
  }
  const staticPacket = input.staticPacket || "000057";
  return packetText(staticPacket);
}

function runRs6(input = {}) {
  const env = installEnvironment(input);
  const stages = [];
  function stage(name, fn, options = {}) {
    stages.push({ name, start: Date.now() });
    try {
      const value = fn();
      stages[stages.length - 1].ok = true;
      stages[stages.length - 1].end = Date.now();
      return value;
    } catch (err) {
      stages[stages.length - 1].ok = false;
      stages[stages.length - 1].end = Date.now();
      stages[stages.length - 1].error = err && err.message;
      stages[stages.length - 1].stack = err && err.stack;
      if (!options.tolerate) {
        throw err;
      }
      return undefined;
    }
  }
  const rsScripts = extractRsScripts(env.pageHtml);
  let body = input.body || buildBody(input);
  let targetPath = input.targetPath || DEFAULT_TARGET_PATH;
  let url = "";
  let suffix = null;
  let headerSuffix = null;
  let cookieAtSend = "";
  let cookiesAtSend = null;
  const method = input.method || "POST";
  try {
    stage("inline_seed", () => {
      for (const item of rsScripts) {
        if (item.type === "external") break;
        if (item.type === "inline") {
          runScript(item.source, "inline_rs_seed.js", input.scriptTimeoutMs || 8000);
        }
      }
    });
    stage("static_rs", () => {
      const staticSource = loadStaticRsScript(input);
      runScript(staticSource, "rs_static.js", input.scriptTimeoutMs || 15000);
    }, { tolerate: input.continueAfterStaticError !== false });
    stage("inline_after_static", () => {
      let afterExternal = false;
      for (const item of rsScripts) {
        if (item.type === "external") {
          afterExternal = true;
          continue;
        }
        if (afterExternal && item.type === "inline") {
          runScript(item.source, "inline_rs_after_static.js", input.scriptTimeoutMs || 8000);
        }
      }
    });
    stage("flush_init_timers", () => env.flushTimers());
    if (!input.skipXhrBoundary) {
      stage("xhr_boundary", () => {
        const xhr = new XMLHttpRequest();
        xhr.open(method, targetPath, true);
        xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
        xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
        xhr.send(body);
      });
      // RS wrapper 在 send 内同步改写 cookie；必须立刻快照，后续 timer 会再次刷新
      cookieAtSend = serializeCookies(env.visibleJar, null);
      cookiesAtSend = Object.assign({}, env.visibleJar);
      stage("flush_xhr_timers", () => env.flushTimers());
      const lastSend = [...env.captured].reverse().find((item) => item.type === "send");
      const lastOpen = [...env.captured].reverse().find((item) => item.type === "open");
      url = (lastSend && lastSend.url) || (lastOpen && lastOpen.url) || "";
      const parsed = url ? new URL(url) : null;
      suffix = parsed ? parsed.searchParams.get(QUERY_NAME) : null;
      if (!suffix && parsed && parsed.search) {
        const first = [...parsed.searchParams.entries()][0];
        if (first) suffix = first[1];
      }
      if (lastSend && lastSend.headers && lastSend.headers[QUERY_NAME]) {
        headerSuffix = lastSend.headers[QUERY_NAME];
      } else {
        const lastHeader = [...env.captured].reverse().find((item) => item.type === "header" && item.name === QUERY_NAME);
        if (lastHeader && lastHeader.value) headerSuffix = lastHeader.value;
      }
    }
  } catch (err) {
    return {
      ok: false,
      queryName: QUERY_NAME,
      [QUERY_NAME]: null,
      headerSuffix: null,
      cookieAtSend: "",
      cookiesAtSend: null,
      url,
      body,
      cookie: serializeCookies(env.visibleJar, env.httpOnlyJar),
      cookies: Object.assign({}, env.httpOnlyJar, env.visibleJar),
      visibleCookies: Object.assign({}, env.visibleJar),
      captured: env.captured,
      stages,
      error: err && err.message,
      stack: err && err.stack,
      tsKeys: Object.keys(global.$_ts || {}),
      xhrOpenChanged: env.originalXhrOpen !== XMLHttpRequest.prototype.open,
      xhrOpenSource: Function.prototype.toString.call(XMLHttpRequest.prototype.open).slice(0, 180),
      rtLogs: rtLogRing,
      nativeProtected: nativeList,
    };
  }
  const tsKeys = Object.keys(global.$_ts || {});
  return {
    ok: !!suffix,
    queryName: QUERY_NAME,
    [QUERY_NAME]: suffix,
    headerSuffix,
    cookieAtSend,
    cookiesAtSend,
    url,
    body,
    cookie: serializeCookies(env.visibleJar, env.httpOnlyJar),
    cookies: Object.assign({}, env.httpOnlyJar, env.visibleJar),
    visibleCookies: Object.assign({}, env.visibleJar),
    captured: env.captured,
    stages,
    tsKeys,
    xhrOpenChanged: env.originalXhrOpen !== XMLHttpRequest.prototype.open,
    xhrOpenSource: Function.prototype.toString.call(XMLHttpRequest.prototype.open).slice(0, 180),
    rtLogs: rtLogRing,
    nativeProtected: nativeList,
  };
}

function get_8h6a7FPl(input = {}) {
  return runRs6(input)[QUERY_NAME];
}

function get_cookie(input = {}) {
  return runRs6(input).cookie;
}

function get_rs6(input = {}) {
  return runRs6(input);
}

function build_target_request(input = {}) {
  const result = runRs6(input);
  return {
    ok: result.ok,
    method: "POST",
    url: result.url,
    body: result.body,
    headers: {
      "User-Agent": input.userAgent || USER_AGENT,
      Accept: "text/html, */*; q=0.01",
      "Accept-Language": "en-US,en;q=0.9",
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      Origin: SITE_ORIGIN,
      Referer: input.pageUrl || SITE_ORIGIN + "/queryData/queryDataList",
      "Sec-Fetch-Dest": "empty",
      "Sec-Fetch-Mode": "cors",
      "Sec-Fetch-Site": "same-origin",
      "Idempotency-Key": input.idempotencyKey || '"' + Date.now() + Math.floor(Math.random() * 1000000) + '"',
      Cookie: result.cookie,
    },
    rs6: result,
  };
}

module.exports = {
  get_8h6a7FPl,
  get_cookie,
  get_rs6,
  build_target_request,
};

if (require.main === module) {
  try {
    // 大 payload（含 350KB 瑞数脚本正文）走 stdin，避免 Windows 环境变量 32KB 上限
    let raw = process.env.CODEX_INPUT_JSON;
    if (!raw) {
      try {
        raw = fs.readFileSync(0, "utf8");
      } catch (err) {}
    }
    raw = raw || process.argv[2] || "{}";
    const input = JSON.parse(raw);
    const action = input.action || "get_rs6";
    const table = {
      get_8h6a7FPl,
      get_cookie,
      get_rs6,
      build_target_request,
    };
    if (!table[action]) {
      throw new Error("unknown action: " + action);
    }
    const output = table[action](input);
    process.stdout.write(JSON.stringify(output, null, 2));
  } catch (err) {
    process.stdout.write(JSON.stringify({ ok: false, error: err && err.message, stack: err && err.stack }, null, 2));
    process.exitCode = 1;
  }
}
