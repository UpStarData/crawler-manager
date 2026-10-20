# -*- coding: utf-8 -*-
"""
抓取中央气象台《全国农业气象周报》全部正文（REST 接口版）。

接口: https://www.nmc.cn/f/rest/getContent?dataId=<dataId>&_=<毫秒时间戳>
dataId 形如 SEVP_NMC_AMDB_SFER_EAB_ACHN_L88_P9_YYYYMMDD000000000，
其中 YYYYMMDD 为周报发布日期（通常为周一）。接口直接返回一段 HTML 片段，
由 <div class=title>、<div class=author>、<div class=writing> 三部分组成。

范围: 从标题“全国农业气象周报”到正文结束的全部内容，包括大字标题、
各章节文字、图1-图4 及附图等图表（Markdown 中直接引用原始图片链接，
Word 中下载后嵌入）以及附表（HTML 表格转为 Markdown/Word 表格，
支持跨列合并）。

输出:
  - 全国农业气象周报_YYYYMMDD.md   (Markdown，图片为原始链接)
  - 全国农业气象周报_YYYYMMDD.docx (Word，图片嵌入; 需安装 python-docx，
    未安装时自动跳过 Word 输出)

用法: 先把下方 ISSUE_DATE 改成目标周报的发布日期（通常为周一，
支持 "2026-10-12"、"20261012"、"2026/10/12" 或完整 dataId），再运行
  python fetch_agro_weekly.py

指定日期当天无周报时自动向前（最多 7 天）寻找最近一期。
"""

import io
import os
import re
import sys
import time
from datetime import date, timedelta
from html.parser import HTMLParser

import requests

API = "https://www.nmc.cn/f/rest/getContent"
DATA_ID_PREFIX = "SEVP_NMC_AMDB_SFER_EAB_ACHN_L88_P9_"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://www.nmc.cn/",
}
OUT_DIR = os.path.dirname(os.path.abspath(__file__))

ISSUE_DATE = "2026-10-19"  # ← 周报发布日期，在此手动修改（也可填完整 dataId）

SECTION_RE = re.compile(r"^(附)?[一二三四五六七八九十]+、")  # “一、…”“附一、…”章节标题
SUBSECTION_RE = re.compile(r"^\d+\.\d+\s")  # “1.1 …”小节标题
FIGCAP_RE = re.compile(r"^附?图\d+")  # 图注（含附图）
TABCAP_RE = re.compile(r"^附表\d+")  # 附表标题

PROXY_URL = os.getenv("PROXY_URL", "")
proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}


def normalize(text):
    return re.sub(r"[\s\u3000]+", " ", text).strip()


class AgroWeeklyParser(HTMLParser):
    """截取 title/author/writing 三个块，把 writing 解析成段落与表格序列。"""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.phase = "seek"  # seek -> title -> between -> author -> between2 -> writing -> done
        self.title_runs = []
        self.author_runs = []
        self.blocks = []  # {'type':'p','text':…,'imgs':[…]} / {'type':'table','rows':[…]}
        self.stack = []  # writing 区内打开的 table/tr/td/p 标签栈
        self.runs = []  # 当前顶层段落文本
        self.imgs = []  # 当前顶层段落图片
        self.cell = None  # 当前表格单元格 {'text':…, 'runs':…, 'colspan':…}
        self._skip = 0

    # -- 标签 --
    def handle_starttag(self, tag, attrs):
        if self._skip:
            return
        if tag in ("script", "style"):
            self._skip += 1
            return
        if self.phase == "seek":
            if tag == "div" and "title" in (dict(attrs).get("class") or "").split():
                self.phase = "title"
            return
        if self.phase == "between":
            if tag == "div" and "author" in (dict(attrs).get("class") or "").split():
                self.phase = "author"
            return
        if self.phase == "between2":
            if tag == "div" and "writing" in (dict(attrs).get("class") or "").split():
                self.phase = "writing"
            return
        if self.phase != "writing":
            return
        adict = dict(attrs)
        if tag == "table":
            self._flush_paragraph()  # 表格前先收口挂起的段落，保证块顺序与原文一致
            self.blocks.append({"type": "table", "rows": []})
            self.stack.append("table")
        elif tag == "tr":
            self.blocks[-1]["rows"].append([])
            self.stack.append("tr")
        elif tag == "td":
            self.cell = {"text": "", "runs": [], "colspan": int(adict.get("colspan") or 1)}
            self.blocks[-1]["rows"][-1].append(self.cell)
            self.stack.append("td")
        elif tag == "p":
            if self.cell is not None:  # 单元格内的 p（Word 导出风格），文本并入单元格
                self.stack.append("p")
            else:
                self._flush_paragraph()  # 上一段在此收口
                self.stack.append("p")
        elif tag == "img" and adict.get("src"):
            if self.cell is None:
                self.imgs.append(adict["src"])
        elif tag == "br":
            target = self.cell["runs"] if self.cell is not None else self.runs
            target.append(" ")

    def handle_endtag(self, tag):
        if self._skip:
            if tag in ("script", "style"):
                self._skip -= 1
            return
        if self.phase == "title":
            if tag == "div":
                self._title = normalize("".join(self.title_runs))
                self.phase = "between"
            return
        if self.phase == "author":
            if tag == "div":
                self._author = normalize("".join(self.author_runs))
                self.phase = "between2"
            return
        if self.phase != "writing":
            return
        if tag == "p":
            if self.stack and self.stack[-1] == "p":
                self.stack.pop()
                if self.cell is None:
                    self._flush_paragraph()
        elif tag == "td" and "td" in self.stack:
            while self.stack.pop() != "td":
                pass
            self.cell["text"] = normalize("".join(self.cell["runs"]))
            self.cell = None
        elif tag == "tr" and "tr" in self.stack:
            while self.stack.pop() != "tr":
                pass
        elif tag == "table" and "table" in self.stack:
            while self.stack.pop() != "table":
                pass
        elif tag == "div":
            self._flush_paragraph()
            self.phase = "done"

    def handle_data(self, data):
        if self._skip:
            return
        if self.phase == "title":
            self.title_runs.append(data)
        elif self.phase == "author":
            self.author_runs.append(data)
        elif self.phase == "writing":
            target = self.cell["runs"] if self.cell is not None else self.runs
            target.append(data)

    def _flush_paragraph(self):
        text = normalize("".join(self.runs))
        if text or self.imgs:
            self.blocks.append({"type": "p", "text": text, "imgs": self.imgs})
        self.runs, self.imgs = [], []

    # -- 结果 --
    @property
    def title(self):
        return getattr(self, "_title", "")

    @property
    def author(self):
        return getattr(self, "_author", "")


def build_data_id(d):
    return f"{DATA_ID_PREFIX}{d:%Y%m%d}000000000"


def fetch_html(url):
    try:
        resp = requests.get(url, headers=HEADERS, proxies=None, timeout=30)
    except requests.RequestException as e:
        print(f"  [warn] 请求失败: {e}")
        return None
    if resp.status_code != 200:
        return None
    resp.encoding = "utf-8"
    return resp.text


def issue_date_of(data_id):
    m = re.search(r"(\d{8})\d{9}$", data_id)
    return f"{m[1][:4]}-{m[1][4:6]}-{m[1][6:]}" if m else "未知日期"


def fetch_issue(target):
    """target 为 date（当天没有则向前探测）或完整 dataId 字符串。"""
    if isinstance(target, str):
        data_id = target
        html = fetch_html(f"{API}?dataId={data_id}&_={int(time.time() * 1000)}")
        return (data_id, html) if html and '<div class="title"' in html else (None, None)
    for i in range(8):
        data_id = build_data_id(target - timedelta(days=i))
        html = fetch_html(f"{API}?dataId={data_id}&_={int(time.time() * 1000)}")
        if html and '<div class="title"' in html:
            if i:
                print(f"  [info] {target:%Y-%m-%d} 当天无周报，改用最近一期 {issue_date_of(data_id)} 发布的周报")
            return data_id, html
    return None, None


def parse_target(value):
    """把 ISSUE_DATE 解析为 date 或完整 dataId。"""
    if value.upper().startswith("SEVP_"):
        return value
    m = re.search(r"(\d{4})[年\-/.]?(\d{1,2})[月\-/.]?(\d{1,2})日?", value)
    if not m:
        sys.exit(f"[error] ISSUE_DATE 无法识别: {value}")
    return date(int(m[1]), int(m[2]), int(m[3]))


def build_markdown(title, author, blocks):
    lines = [f"# {title}", "", author, "", "---", ""]
    in_head = True  # 第一个章节标题之前为两行大字标题
    for blk in blocks:
        if blk["type"] == "table":
            lines += table_to_md(blk["rows"])
            lines.append("")
            continue
        text, imgs = blk["text"], blk["imgs"]
        for url in imgs:
            lines.append(f"![{title}图表]({url})")
            lines.append("")
        if not text:
            continue
        if in_head and SECTION_RE.match(text):
            in_head = False
        if in_head:  # 大字标题
            lines.append(f"**{text}**")
        elif SECTION_RE.match(text):  # “一、”“附一、”章节
            lines.append(f"## {text}")
        elif SUBSECTION_RE.match(text):  # “1.1 …”小节
            lines.append(f"### {text}")
        elif FIGCAP_RE.match(text):  # 图注
            lines.append(f"*{text}*")
        elif TABCAP_RE.match(text):  # 附表标题
            lines.append(f"**{text}**")
        else:
            lines.append(text)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def table_to_md(rows):
    ncols = max(sum(c["colspan"] for c in row) for row in rows)
    lines = []
    for ri, row in enumerate(rows):
        cells = [""] * ncols
        c = 0
        for cell in row:
            cells[c] = cell["text"].replace("|", "\\|")
            c += cell["colspan"]
        lines.append("| " + " | ".join(cells) + " |")
        if ri == 0:
            lines.append("|" + " --- |" * ncols)
    return lines


def save_docx(path, title, author, blocks):
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.shared import Inches, Pt

    doc = Document()
    style = doc.styles["Normal"]
    style.font.name = "Times New Roman"
    style.font.size = Pt(12)
    style.font._element.rPr.rFonts.set(qn("w:eastAsia"), "微软雅黑")

    h = doc.add_paragraph()
    h.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = h.add_run(title)
    run.bold = True
    run.font.size = Pt(18)

    a = doc.add_paragraph()
    a.alignment = WD_ALIGN_PARAGRAPH.CENTER
    a.add_run(author)

    in_head = True
    for blk in blocks:
        if blk["type"] == "table":
            add_docx_table(doc, blk["rows"])
            continue
        text, imgs = blk["text"], blk["imgs"]
        for url in imgs:
            data = None
            for attempt in range(3):  # 代理链路不稳定，失败重试
                try:
                    data = requests.get(url, headers=HEADERS, proxies=None, timeout=60).content
                    break
                except requests.RequestException as e:
                    print(f"  [warn] 图片下载失败(第{attempt + 1}次): {e}")
            if data is None:
                continue
            try:
                tmp = os.path.join(OUT_DIR, "_tmp_img")
                with open(tmp, "wb") as f:
                    f.write(data)
                doc.add_picture(tmp, width=Inches(6))
                doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
                os.remove(tmp)
            except Exception as e:
                print(f"  [warn] 图片嵌入失败 {url}: {e}")
        if not text:
            continue
        if in_head and SECTION_RE.match(text):
            in_head = False
        para = doc.add_paragraph()
        run = para.add_run(text)
        if in_head:  # 大字标题
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run.bold = True
            run.font.size = Pt(15)
        elif SECTION_RE.match(text):  # 章节
            run.bold = True
            run.font.size = Pt(14)
        elif SUBSECTION_RE.match(text):  # 小节
            run.bold = True
            run.font.size = Pt(12)
        elif FIGCAP_RE.match(text):  # 图注
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run.font.size = Pt(10)
        elif TABCAP_RE.match(text):  # 附表标题
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run.bold = True
            run.font.size = Pt(10.5)
        else:
            para.paragraph_format.first_line_indent = Pt(24)

    try:
        doc.save(path)
        return path
    except PermissionError:
        alt = re.sub(r"\.docx$", "_new.docx", path)
        doc.save(alt)
        print(f"  [warn] {os.path.basename(path)} 被占用（可能正在 Word 中打开），已另存为 {os.path.basename(alt)}")
        return alt


def add_docx_table(doc, rows):
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Pt

    ncols = max(sum(c["colspan"] for c in row) for row in rows)
    table = doc.add_table(rows=len(rows), cols=ncols)
    table.style = "Table Grid"
    for r, row in enumerate(rows):
        c = 0
        for cell in row:
            span = max(1, min(cell["colspan"], ncols - c))
            anchor = table.cell(r, c)
            merged = anchor.merge(table.cell(r, c + span - 1)) if span > 1 else anchor
            merged.text = cell["text"]
            for para in merged.paragraphs:
                para.alignment = WD_ALIGN_PARAGRAPH.CENTER
                for run in para.runs:
                    run.font.size = Pt(10.5)
            c += span
    return table


def main():
    sys.stdout.reconfigure(errors="replace")  # 兼容 GBK 控制台
    target = parse_target(ISSUE_DATE)
    label = ISSUE_DATE
    print(f"抓取周报: {label}")
    data_id, html = fetch_issue(target)
    if not html:
        print(f"[error] 未找到 {label} 的《全国农业气象周报》（接口无该期数据）")
        sys.exit(1)
    parser = AgroWeeklyParser()
    parser.feed(html)
    parser.close()
    title = parser.title or "全国农业气象周报"
    author = re.sub(r"(?<!\s)签发：", "  签发：", parser.author)  # 落款粘连处补空格
    author = re.sub(r"(\d)\s+(年|月|日)", r"\1\2", author)  # 去掉“2026 年”中的空格
    author = re.sub(r"(年|月)\s+(?=\d)", r"\1", author)  # 去掉“年 10月”中的空格
    blocks = [b for b in parser.blocks if b["type"] == "table" or b["text"] or b["imgs"]]
    if not blocks:
        print("[error] 未解析到正文，接口返回结构可能已变化。")
        sys.exit(1)
    n_img = sum(len(b["imgs"]) for b in blocks if b["type"] == "p")
    n_tab = sum(1 for b in blocks if b["type"] == "table")
    n_para = sum(1 for b in blocks if b["type"] == "p")
    print(f"  {issue_date_of(data_id)} 发布 | 段落 {n_para} | 表格 {n_tab} | 图片 {n_img}")

    base = os.path.join(OUT_DIR, f"{title}_{issue_date_of(data_id).replace('-', '')}")
    md_path = base + ".md"
    with io.open(md_path, "w", encoding="utf-8") as f:
        f.write(build_markdown(title, author, blocks))
    print(f"已保存 Markdown: {md_path}")

    try:
        import docx  # noqa: F401
    except ImportError:
        print("未安装 python-docx，跳过 Word 输出 (pip install python-docx 后重试可生成 docx)")
        return
    docx_path = save_docx(base + ".docx", title, author, blocks)
    print(f"已保存 Word: {docx_path}")


if __name__ == "__main__":
    main()
