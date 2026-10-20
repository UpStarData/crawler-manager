#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把本目录下的视频上传到阿里云 OSS，并返回每个视频的公网 URL。

视频来源支持两种：
1. 目录下直接放置的视频文件（mp4/mov/mkv 等）；
2. 目录下的 zip 压缩包，脚本会自动解压其中的视频后上传。

zip 文件名里的区间（如 `农业视频_360p_01-10.zip`、`11-15`）会被解析成
“压缩包内应有的视频个数”（10、5……），上传前与实际视频数做校验。

依赖：
    pip install oss2

凭证按 CLI 参数 > 环境变量 > oss_config.json 的顺序读取：
    OSS_ACCESS_KEY_ID
    OSS_ACCESS_KEY_SECRET
    OSS_ENDPOINT         例如 oss-cn-hangzhou.aliyuncs.com
    OSS_BUCKET
    OSS_PREFIX           对象 key 前缀，默认 agri-videos/
    OSS_URL_BASE         自定义访问域名（CDN 等），可选

示例：
    set OSS_ACCESS_KEY_ID=LTAI...
    set OSS_ACCESS_KEY_SECRET=...
    python upload_videos_to_oss.py --endpoint oss-cn-hangzhou.aliyuncs.com --bucket my-bucket

    # 只检查文件、打印将生成的 URL，不连 OSS
    python upload_videos_to_oss.py --dry-run --endpoint oss-cn-hangzhou.aliyuncs.com --bucket my-bucket

    # 已上传过的对象也重新上传
    python upload_videos_to_oss.py ... --overwrite

OSS Key 默认布局（--key-layout quality）：
    agri-videos/360p/01.mp4
如需平铺或按压缩包分目录，可用 --key-layout flat / zip。
"""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import re
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

try:
    import oss2
    import requests
except ImportError as exc:  # pragma: no cover - 仅在缺少依赖时触发
    raise SystemExit(f"缺少依赖 {exc.name}，请先执行: pip install oss2 requests") from exc


VIDEO_EXTS = {
    ".mp4", ".mov", ".avi", ".mkv", ".flv", ".wmv", ".webm",
    ".m4v", ".mpeg", ".mpg", ".ts",
}

RANGE_RE = re.compile(r"(\d+)\s*[-~—–至]\s*(\d+)")
COUNT_RE = re.compile(r"(\d+)\s*个")
QUALITY_RE = re.compile(r"(?<!\d)(\d{3,4}p)(?!\d)", re.IGNORECASE)

DEFAULT_PREFIX = "agri-videos/"


@dataclass
class VideoItem:
    """一个待上传的视频。"""

    source: Path                 # zip 路径，或直接放置的视频路径
    name: str                    # 视频文件名
    size: int
    key: str                     # OSS 对象 key
    archive_name: str | None = None   # zip 内的相对路径
    local_path: Path | None = None    # 直接放置的视频路径


@dataclass
class UploadResult:
    source: str
    expected_count: int | None
    actual_count: int | None
    video: str
    size: int
    key: str
    url: str
    status: str
    error: str = ""


@dataclass
class Config:
    access_key_id: str = ""
    access_key_secret: str = ""
    endpoint: str = ""
    bucket: str = ""
    prefix: str = DEFAULT_PREFIX
    url_base: str = ""


def is_video_name(name: str) -> bool:
    path = Path(name)
    if path.name.startswith("._") or "__MACOSX" in path.parts:
        return False
    return path.suffix.lower() in VIDEO_EXTS


def expected_count_from_name(stem: str) -> int | None:
    """从 zip 文件名解析视频个数：优先区间 01-10，其次 “10个”。"""
    match = RANGE_RE.search(stem)
    if match:
        start, end = int(match.group(1)), int(match.group(2))
        if 0 <= start <= end:
            return end - start + 1
    match = COUNT_RE.search(stem)
    if match:
        return int(match.group(1))
    return None


def quality_from_name(name: str) -> str | None:
    match = QUALITY_RE.search(name)
    return match.group(1).lower() if match else None


def normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip().strip("/")
    return f"{prefix}/" if prefix else ""


def make_key(
    prefix: str,
    layout: str,
    name: str,
    source_stem: str,
    archive_name: str | None,
) -> str:
    prefix = normalize_prefix(prefix)
    if layout == "flat":
        return f"{prefix}{name}"
    if layout == "zip":
        rel = archive_name or name
        return f"{prefix}{source_stem}/{rel}"
    quality = quality_from_name(source_stem)
    return f"{prefix}{quality}/{name}" if quality else f"{prefix}{name}"


def collect_items(folder: Path, prefix: str, layout: str) -> list[VideoItem]:
    items: list[VideoItem] = []

    # 直接放在目录下的视频
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTS and is_video_name(path.name):
            items.append(
                VideoItem(
                    source=path,
                    name=path.name,
                    size=path.stat().st_size,
                    key=make_key(prefix, layout, path.name, path.stem, None),
                    local_path=path,
                )
            )

    # zip 内的视频
    for path in sorted(folder.glob("*.zip")):
        if not path.is_file():
            continue
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                if info.is_dir() or not is_video_name(info.filename):
                    continue
                rel = info.filename.replace("\\", "/").lstrip("/")
                name = Path(rel).name
                items.append(
                    VideoItem(
                        source=path,
                        name=name,
                        size=info.file_size,
                        key=make_key(prefix, layout, name, path.stem, rel),
                        archive_name=rel,
                    )
                )
    return items


def duplicate_keys(items: Iterable[VideoItem]) -> dict[str, list[VideoItem]]:
    seen: dict[str, list[VideoItem]] = {}
    for item in items:
        seen.setdefault(item.key, []).append(item)
    return {key: group for key, group in seen.items() if len(group) > 1}


def build_url(endpoint: str, bucket: str, key: str, url_base: str = "") -> str:
    encoded = quote(key, safe="/")
    if url_base:
        return f"{url_base.rstrip('/')}/{encoded}"
    if endpoint.startswith(("http://", "https://")):
        scheme, host = endpoint.split("://", 1)
    else:
        scheme, host = "https", endpoint
    host = host.rstrip("/")
    return f"{scheme}://{bucket}.{host}/{encoded}"


def load_config(path: Path) -> Config:
    config = Config()
    if path.is_file():
        with path.open(encoding="utf-8-sig") as fh:
            data = json.load(fh)
        config.access_key_id = str(data.get("access_key_id", ""))
        config.access_key_secret = str(data.get("access_key_secret", ""))
        config.endpoint = str(data.get("endpoint", ""))
        config.bucket = str(data.get("bucket", ""))
        config.prefix = str(data.get("prefix", DEFAULT_PREFIX))
        config.url_base = str(data.get("url_base", ""))
    return config


def resolve_config(args: argparse.Namespace) -> Config:
    config = load_config(Path(args.config))
    config.access_key_id = args.access_key_id or os.getenv("OSS_ACCESS_KEY_ID", "") or config.access_key_id
    config.access_key_secret = args.access_key_secret or os.getenv("OSS_ACCESS_KEY_SECRET", "") or config.access_key_secret
    config.endpoint = args.endpoint or os.getenv("OSS_ENDPOINT", "") or config.endpoint
    config.bucket = args.bucket or os.getenv("OSS_BUCKET", "") or config.bucket
    config.prefix = args.prefix if args.prefix is not None else (os.getenv("OSS_PREFIX") or config.prefix)
    config.url_base = args.url_base if args.url_base is not None else (os.getenv("OSS_URL_BASE") or config.url_base)
    return config


def normalize_endpoint(endpoint: str) -> str:
    """未写协议时默认走 HTTPS，避免明文 HTTP 通道被中间设备干扰。"""
    endpoint = endpoint.strip()
    if endpoint and not endpoint.startswith(("http://", "https://")):
        return f"https://{endpoint}"
    return endpoint


def make_bucket(config: Config) -> "oss2.Bucket":
    auth = oss2.Auth(config.access_key_id, config.access_key_secret)
    return oss2.Bucket(auth, normalize_endpoint(config.endpoint), config.bucket, connect_timeout=30)


def upload_one(
    bucket: "oss2.Bucket",
    item: VideoItem,
    path: Path,
    overwrite: bool,
    retries: int = 3,
) -> str:
    """上传单个文件，返回 uploaded / skipped。失败时抛异常。"""
    if not overwrite and bucket.object_exists(item.key):
        return "skipped"

    content_type = mimetypes.guess_type(item.name)[0] or "application/octet-stream"
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            bucket.put_object_from_file(item.key, str(path), headers={"Content-Type": content_type})
            return "uploaded"
        except oss2.exceptions.OssError as exc:
            last_error = exc
            # 400/401/403 这类错误重试没有意义
            if exc.status and 400 <= exc.status < 500:
                raise
        except (requests.RequestException, OSError) as exc:
            last_error = exc
        if attempt < retries:
            wait = min(2 ** attempt, 10)
            print(f"      第 {attempt} 次上传失败，{wait}s 后重试：{last_error}", flush=True)
            time.sleep(wait)
    assert last_error is not None
    raise last_error


def extract_video(zf: zipfile.ZipFile, item: VideoItem, work_dir: Path) -> Path:
    assert item.archive_name is not None
    target = Path(zf.extract(item.archive_name, work_dir))
    if not target.is_file():
        raise FileNotFoundError(f"解压后未找到视频：{item.archive_name}")
    return target


def format_size(num: int) -> str:
    value = float(num)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def write_csv(results: list[UploadResult], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["压缩包/来源", "名称标注视频数", "实际视频数", "视频文件", "字节数", "OSS Key", "URL", "状态", "错误信息"]
        )
        for item in results:
            writer.writerow(
                [
                    item.source,
                    "" if item.expected_count is None else item.expected_count,
                    "" if item.actual_count is None else item.actual_count,
                    item.video,
                    item.size,
                    item.key,
                    item.url,
                    item.status,
                    item.error,
                ]
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="把目录下的视频（含 zip 内的视频）上传到阿里云 OSS，并输出 URL。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--folder", default=str(Path(__file__).resolve().parent), help="视频/zip 所在目录")
    parser.add_argument("--config", default=str(Path(__file__).resolve().parent / "oss_config.json"), help="JSON 配置文件路径")
    parser.add_argument("--access-key-id", default="", help="OSS AccessKeyId（也可用 OSS_ACCESS_KEY_ID）")
    parser.add_argument("--access-key-secret", default="", help="OSS AccessKeySecret（也可用 OSS_ACCESS_KEY_SECRET）")
    parser.add_argument("--endpoint", default="", help="OSS Endpoint，如 oss-cn-hangzhou.aliyuncs.com")
    parser.add_argument("--bucket", default="", help="Bucket 名称")
    parser.add_argument("--prefix", default=None, help=f"对象 key 前缀，默认 {DEFAULT_PREFIX}")
    parser.add_argument("--url-base", default=None, help="自定义访问域名（CDN/自定义域名），默认按 bucket+endpoint 拼接")
    parser.add_argument(
        "--key-layout",
        choices=["quality", "flat", "zip"],
        default="quality",
        help="key 布局：quality=前缀/360p/01.mp4，flat=前缀/01.mp4，zip=前缀/压缩包名/01.mp4",
    )
    parser.add_argument("--overwrite", action="store_true", help="对象已存在时也重新上传")
    parser.add_argument("--dry-run", action="store_true", help="只解析并打印结果，不连接 OSS")
    parser.add_argument("--keep-extracted", action="store_true", help="保留解压出的视频文件到 目录/_extracted")
    parser.add_argument("--result-file", default="", help="结果 CSV 路径，默认 目录/oss_upload_result.csv")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    folder = Path(args.folder).resolve()
    if not folder.is_dir():
        print(f"目录不存在：{folder}", file=sys.stderr)
        return 2

    config = resolve_config(args)
    items = collect_items(folder, config.prefix, args.key_layout)
    if not items:
        print(f"目录中没有找到视频或含视频的 zip：{folder}")
        return 0

    # 同一个 key 对应多个文件时会互相覆盖，直接报错
    duplicates = duplicate_keys(items)
    if duplicates:
        print("发现 OSS Key 冲突（会互相覆盖），请调整 --key-layout 或 --prefix：", file=sys.stderr)
        for key, group in duplicates.items():
            names = ", ".join(f"{i.source.name}:{i.name}" for i in group)
            print(f"  {key} <- {names}", file=sys.stderr)
        return 2

    missing = [name for name, value in {
        "endpoint": config.endpoint,
        "bucket": config.bucket,
    }.items() if not value]
    if not args.dry_run:
        for name, value in {
            "access_key_id": config.access_key_id,
            "access_key_secret": config.access_key_secret,
        }.items():
            if not value:
                missing.append(name)
    if missing:
        print(f"缺少 OSS 配置：{', '.join(missing)}", file=sys.stderr)
        print(
            "请通过 CLI 参数、环境变量（OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / "
            "OSS_ENDPOINT / OSS_BUCKET）或 oss_config.json 提供。",
            file=sys.stderr,
        )
        return 2

    # 按来源统计，并校验 zip 名称里的数量
    counts: dict[Path, tuple[int | None, int]] = {}
    for item in items:
        expected = expected_count_from_name(item.source.stem) if item.source.suffix.lower() == ".zip" else None
        actual = counts.get(item.source, (expected, 0))[1] + 1
        counts[item.source] = (expected, actual)

    print(f"目录：{folder}")
    for source in sorted(counts, key=lambda p: p.name):
        expected, actual = counts[source]
        if expected is None:
            print(f"  {source.name}: 实际 {actual} 个视频")
        elif expected == actual:
            print(f"  {source.name}: 名称标注 {expected} 个，实际 {actual} 个 ✓")
        else:
            print(f"  {source.name}: 名称标注 {expected} 个，实际 {actual} 个（不一致，仍继续上传）")
    print(f"共 {len(items)} 个视频，合计 {format_size(sum(i.size for i in items))}\n")

    if args.dry_run:
        for index, item in enumerate(items, 1):
            url = build_url(config.endpoint, config.bucket, item.key, config.url_base)
            print(f"[{index:02d}/{len(items)}] {item.name}  {format_size(item.size)}  ->  {url}")
        print(f"\n干跑完成：将上传 {len(items)} 个视频（未连接 OSS）。")
        return 0

    bucket = make_bucket(config)
    results: list[UploadResult] = []
    work_root: Path | str
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.keep_extracted:
        work_root = folder / "_extracted"
        work_root.mkdir(exist_ok=True)
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="oss_upload_")
        work_root = temp_dir.name

    try:
        for index, item in enumerate(items, 1):
            expected, actual = counts[item.source]
            url = build_url(config.endpoint, config.bucket, item.key, config.url_base)
            prefix = f"[{index:02d}/{len(items)}]"
            extracted: Path | None = None
            try:
                if item.local_path is not None:
                    local_path = item.local_path
                else:
                    with zipfile.ZipFile(item.source) as zf:
                        target_root = Path(work_root) / item.source.stem
                        target_root.mkdir(parents=True, exist_ok=True)
                        extracted = extract_video(zf, item, target_root)
                    local_path = extracted

                status = upload_one(bucket, item, local_path, overwrite=args.overwrite)
                results.append(
                    UploadResult(
                        source=item.source.name,
                        expected_count=expected,
                        actual_count=actual,
                        video=item.name,
                        size=item.size,
                        key=item.key,
                        url=url,
                        status=status,
                    )
                )
                label = "已存在，跳过" if status == "skipped" else "上传成功"
                print(f"{prefix} {item.name}  {format_size(item.size)}  [{label}]  {url}", flush=True)
            except Exception as exc:  # noqa: BLE001 - 单个失败不影响后续文件
                results.append(
                    UploadResult(
                        source=item.source.name,
                        expected_count=expected,
                        actual_count=actual,
                        video=item.name,
                        size=item.size,
                        key=item.key,
                        url=url,
                        status="failed",
                        error=str(exc),
                    )
                )
                print(f"{prefix} {item.name}  [失败]  {exc}", file=sys.stderr, flush=True)
            finally:
                if extracted is not None and not args.keep_extracted:
                    try:
                        extracted.unlink()
                    except OSError:
                        pass
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()

    result_file = Path(args.result_file) if args.result_file else folder / "oss_upload_result.csv"
    write_csv(results, result_file)

    uploaded = sum(1 for r in results if r.status == "uploaded")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = sum(1 for r in results if r.status == "failed")
    print(f"\n完成：成功 {uploaded} 个，跳过 {skipped} 个，失败 {failed} 个。")
    print(f"结果已写入：{result_file}")
    if failed:
        print("失败项详见结果 CSV 的“错误信息”列。", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
