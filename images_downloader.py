
import json
import os
import re
import sys
import mimetypes
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, Optional
from urllib import request, parse, error
from collections import defaultdict


def sanitize_filename(name: str, replacement: str = "_") -> str:
    """Sanitize a filename or folder name for Windows and POSIX.

    - Replaces invalid characters: <>:\"/\\|?*
    - Strips leading/trailing whitespace and dots
    - Collapses repeated spaces/underscores
    """
    # Replace invalid filename characters
    name = re.sub(r'[<>:"/\\|?*]', replacement, name)
    # Remove control chars
    name = re.sub(r"[\x00-\x1f]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Avoid trailing dots/spaces on Windows
    name = name.rstrip(" .")
    if not name:
        name = "untitled"
    return name


def colon_lookalike(text: str) -> str:
    """Replace ASCII ':' with a Unicode lookalike (U+A789 MODIFIER LETTER COLON)
    to keep the visual format '11:39:35' on Windows filenames where ':' is illegal.
    """
    return text.replace(":", "꞉")


def get_first(d: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def parse_created_at(value: Any) -> Optional[datetime]:
    """Parse a created-at value into a timezone-aware UTC datetime if possible.

    Supports:
    - UNIX seconds or milliseconds (int/float)
    - ISO 8601 strings with or without timezone (Z or offset)
    - Common date-time string formats
    """
    if value is None:
        return None

    # Numeric timestamp (seconds or milliseconds)
    if isinstance(value, (int, float)):
        try:
            # Heuristic: treat > 10^12 as milliseconds
            if value > 1_000_000_000_000:
                value = value / 1000.0
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            return dt
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Try ISO 8601
        try:
            # Replace trailing Z with +00:00 for fromisoformat
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            pass

        # Try a few common formats
        fmts = [
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt
            except Exception:
                continue
    return None


def make_date_suffix(dt: Optional[datetime], original_value: Any = None) -> str:
    """Create a human-readable date suffix.

    Format requested: Month_DD_YYYY-hh:mm:ss AM/PM (e.g., September_11_2024-11:40:27 AM)
    If time not present, use: Month_DD_YYYY
    Note: On Windows, ':' is not permitted in filenames; we later sanitize to '-'.
    """
    if dt is None:
        return "unknown-date"
    # Convert to IST (UTC+05:30) before formatting
    IST = timezone(timedelta(hours=5, minutes=30))
    dt_local = dt.astimezone(IST)

    has_time = not (dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0)
    # If the original string clearly had time info, keep time in suffix
    if isinstance(original_value, str) and re.search(r"\d[T\s]\d", original_value):
        has_time = True
    if has_time:
        # 12-hour with AM/PM in IST
        return dt_local.strftime("%B_%d_%Y-%I:%M:%S %p")
    return dt_local.strftime("%B_%d_%Y")


def ensure_unique_path(base_dir: Path, base_name: str, date_suffix: str, ext: str) -> Path:
    """Return a unique file path by inserting (n) before the date suffix if needed.

    Example: base_name="image", date_suffix="2025-08-11", ext=".jpg"
    - image_2025-08-11.jpg
    - image (1)_2025-08-11.jpg
    - image (2)_2025-08-11.jpg
    """
    n = 0
    while True:
        name_part = base_name if n == 0 else f"{base_name} ({n})"
        # Build filename, convert ':' to a safe lookalike, then sanitize for Windows
        raw_filename = f"{name_part}_{date_suffix}{ext}"
        raw_filename = colon_lookalike(raw_filename)
        filename = sanitize_filename(raw_filename, replacement="-")
        candidate = base_dir / filename
        if not candidate.exists():
            return candidate
        n += 1


def guess_extension(filename: str, url: str, content_type: Optional[str]) -> str:
    # 1) from provided filename
    ext = Path(filename).suffix
    if ext:
        return ext
    # 2) from URL path
    try:
        url_path = parse.urlparse(url).path
        ext = Path(url_path).suffix
        if ext:
            return ext
    except Exception:
        pass
    # 3) from content-type
    if content_type:
        ctype = content_type.split(";")[0].strip().lower()
        ext = mimetypes.guess_extension(ctype) or ""
        if ext:
            return ext
    # fallback
    return ".bin"


def stream_download(url: str, dest_path: Path) -> None:
    """Download a URL to dest_path, streaming to avoid large memory usage."""
    headers = {"User-Agent": "images-downloader/1.0 (+https://github.com)"}
    req = request.Request(url, headers=headers)
    with request.urlopen(req, timeout=60) as resp:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            while True:
                chunk = resp.read(8192)
                if not chunk:
                    break
                f.write(chunk)


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "images.json"

    # Limit: download only N images per category
    MAX_PER_CATEGORY = 400

    if not json_path.exists():
        print(f"images.json not found at {json_path}")
        return 1

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            images = json.load(f)
    except Exception as e:
        print(f"Failed to read images.json: {e}")
        return 1

    if not isinstance(images, list):
        print("images.json is not a list")
        return 1

    category_keys = [
        "category",
        "image_category_name",
        "imageCategoryName",
        "image_category",
        "folder",
        "category_name",
    ]
    url_keys = ["url", "download_url", "href", "link"]
    filename_keys = ["filename", "file_name", "name", "title"]
    created_keys = [
        "created_at",
        "createdAt",
        "created",
        "created_date",
        "createdDate",
        "date",
    ]

    total = len(images)
    success = 0
    skipped = 0
    failed = 0
    per_category_count: Dict[str, int] = defaultdict(int)

    print(f"Found {total} images in images.json")

    for idx, item in enumerate(images, start=1):
        try:
            if not isinstance(item, dict):
                skipped += 1
                print(f"[{idx}/{total}] Skipping non-object entry")
                continue

            category_val = get_first(item, category_keys) or "Uncategorized"
            url_val = get_first(item, url_keys)
            filename_val = get_first(item, filename_keys)
            created_val = get_first(item, created_keys)

            if not url_val or not filename_val:
                skipped += 1
                print(f"[{idx}/{total}] Missing url or filename -> skipped")
                continue

            category = sanitize_filename(str(category_val))

            # Enforce per-category limit
            if per_category_count[category] >= MAX_PER_CATEGORY:
                skipped += 1
                print(
                    f"[{idx}/{total}] Skipping (limit {MAX_PER_CATEGORY} reached for category '{category}')"
                )
                continue
            url_str = str(url_val)
            orig_filename = sanitize_filename(str(filename_val))

            # Parse created date and build suffix
            dt = parse_created_at(created_val)
            date_suffix = make_date_suffix(dt, created_val)

            # Compute ext
            ext = guess_extension(orig_filename, url_str, None)
            # Derive base name without extension from provided filename
            base_name = Path(orig_filename).stem
            base_name = sanitize_filename(base_name)

            target_dir = base_dir / category
            target_dir.mkdir(parents=True, exist_ok=True)

            # We may want to peek content-type to refine extension; use a HEAD-like GET
            headers = {"User-Agent": "images-downloader/1.0 (+https://github.com)"}
            req = request.Request(url_str, headers=headers)
            try:
                with request.urlopen(req, timeout=10) as resp:
                    ct = resp.headers.get("Content-Type")
                    # If extension unknown, refine
                    if ext == ".bin":
                        ext = guess_extension(orig_filename, url_str, ct)
            except Exception:
                # If peek failed, we'll still try to download later; keep ext
                pass

            # Build unique path
            out_path = ensure_unique_path(target_dir, base_name, date_suffix, ext)

            # Download
            print(f"[{idx}/{total}] Downloading -> {out_path.relative_to(base_dir)}")
            try:
                stream_download(url_str, out_path)
            except error.HTTPError as he:
                failed += 1
                print(f"    HTTP error {he.code}: {he.reason}")
                # Clean up partial
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except Exception:
                        pass
                continue
            except Exception as e:
                failed += 1
                print(f"    Failed: {e}")
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except Exception:
                        pass
                continue

            success += 1
            per_category_count[category] += 1
        except KeyboardInterrupt:
            print("Interrupted by user")
            break
        except Exception as e:
            failed += 1
            print(f"[{idx}/{total}] Unexpected error: {e}")

    print(
        f"Done. Success: {success}, Skipped: {skipped}, Failed: {failed}, Total processed: {success+skipped+failed}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())