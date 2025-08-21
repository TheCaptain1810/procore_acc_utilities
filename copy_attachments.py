from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
import string


# Hardcoded paths
# Adjust these two lines as needed
SRC_DIR = Path("E:/ProcoreDownloadsMigration")  # Source folder to scan recursively
DEST_DIR = Path("E:/ProcoreToACCExported/Submittals_Final")  # Destination folder (flat)


def sanitize_filename(name: str, max_length: int = 200) -> str:
	valid_chars = f"-_.() {string.ascii_letters}{string.digits}"
	cleaned = "".join(c for c in name if c in valid_chars).strip()
	if not cleaned:
		cleaned = "file"
	base, ext = os.path.splitext(cleaned)
	if len(cleaned) > max_length:
		cleaned = base[: max(1, max_length - len(ext))] + ext
	return cleaned


def unique_destination_path(dest_dir: Path, filename: str) -> Path:
	dest = dest_dir / filename
	if not dest.exists():
		return dest
	base, ext = os.path.splitext(filename)
	i = 1
	while True:
		candidate = dest_dir / f"{base}_{i}{ext}"
		if not candidate.exists():
			return candidate
		i += 1


def copy_all_files(src_folder: Path, dest_dir: Path) -> tuple[int, int]:
	"""
	Copy all files from src_folder (recursively) into dest_dir (flat).
	Returns (copied_count, skipped_count)
	"""
	copied = 0
	skipped = 0
	src_folder = src_folder.resolve()
	dest_dir = dest_dir.resolve()

	# Fallback check for older Python versions
	def _is_subpath(child: Path, parent: Path) -> bool:
		try:
			child.relative_to(parent)
			return True
		except Exception:
			return False

	for root, dirs, files in os.walk(src_folder):
		root_path = Path(root)
		# Avoid walking into destination directory if it's inside the source
		dirs[:] = [d for d in dirs if not _is_subpath(root_path / d, dest_dir)]
		for fname in files:
			src_path = root_path / fname
			# Skip if file lives under destination (self-copy)
			if _is_subpath(src_path, dest_dir):
				skipped += 1
				continue
			safe_name = sanitize_filename(fname)
			dest_path = unique_destination_path(dest_dir, safe_name)
			dest_path.parent.mkdir(parents=True, exist_ok=True)
			try:
				shutil.copy2(src_path, dest_path)
				print(f"✅ Copied: {src_path} -> {dest_path}")
				copied += 1
			except Exception as e:
				print(f"❌ Failed: {src_path} -> {dest_path} ({e})")
				skipped += 1
	return copied, skipped


def main(argv: list[str] | None = None) -> int:
	src_path = SRC_DIR
	if not src_path.exists() or not src_path.is_dir():
		print(f"Error: Source folder does not exist or is not a directory: {src_path}")
		return 1

	print(f"Source (hardcoded): {src_path}")
	print(f"Destination (hardcoded): {DEST_DIR}")
	DEST_DIR.mkdir(parents=True, exist_ok=True)

	copied, skipped = copy_all_files(src_path, DEST_DIR)
	print(f"\nDone. Copied: {copied}, Skipped: {skipped}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

