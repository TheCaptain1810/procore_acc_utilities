from __future__ import annotations

import argparse
import json
from collections import Counter, OrderedDict
from pathlib import Path
import sys


def _normalize_name(name: str | None) -> str | None:
	if name is None:
		return None
	# Collapse internal whitespace and trim
	cleaned = " ".join(str(name).split()).strip()
	return cleaned or None


def extract_unique_managers(items: list[dict]) -> tuple[list[str], dict[str, int]]:
	"""
	Given a list of RFI dicts, return:
	  - unique manager names (first-seen casing), sorted case-insensitively
	  - counts per normalized (casefolded) name
	"""
	seen_map: OrderedDict[str, str] = OrderedDict()  # key: normalized (casefold), val: original cleaned
	counts: Counter[str] = Counter()

	for rec in items:
		mgr = rec.get("rfi_manager")
		name = None
		if isinstance(mgr, dict):
			name = mgr.get("name")
		elif isinstance(mgr, str):
			name = mgr

		cleaned = _normalize_name(name)
		if not cleaned:
			continue

		key = cleaned.casefold()
		counts[key] += 1
		if key not in seen_map:
			seen_map[key] = cleaned

	unique = sorted(seen_map.values(), key=lambda s: s.casefold())
	# Map counts back to the displayed names
	counts_by_display = {seen_map[k]: counts[k] for k in seen_map}
	return unique, counts_by_display


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description="List unique RFI manager names from rfi_response.json")
	parser.add_argument(
		"-f",
		"--file",
		type=Path,
		default=Path(__file__).with_name("rfi_response.json"),
		help="Path to rfi_response.json (defaults to sibling file)",
	)
	parser.add_argument("--counts", action="store_true", help="Show occurrence counts for each manager")
	parser.add_argument("--json", action="store_true", help="Output JSON instead of plain text")
	args = parser.parse_args(argv)

	json_path: Path = args.file
	if not json_path.exists():
		print(f"Error: file not found: {json_path}", file=sys.stderr)
		return 1

	try:
		with json_path.open("r", encoding="utf-8") as f:
			data = json.load(f)
	except json.JSONDecodeError as e:
		print(f"Error: failed to parse JSON ({json_path}): {e}", file=sys.stderr)
		return 1

	if not isinstance(data, list):
		print("Error: expected a JSON array of RFIs", file=sys.stderr)
		return 1

	uniques, counts = extract_unique_managers(data)

	if args.json:
		if args.counts:
			print(json.dumps({"unique": uniques, "counts": counts}, ensure_ascii=False, indent=2))
		else:
			print(json.dumps(uniques, ensure_ascii=False, indent=2))
		return 0

	if args.counts:
		for name in uniques:
			print(f"{counts.get(name, 0):3d}  {name}")
	else:
		for name in uniques:
			print(name)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())

