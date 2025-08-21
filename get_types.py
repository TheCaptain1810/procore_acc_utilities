from __future__ import annotations

import argparse
import json
from collections import Counter, OrderedDict
from pathlib import Path
import sys


def _normalize(value: str | None) -> str | None:
	if value is None:
		return None
	cleaned = " ".join(str(value).split()).strip()
	return cleaned or None


def extract_unique_types(items: list[dict]) -> tuple[list[str], dict[str, int]]:
	"""
	From a list of Submittal dicts, extract unique type names and counts.
	Priority of fields when type is a dict: translated_name, name, display_name.
	"""
	seen: OrderedDict[str, str] = OrderedDict()  # key: casefolded, val: display
	counts: Counter[str] = Counter()

	for rec in items:
		t = rec.get("type")
		name = None
		if isinstance(t, dict):
			name = t.get("translated_name") or t.get("name") or t.get("display_name")
		elif isinstance(t, str):
			name = t

		cleaned = _normalize(name)
		if not cleaned:
			continue

		key = cleaned.casefold()
		counts[key] += 1
		if key not in seen:
			seen[key] = cleaned

	unique_list = sorted(seen.values(), key=lambda s: s.casefold())
	counts_by_display = {seen[k]: counts[k] for k in seen}
	return unique_list, counts_by_display


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description="List unique Submittal types from submittals_response.json")
	parser.add_argument(
		"-f",
		"--file",
		type=Path,
		default=Path(__file__).with_name("submittals_response.json"),
		help="Path to submittals_response.json (defaults to sibling file)",
	)
	parser.add_argument("--counts", action="store_true", help="Show occurrence counts for each type")
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
		print("Error: expected a JSON array of Submittals", file=sys.stderr)
		return 1

	uniques, counts = extract_unique_types(data)

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

