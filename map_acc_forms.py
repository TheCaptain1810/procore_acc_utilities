import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests


# =========================
# Hardcoded configuration
# =========================
# NOTE: Replace the token with a valid Autodesk Construction Cloud (ACC) OAuth token.
# Per request, values are hardcoded (not read from env/args).
ACC_BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlZiakZvUzhQU3lYODQyMV95dndvRUdRdFJEa19SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoiczRuNTYyQjY1UzAzUnFQaGVwZWR0Q2dGN3hkWFBHdXFsUFRDUUNIbXEwbTRsM3pVRHhKUzJjUUZoODhEeWg0RiIsInVzZXJpZCI6IlBMVUpYVTJFUERQRlJOUk0iLCJleHAiOjE3NTU3NzQ0MjZ9.U6EDBwc0mXVvYE5aa_aYMLJicnD3sMpin8DUXZ4arPHgS3jSRJ2efn3-ME8AWtgGBZhGHCwXq-NhZXtf35HwI2FclBcT_WJpoc6e3HhAykzcN94pcDj_TOtQLkX-CwHbGYvM91nEiF8_L-ns13UWAMakM8dVas4EDmauARKSPeGrIh6LD_09x3byqcgTjPXxqk-Pvrmt9Q1Tatlv3n5aNSltMGdLnzjMU-K1YV2B011IuT7qIwgzSvSBxPS7iZwjYH6C0zXwdOr9tAt4i9VPCV2gQ1gnUAW_XrT8X_pYYAOltWtb-NS9_xN-8kS1oKxxRzivGJO3YuUpt4J3_b8YTA"

# ACC project and forms template context (from the prompt)
ACC_PROJECT_ID = "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"
ACC_FORM_TEMPLATE_ID = "72b9c3e9-a5b8-59d1-9a3d-77daaffac1d0"

# API base and paging
ACC_BASE_URL = "https://developer.api.autodesk.com/construction/forms/development/v2"
PAGE_LIMIT = 50
MAX_PAGES = 200  # safety cap in case of unexpected loops

# Filtering/sort (kept to match prompt URL)
INCLUDE_PARAMS = [
	("assigneeIncludeMembers", "true"),
	("include", "inactiveFormTemplates"),
	("include", "layoutInfo"),
	("include", "sublocations"),
]
STATUSES = ["inProgress", "inReview", "closed"]
SORT = "formNum asc,updatedAt desc"
SEARCH = ""

# Input/Output paths
THIS_DIR = Path(__file__).resolve().parent
INPUT_JSON = THIS_DIR / "work-inspections-disclaimers.json"
BACKUP_JSON = THIS_DIR / "work-inspections-disclaimers.backup.json"


def _session() -> requests.Session:
	s = requests.Session()
	s.headers.update(
		{
			"Authorization": f"Bearer {ACC_BEARER_TOKEN}",
			"Accept": "application/json",
		}
	)
	return s


def _forms_url(offset: int) -> str:
	"""Build the ACC forms list URL with pagination and filters."""
	# Build query string manually to include repeated params
	base = (
		f"{ACC_BASE_URL}/projects/{ACC_PROJECT_ID}/forms"
		f"?templateId={ACC_FORM_TEMPLATE_ID}"
		f"&limit={PAGE_LIMIT}&offset={offset}"
		f"&search={SEARCH}"
		f"&sort={quote(SORT, safe='')}"
	)

	# statuses (repeat param)
	for st in STATUSES:
		base += f"&statuses={st}"

	# include flags
	for k, v in INCLUDE_PARAMS:
		base += f"&{k}={v}"

	return base


def _get_json(s: requests.Session, url: str) -> Dict[str, Any]:
	resp = s.get(url, timeout=60)
	resp.raise_for_status()
	return resp.json()


def fetch_all_acc_forms() -> List[Dict[str, Any]]:
	"""Fetch all ACC forms matching filters using offset pagination."""
	s = _session()
	all_rows: List[Dict[str, Any]] = []
	seen_ids: set = set()

	print("Fetching ACC forms with pagination...", flush=True)
	page = 0
	while page < MAX_PAGES:
		offset = page * PAGE_LIMIT
		url = _forms_url(offset)
		try:
			payload = _get_json(s, url)
		except requests.HTTPError as http_err:
			print(f"HTTP error on page {page + 1}: {http_err}", file=sys.stderr)
			if http_err.response is not None:
				try:
					print(http_err.response.text, file=sys.stderr)
				except Exception:
					pass
			raise

		data = (payload or {}).get("data") or []
		if not isinstance(data, list):
			raise ValueError("Unexpected response: 'data' is not a list")

		added = 0
		for row in data:
			if not isinstance(row, dict):
				continue
			rid = row.get("id")
			if not rid or rid in seen_ids:
				continue
			seen_ids.add(rid)
			all_rows.append(row)
			added += 1

		print(
			f"  Page {page + 1}: +{added} forms (total={len(all_rows)})",
			flush=True,
		)

		if len(data) < PAGE_LIMIT:
			break  # last page
		page += 1

	print(f"Fetched {len(all_rows)} total ACC forms.", flush=True)
	return all_rows


def _parse_dt(dt: Optional[str]) -> Tuple[int, str]:
	"""Parse ISO datetime; return sort key (timestamp int) and original string.

	If parsing fails or dt is falsy, returns (0, "").
	"""
	if not dt:
		return (0, "")
	try:
		# Example: 2025-08-20T16:24:57.056370+00:00
		return (int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp()), dt)
	except Exception:
		return (0, dt)


def build_identifier(name: Optional[str]) -> Optional[str]:
	if not name or not isinstance(name, str):
		return None
	# Per instructions: replace exact "Work Inspection Request-" with "" and trim
	ident = name.replace("Work Inspection Request-", "").strip()
	return ident or None


def map_forms_by_identifier(forms: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
	"""Map identifier -> chosen ACC form row.

	If multiple forms share the same identifier, keep the most recently updated.
	"""
	mapping: Dict[str, Dict[str, Any]] = {}
	collisions = 0
	for row in forms:
		ident = build_identifier(row.get("name"))
		if not ident:
			continue
		if ident not in mapping:
			mapping[ident] = row
		else:
			# pick the newer one by updatedAt
			ts_new, _ = _parse_dt(row.get("updatedAt"))
			ts_old, _ = _parse_dt(mapping[ident].get("updatedAt"))
			if ts_new >= ts_old:
				mapping[ident] = row
			collisions += 1

	if collisions:
		print(f"Note: {collisions} identifier collisions resolved by updatedAt.", flush=True)
	print(f"Identifiers available from ACC: {len(mapping)}", flush=True)
	return mapping


def load_work_inspections() -> List[Dict[str, Any]]:
	with open(INPUT_JSON, "r", encoding="utf-8") as f:
		data = json.load(f)
		if not isinstance(data, list):
			raise ValueError("Input JSON is not a list")
		return data


def backup_file() -> None:
	try:
		if INPUT_JSON.exists():
			BACKUP_JSON.write_text(INPUT_JSON.read_text(encoding="utf-8"), encoding="utf-8")
			print(f"Backup created: {BACKUP_JSON.name}", flush=True)
	except Exception as ex:
		print(f"Warning: backup failed: {ex}", file=sys.stderr)


def update_json_with_acc_ids(objs: List[Dict[str, Any]], id_map: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
	"""Update each object with accFormId; returns (matched, unmatched)."""
	matched = 0
	unmatched = 0
	total = len(objs)
	print(f"Updating {total} JSON records with accFormId...", flush=True)

	for i, obj in enumerate(objs, start=1):
		identifier = obj.get("identifier")
		# normalize to str for lookup consistency
		ident_key = str(identifier) if identifier is not None else None
		acc_form_id: Optional[str] = None

		if ident_key and ident_key in id_map:
			acc_form_id = id_map[ident_key].get("id")
			matched += 1
		else:
			unmatched += 1

		obj["accFormId"] = acc_form_id

		# progress every 25 or at end
		if i % 25 == 0 or i == total:
			pct = (i / total * 100) if total else 100
			print(f"  {i}/{total} ({pct:.1f}%)", flush=True)

	return matched, unmatched


def main() -> int:
	if not ACC_BEARER_TOKEN or ACC_BEARER_TOKEN.startswith("REPLACE_"):
		print(
			"Error: Please set ACC_BEARER_TOKEN to a valid ACC OAuth token inside this script.",
			file=sys.stderr,
		)
		return 2

	if not INPUT_JSON.exists():
		print(f"Error: Input file not found: {INPUT_JSON}", file=sys.stderr)
		return 2

	try:
		acc_forms = fetch_all_acc_forms()
		id_map = map_forms_by_identifier(acc_forms)
		objs = load_work_inspections()
		backup_file()
		matched, unmatched = update_json_with_acc_ids(objs, id_map)

		# Write back
		INPUT_JSON.write_text(json.dumps(objs, ensure_ascii=False, indent=2), encoding="utf-8")
		print(
			f"Done. Matched: {matched}, Unmatched: {unmatched}. Updated {INPUT_JSON.name}.",
			flush=True,
		)
		return 0
	except requests.HTTPError:
		return 1
	except Exception as ex:
		print(f"Unhandled error: {ex}", file=sys.stderr)
		return 1


if __name__ == "__main__":
	sys.exit(main())
