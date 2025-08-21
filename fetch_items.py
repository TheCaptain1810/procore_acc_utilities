"""
Compare local folder files with Autodesk Construction Cloud (ACC) folder files.

Hardcode your ACC bearer token, project ID, and folder URN/ID below, and provide
the local folder path (hardcoded default or CLI arg). The script fetches file
names from both locations and prints which are missing on either side.

ACC API: Data Management v1
GET https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/contents

Notes:
- Token must include at least data:read scope.
- project_id is like "b.{...}" (BIM 360/ACC) and folder_id is an fs.folder URN (urn:adsk.wipprod:fs.folder:co...).
- Comparison is case-insensitive on filenames.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Iterable, List, Set, Tuple

import requests


# ========================
# Configuration (edit these)
# ========================
# Replace with a valid ACC bearer token (data:read scope)
AUTH_TOKEN: str = "REPLACE_WITH_BEARER_TOKEN"

# Replace with your ACC/BIM 360 project ID (starts with b.)
PROJECT_ID: str = "REPLACE_WITH_PROJECT_ID"

# Replace with the target folder URN/ID (urn:adsk.wipprod:fs.folder:co...)
FOLDER_ID: str = "REPLACE_WITH_FOLDER_URN"

# Default local folder to compare (can be overridden via CLI arg)
LOCAL_FOLDER: str = os.path.abspath("./attachments")


# ========================
# HTTP helpers
# ========================
BASE_DATA_URL = "https://developer.api.autodesk.com/data/v1"
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3


def _headers() -> dict:
	return {
		"Authorization": f"Bearer {AUTH_TOKEN}",
		"Accept": "application/json",
	}


def _retryable_request(method: str, url: str, **kwargs) -> requests.Response:
	timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
	resp: requests.Response | None = None
	for attempt in range(1, MAX_RETRIES + 1):
		resp = requests.request(method, url, timeout=timeout, **kwargs)
		if resp.status_code in (429, 500, 502, 503, 504):
			if attempt < MAX_RETRIES:
				retry_after = resp.headers.get("Retry-After")
				try:
					wait = float(retry_after) if retry_after else (2 ** attempt)
				except ValueError:
					wait = 2 ** attempt
				print(f"WARN: {resp.status_code} on {url}; retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
				time.sleep(wait)
				continue
		return resp
	if resp is None:
		raise RuntimeError("Request failed to execute")
	return resp  # pragma: no cover


# ========================
# Core logic
# ========================
def list_local_files(folder: str) -> Set[str]:
	"""Return a set of filenames (not recursive) in the given local folder.

	Comparison is case-insensitive and returns names as-lowered.
	"""
	if not os.path.isdir(folder):
		raise FileNotFoundError(f"Local folder does not exist: {folder}")
	names: Set[str] = set()
	for entry in os.listdir(folder):
		path = os.path.join(folder, entry)
		if os.path.isfile(path):
			names.add(entry.strip().lower())
	return names


def list_acc_files(project_id: str, folder_id: str) -> Set[str]:
	"""Return a set of filenames directly under an ACC folder (not recursive).

	Uses Data Management API: /projects/{project_id}/folders/{folder_id}/contents
	Follows pagination via links.next when present.
	Comparison is case-insensitive and returns names as-lowered.
	"""
	if not AUTH_TOKEN or AUTH_TOKEN == "REPLACE_WITH_BEARER_TOKEN":
		raise RuntimeError("Please set AUTH_TOKEN to a valid Bearer token with data:read")
	if not project_id or project_id.startswith("REPLACE_WITH"):
		raise RuntimeError("Please set PROJECT_ID to a valid project ID (starts with 'b.')")
	if not folder_id or folder_id.startswith("REPLACE_WITH"):
		raise RuntimeError("Please set FOLDER_ID to a valid folder URN (urn:adsk.wipprod:fs.folder:...)")

	names: Set[str] = set()
	url = f"{BASE_DATA_URL}/projects/{project_id}/folders/{folder_id}/contents"

	while url:
		resp = _retryable_request("GET", url, headers=_headers())
		if resp.status_code == 401:
			raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN and scopes.")
		if resp.status_code == 404:
			raise RuntimeError("Not found (404). Check PROJECT_ID and FOLDER_ID.")
		if resp.status_code != 200:
			raise RuntimeError(f"Failed to list folder contents: {resp.status_code} {resp.text}")

		payload = resp.json() or {}
		data = payload.get("data", [])
		if isinstance(data, list):
			for item in data:
				try:
					if not isinstance(item, dict):
						continue
					if item.get("type") != "items":
						# skip subfolders here; only compare files
						continue
					attrs = item.get("attributes", {}) or {}
					# displayName often contains the filename
					name = (attrs.get("displayName") or attrs.get("name") or "").strip()
					if name:
						names.add(name.lower())
				except Exception:
					# Be resilient to odd entries
					continue

		# pagination: JSON:API style links.next.href
		links = payload.get("links", {}) or {}
		next_link = links.get("next") or {}
		url = next_link.get("href") if isinstance(next_link, dict) else None

	return names


def diff_names(local_names: Set[str], acc_names: Set[str]) -> Tuple[Set[str], Set[str]]:
	"""Return (missing_in_acc, missing_locally)"""
	missing_in_acc = local_names - acc_names
	missing_locally = acc_names - local_names
	return missing_in_acc, missing_locally


def main(argv: List[str] | None = None) -> int:
	argv = list(argv or sys.argv[1:])
	local = argv[0] if argv else LOCAL_FOLDER
	local = os.path.abspath(local)

	print("=== ACC vs Local File Comparison ===")
	print(f"Local folder: {local}")
	print(f"Project ID:  {PROJECT_ID}")
	print(f"Folder ID:   {FOLDER_ID}")

	try:
		local_names = list_local_files(local)
	except Exception as e:
		print(f"ERROR: {e}")
		return 2

	try:
		acc_names = list_acc_files(PROJECT_ID, FOLDER_ID)
	except Exception as e:
		print(f"ERROR: {e}")
		return 3

	missing_in_acc, missing_locally = diff_names(local_names, acc_names)

	print("\n--- Summary ---")
	print(f"Local files: {len(local_names)}")
	print(f"ACC files:   {len(acc_names)}")
	print(f"Missing in ACC (present locally only): {len(missing_in_acc)}")
	print(f"Missing locally (present in ACC only): {len(missing_locally)}")

	if missing_in_acc:
		print("\nMissing in ACC:")
		for name in sorted(missing_in_acc):
			print(f"  - {name}")
	if missing_locally:
		print("\nMissing locally:")
		for name in sorted(missing_locally):
			print(f"  - {name}")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())

