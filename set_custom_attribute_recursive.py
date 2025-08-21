"""
Sync a single Custom Attribute from a Test ACC project folder to a Live ACC project folder.

What it does
- Lists all documents (SEED_FILE) in the provided Test folder URN and Live folder URN
- Reads the custom attribute value (by id) from the Test project for each file
- Matches files by saved filename between Test and Live folders (case-insensitive)
- Updates the same custom attribute (using the Live attribute id) in the Live project

Safety
- Only operates on the two provided folder URNs
- Only updates a single custom attribute id you define for the Live project
- Skips files with missing/empty values

Configuration
- Fill in ACCESS_TOKEN, TEST_PROJECT_ID, LIVE_PROJECT_ID, TEST_FOLDER_URN, LIVE_FOLDER_URN
- Set TEST_CUSTOM_ATTRIBUTE_ID (source) and LIVE_CUSTOM_ATTRIBUTE_ID (target)

Usage (Windows bash)
- Ensure Python 3.9+ is installed
- Run:  python set_custom_attribute.py
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, quote
from urllib.request import Request, urlopen


# =============================
# REQUIRED CONFIGURATION VALUES
# =============================

# Hardcode the access token to use for BOTH projects (must include required scopes for DM v3)
ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlZiakZvUzhQU3lYODQyMV95dndvRUdRdFJEa19SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoidk9rMnduTzNDUUQ1bGdDdEJhMmVOSWtwTEFpZXo5Nkh0ZmxnNDd4dXh0b2J5YlZLTmhLUndzUXZPaktPbXdjWiIsInVzZXJpZCI6IlBMVUpYVTJFUERQRlJOUk0iLCJleHAiOjE3NTU3MjE1Njh9.e1-25GTmSmmvp8Nny0YI1gErQwmXbGAA0hRoWiccftZBrPuFYcK6tkA6bo9sQwwliJw4I20p-_3sYkk0qzbs0SR4u_QxLKMUvRIyJTBc_0rGEPzszMc0Kxl3rem6HTlKhvS7EsAL5JHd95zd_eZQDtMmZFQXnrivPZmMqYLI-zqd37VIjIcRt0bX_UQlkZ41fZsWAxg7o953SpoDqJTSY-egN5KXD2eumSjm_ewfQOD4jt8UokaCpcXbV8jCfqHJ-Syox5NUmyM8jN4_C8lgHgGbnYy5UdV0v4aii6vIkrMK0aPFV1DJ0TKZK4dpCSLEpIfQ7iE1F4LNrUkO1qMCIQ"

# Project IDs
TEST_PROJECT_ID = "b.fe3cafee-bfb9-41a7-a199-045a926ed74c"  # e.g. "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"
LIVE_PROJECT_ID = "b.6aeb3477-6b0e-4f54-b13d-26ae6de87c36"  # e.g. "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"

# Folder URNs (lineage URNs for the folders, starting with "urn:adsk.wipprod:fs.folder:co.")
TEST_FOLDER_URN = "urn:adsk.wipprod:fs.folder:co.q0e-8amoQ7eLTctXHKBh_A"  # e.g. "urn:adsk.wipprod:fs.folder:co.XXXX"
LIVE_FOLDER_URN = "urn:adsk.wipprod:fs.folder:co.7Z4V3H3KUZGYSVdiD4K-qQ"  # e.g. "urn:adsk.wipprod:fs.folder:co.YYYY"

# Custom Attribute IDs
# Source (Test project)
TEST_CUSTOM_ATTRIBUTE_ID = 6798099
# Target (Live project)
LIVE_CUSTOM_ATTRIBUTE_ID = 6819199


# ============
# CONSTANTS
# ============
BASE_URL = "https://developer.api.autodesk.com/"
DM_V3 = "dm/v3/"
DM_V2 = "data/v1/"

# Tuning
SEARCH_PAGE_LIMIT = 200  # number of docs per page for search
HTTP_TIMEOUT_SEC = 60
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1.8


# ============
# LOGGING
# ============
logger = logging.getLogger("set_custom_attribute")
logger.setLevel(logging.INFO)
_handler = logging.FileHandler("set_custom_attribute.log", mode="a", encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_handler)


# ============
# HTTP HELPERS
# ============
def _headers_json() -> Dict[str, str]:
	return {
		"Authorization": f"Bearer {ACCESS_TOKEN}",
		"Content-Type": "application/json",
		"Accept": "application/json",
	}


def _request_json(method: str, url: str, payload: Optional[Any] = None) -> dict:
	"""Make an HTTP request and return parsed JSON with retry on transient failures."""
	body = None
	if payload is not None:
		body = json.dumps(payload).encode("utf-8")

	for attempt in range(1, MAX_RETRIES + 1):
		try:
			req = Request(url=url, data=body, method=method.upper(), headers=_headers_json())
			with urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
				data = resp.read()
				if not data:
					return {}
				return json.loads(data.decode("utf-8"))
		except HTTPError as e:
			status = e.code
			err_body = e.read().decode("utf-8", errors="ignore")
			retryable = status in {408, 429, 500, 502, 503, 504}
			logger.warning("HTTP %s to %s failed (status %s). Attempt %s/%s. Body: %s", method, url, status, attempt, MAX_RETRIES, err_body)
			if not retryable or attempt == MAX_RETRIES:
				raise
			sleep_s = (RETRY_BACKOFF_BASE ** (attempt - 1)) + (0.1 * attempt)
			time.sleep(sleep_s)
		except URLError as e:
			logger.warning("Network error calling %s %s: %s (attempt %s/%s)", method, url, e, attempt, MAX_RETRIES)
			if attempt == MAX_RETRIES:
				raise
			sleep_s = (RETRY_BACKOFF_BASE ** (attempt - 1)) + (0.1 * attempt)
			time.sleep(sleep_s)

	# Should not reach here
	return {}


def _get_json(url: str) -> dict:
	"""HTTP GET with retries, returning parsed JSON."""
	for attempt in range(1, MAX_RETRIES + 1):
		try:
			req = Request(url=url, method="GET", headers={
				"Authorization": f"Bearer {ACCESS_TOKEN}",
				"Accept": "application/json",
			})
			with urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
				return json.loads(resp.read().decode("utf-8"))
		except HTTPError as e:
			status = e.code
			err_body = e.read().decode("utf-8", errors="ignore")
			retryable = status in {408, 429, 500, 502, 503, 504}
			logger.warning("HTTP GET %s failed (status %s). Attempt %s/%s. Body: %s", url, status, attempt, MAX_RETRIES, err_body)
			if not retryable or attempt == MAX_RETRIES:
				raise
			sleep_s = (RETRY_BACKOFF_BASE ** (attempt - 1)) + (0.1 * attempt)
			time.sleep(sleep_s)
		except URLError as e:
			logger.warning("Network error GET %s: %s (attempt %s/%s)", url, e, attempt, MAX_RETRIES)
			if attempt == MAX_RETRIES:
				raise
			sleep_s = (RETRY_BACKOFF_BASE ** (attempt - 1)) + (0.1 * attempt)
			time.sleep(sleep_s)

	return {}


# ============
# DM V3 HELPERS
# ============
def _dm_url(path: str) -> str:
	return urljoin(BASE_URL, urljoin(DM_V3, path.lstrip("/")))


def _normalize_project_id(project_id: str) -> str:
	"""DM v3 expects GUID without the 'b.' prefix. Strip if present."""
	return project_id[2:] if project_id.startswith("b.") else project_id


def _dm_v2_url(path: str) -> str:
	return urljoin(BASE_URL, urljoin(DM_V2, path.lstrip("/")))


def list_folder_contents_v2(project_id_with_b: str, folder_urn: str) -> Tuple[List[dict], List[dict]]:
	"""List contents (items and child folder dicts) of a folder via Data Management v2."""
	pid = project_id_with_b if project_id_with_b.startswith("b.") else f"b.{project_id_with_b}"
	enc_folder = quote(folder_urn, safe="")
	page = 0
	items: List[dict] = []
	folders: List[dict] = []
	while True:
		path = f"projects/{pid}/folders/{enc_folder}/contents?page[number]={page}&page[limit]={SEARCH_PAGE_LIMIT}"
		url = _dm_v2_url(path)
		try:
			data = _get_json(url)
		except HTTPError as e:
			body = e.read().decode("utf-8", errors="ignore")
			logger.error("Folder contents GET failed: %s - %s", e.code, body)
			raise

		data_list = data.get("data", [])
		for obj in data_list:
			t = obj.get("type")
			if t == "items":
				items.append(obj)
			elif t == "folders":
				folders.append(obj)

		links = data.get("links", {})
		if links.get("next", {}).get("href"):
			page += 1
		else:
			break

	return items, folders

def _display_name(obj: dict) -> str:
	attrs = (obj or {}).get("attributes") or {}
	return attrs.get("displayName") or attrs.get("name") or ""


def gather_all_items_recursive(project_id_with_b: str, root_folder_urn: str) -> List[dict]:
	"""DFS to accumulate all items under a folder tree, annotating each item with keyPath and nameLower."""
	items_accum: List[dict] = []
	# stack of tuples: (folder_urn, relative_path)
	stack: List[Tuple[str, str]] = [(root_folder_urn, "")]
	visited: set[str] = set()
	while stack:
		folder_urn, rel_path = stack.pop()
		if folder_urn in visited:
			continue
		visited.add(folder_urn)
		its, subs = list_folder_contents_v2(project_id_with_b, folder_urn)
		# add items with computed keyPath
		for it in its:
			name = _display_name(it)
			key_path = f"{rel_path}/{name}" if rel_path else name
			it_copy = dict(it)
			it_copy["keyPath"] = key_path
			it_copy["nameLower"] = (name or "").strip().lower()
			items_accum.append(it_copy)
		# traverse subfolders
		for f in subs:
			fname = _display_name(f)
			new_rel = f"{rel_path}/{fname}" if rel_path else fname
			fid = f.get("id")
			if fid:
				stack.append((fid, new_rel))
	return items_accum


def list_folder_items_v2(project_id_with_b: str, folder_urn: str) -> List[dict]:
	"""List items (files) in a folder via Data Management v2.

	Returns a list of item objects (type == 'items'), each including at least 'id' (lineage urn) and 'attributes.displayName'.
	"""
	# DM v2 expects the b.-prefixed project id
	pid = project_id_with_b if project_id_with_b.startswith("b.") else f"b.{project_id_with_b}"
	enc_folder = quote(folder_urn, safe="")
	page = 0
	items: List[dict] = []
	while True:
		path = f"projects/{pid}/folders/{enc_folder}/contents?page[number]={page}&page[limit]={SEARCH_PAGE_LIMIT}"
		url = _dm_v2_url(path)
		# GET request without payload
		req = Request(url=url, method="GET", headers={
			"Authorization": f"Bearer {ACCESS_TOKEN}",
			"Accept": "application/json",
		})
		try:
			with urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
				data = json.loads(resp.read().decode("utf-8"))
		except HTTPError as e:
			body = e.read().decode("utf-8", errors="ignore")
			logger.error("Folder contents GET failed: %s - %s", e.code, body)
			raise

		data_list = data.get("data", [])
		for obj in data_list:
			if obj.get("type") == "items":
				items.append(obj)

		links = data.get("links", {})
		if links.get("next", {}).get("href"):
			page += 1
		else:
			break

	return items


def batch_get_documents(project_id: str, urns: List[str]) -> List[dict]:
	"""Batch get document records (includes includedVersion.customAttributes)."""
	norm_pid = _normalize_project_id(project_id)
	path = f"projects/{norm_pid}/documents:batch-get"
	url = _dm_url(path)
	results: List[dict] = []

	# Chunk to avoid payload too large
	CHUNK = 200
	for i in range(0, len(urns), CHUNK):
		chunk = urns[i : i + CHUNK]
		payload = {"urns": chunk}
		resp = _request_json("POST", url, payload)
		res = resp.get("results", [])
		results.extend(res)
	return results


def batch_update_custom_attributes(project_id: str, updates: List[dict]) -> dict:
	"""Batch update custom attributes for documents.

	updates: List of {"documentUrn": <lineage urn>, "customAttributes": [{"id": <int>, "value": <str>}]}
	"""
	norm_pid = _normalize_project_id(project_id)
	path = f"projects/{norm_pid}/documents/custom-attributes:batch-update"
	url = _dm_url(path)

	# Chunk updates to be safe
	CHUNK = 100
	agg_result = {"success": 0, "errors": []}
	for i in range(0, len(updates), CHUNK):
		chunk = updates[i : i + CHUNK]
		try:
			resp = _request_json("POST", url, chunk)
		except HTTPError as e:
			body = getattr(e, "read", lambda: b"")()
			err = body.decode("utf-8", errors="ignore")
			logger.error("Batch update failed: HTTP %s - %s", e.code, err)
			agg_result["errors"].append({"httpStatus": e.code, "body": err})
			continue
		# DM v3 commonly returns 207-like per-item results; we just count successes if present
		if isinstance(resp, dict) and "errors" in resp:
			errs = resp.get("errors") or []
			if errs:
				agg_result["errors"].extend(errs)
		# Heuristically count updated items
		agg_result["success"] += len(chunk)

	return agg_result


# ============
# CORE LOGIC
# ============
def extract_attr_value_from_version(version: dict, attr_id: int) -> Optional[str]:
	attrs = (version or {}).get("customAttributes") or []
	for a in attrs:
		if a.get("id") == attr_id:
			val = a.get("value")
			if val is None:
				return None
			return str(val)
	return None


def build_name_to_doc_map(docs: List[dict]) -> Dict[str, dict]:
	"""Return a case-insensitive mapping of filename -> document record."""
	by_name: Dict[str, dict] = {}
	for d in docs:
		name = d.get("name") or (d.get("includedVersion") or {}).get("name")
		if not name:
			continue
		key = name.strip().lower()
		if key in by_name:
			# Duplicate name in same folder; log and keep the first
			logger.warning("Duplicate filename encountered in folder: %s (keeping first)", name)
			continue
		by_name[key] = d
	return by_name


def build_name_to_item_map(items: List[dict]) -> Dict[str, dict]:
	"""Return a case-insensitive mapping of displayName -> v2 item object."""
	by_name: Dict[str, dict] = {}
	for it in items:
		name = ((it or {}).get("attributes") or {}).get("displayName")
		if not name:
			continue
		key = name.strip().lower()
		if key in by_name:
			logger.warning("Duplicate item filename in folder: %s (keeping first)", name)
			continue
		by_name[key] = it
	return by_name


def sync_custom_attribute():
	# Basic config validation
	missing = []
	if not ACCESS_TOKEN:
		missing.append("ACCESS_TOKEN")
	if not TEST_PROJECT_ID:
		missing.append("TEST_PROJECT_ID")
	if not LIVE_PROJECT_ID:
		missing.append("LIVE_PROJECT_ID")
	if not TEST_FOLDER_URN:
		missing.append("TEST_FOLDER_URN")
	if not LIVE_FOLDER_URN:
		missing.append("LIVE_FOLDER_URN")
	if missing:
		msg = f"Missing required configuration values: {', '.join(missing)}"
		logger.error(msg)
		print(msg)
		sys.exit(2)

	print("Recursively listing items in Test folder…")
	logger.info("Recursively listing items in Test folder (DM v2): project=%s, folder=%s", TEST_PROJECT_ID, TEST_FOLDER_URN)
	test_items = gather_all_items_recursive(TEST_PROJECT_ID, TEST_FOLDER_URN)
	print(f"Found {len(test_items)} test items (recursive)")

	print("Recursively listing items in Live folder…")
	logger.info("Recursively listing items in Live folder (DM v2): project=%s, folder=%s", LIVE_PROJECT_ID, LIVE_FOLDER_URN)
	live_items = gather_all_items_recursive(LIVE_PROJECT_ID, LIVE_FOLDER_URN)
	print(f"Found {len(live_items)} live items (recursive)")

	# Batch-get to retrieve customAttributes for test docs (use lineage urns from v2 items)
	test_urns = [str(it.get("id")) for it in test_items if it.get("id")]
	print("Fetching custom attributes from Test…")
	test_details = batch_get_documents(TEST_PROJECT_ID, test_urns)
	# Map urn -> item for lookup
	test_by_urn: Dict[str, dict] = {str(it.get("id")): it for it in test_items if it.get("id")}
	# Build keyPath -> value map (include empty values)
	test_values_by_keypath: Dict[str, str] = {}
	for r in test_details:
		doc_urn = str(r.get("urn"))
		item = test_by_urn.get(doc_urn)
		if not item:
			continue
		key_path = item.get("keyPath") or ""
		ver = r.get("includedVersion") or {}
		val = extract_attr_value_from_version(ver, TEST_CUSTOM_ATTRIBUTE_ID)
		if val is None:
			val = ""
		if key_path and key_path not in test_values_by_keypath:
			test_values_by_keypath[key_path] = val

	# Prepare updates for live
	updates: List[dict] = []
	# Phase A: exact keyPath match
	live_by_keypath: Dict[str, dict] = {str(it.get("keyPath")): it for it in live_items if it.get("keyPath")}
	used_live_urns: set[str] = set()
	for key_path, value in test_values_by_keypath.items():
		live_item = live_by_keypath.get(key_path)
		if not live_item:
			continue
		live_urn = live_item.get("id")
		if not live_urn:
			continue
		live_urn = str(live_urn)
		updates.append({
			"documentUrn": live_urn,
			"customAttributes": [{"id": LIVE_CUSTOM_ATTRIBUTE_ID, "value": value}],
		})
		used_live_urns.add(live_urn)

	# Phase B: fallback by filename (handle duplicates by stable one-to-one pairing)
	def group_by_name(items: List[dict]) -> Dict[str, List[dict]]:
		g: Dict[str, List[dict]] = {}
		for it in items:
			nm = (it.get("nameLower") or "").strip().lower()
			if not nm:
				nm = ((_display_name(it) or "").strip().lower())
			g.setdefault(nm, []).append(it)
		for k in g:
			g[k].sort(key=lambda x: str(x.get("keyPath")))
		return g

	test_group = group_by_name(test_items)
	live_group = group_by_name([it for it in live_items if str(it.get("id")) not in used_live_urns])

	fallback_pairs = 0
	for name_key, test_list in test_group.items():
		if name_key not in live_group:
			continue
		# filter out already-used live items
		live_list = [it for it in live_group[name_key] if str(it.get("id")) not in used_live_urns]
		if not live_list:
			continue
		count = min(len(test_list), len(live_list))
		for i in range(count):
			t_it = test_list[i]
			l_it = live_list[i]
			t_key = t_it.get("keyPath") or ""
			value = test_values_by_keypath.get(t_key, "")
			l_urn = l_it.get("id")
			if not l_urn:
				continue
			l_urn = str(l_urn)
			updates.append({
				"documentUrn": l_urn,
				"customAttributes": [{"id": LIVE_CUSTOM_ATTRIBUTE_ID, "value": value}],
			})
			used_live_urns.add(l_urn)
			fallback_pairs += 1

	total_prepared = len(updates)
	print(f"Prepared {total_prepared} updates (path matches + {fallback_pairs} fallback name matches)")
	logger.info("Prepared updates: total=%s, fallbackNamePairs=%s", total_prepared, fallback_pairs)

	if not updates:
		print("No updates to apply.")
		return

	print("Applying updates to Live…")
	result = batch_update_custom_attributes(LIVE_PROJECT_ID, updates)
	success = result.get("success", 0)
	errors = result.get("errors", [])
	print(f"Applied updates: {success} items. Errors: {len(errors)}")
	if errors:
		logger.error("Errors from batch update: %s", errors)


def main():
	try:
		sync_custom_attribute()
	except Exception as e:
		logger.exception("Fatal error: %s", e)
		print(f"Error: {e}")
		sys.exit(1)


if __name__ == "__main__":
	main()
