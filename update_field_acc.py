"""
Bulk-update a custom Disclaimer field on ACC forms using values from our JSON file.

Flow
- Read WIR_Update/work-inspections-disclaimers.json
- For each record with accFormId and a non-empty disclaimerResponses[0].response,
  update the ACC form's target custom field with that response.
- Handle status transitions: temporarily set status=inProgress, update the value,
  then restore the original status (closed/inReview/inProgress).
- Track progress and support limiting how many forms to update via a constant.

Usage (Windows bash):
- Ensure Python 3.9+ and requests is available
- Set AUTH_TOKEN below or export ACC_AUTH_TOKEN env var
- Run:  python WIR_Update/update_field_acc.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests
import logging


# ========================
# Configuration
# ========================
# Prefer environment variable; fallback to constant.
AUTH_TOKEN = os.environ.get("ACC_AUTH_TOKEN", "eyJhbGciOiJSUzI1NiIsImtpZCI6IlZiakZvUzhQU3lYODQyMV95dndvRUdRdFJEa19SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoiczRuNTYyQjY1UzAzUnFQaGVwZWR0Q2dGN3hkWFBHdXFsUFRDUUNIbXEwbTRsM3pVRHhKUzJjUUZoODhEeWg0RiIsInVzZXJpZCI6IlBMVUpYVTJFUERQRlJOUk0iLCJleHAiOjE3NTU3NzQ0MjZ9.U6EDBwc0mXVvYE5aa_aYMLJicnD3sMpin8DUXZ4arPHgS3jSRJ2efn3-ME8AWtgGBZhGHCwXq-NhZXtf35HwI2FclBcT_WJpoc6e3HhAykzcN94pcDj_TOtQLkX-CwHbGYvM91nEiF8_L-ns13UWAMakM8dVas4EDmauARKSPeGrIh6LD_09x3byqcgTjPXxqk-Pvrmt9Q1Tatlv3n5aNSltMGdLnzjMU-K1YV2B011IuT7qIwgzSvSBxPS7iZwjYH6C0zXwdOr9tAt4i9VPCV2gQ1gnUAW_XrT8X_pYYAOltWtb-NS9_xN-8kS1oKxxRzivGJO3YuUpt4J3_b8YTA")

# Default project; can be overridden with --project argument if needed
PROJECT_ID = "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"

# Target custom field to update
FIELD_ID = "b28119b1-5c4b-45ef-a2ad-ff303e33273c"

# Toggle value to send alongside text (accepted values vary by template; 'Yes' and 'True' are common)
TOGGLE_VALUE = "Yes"

# Limit how many forms to update in this run. Set 0 for all.
MAX_FORMS_TO_UPDATE = 0

BASE_FORMS_URL = "https://developer.api.autodesk.com/construction/forms/development/v2"

# Request tuning
DEFAULT_TIMEOUT = 40  # seconds
MAX_RETRIES = 4

# Logging
LOG_TO_FILE = True
LOG_FILE = os.path.join(os.path.dirname(__file__), "update_field_acc.log")


def _setup_logging() -> logging.Logger:
	logger = logging.getLogger("update_field_acc")
	logger.setLevel(logging.INFO)
	logger.propagate = False

	# Clear existing handlers if re-run
	logger.handlers.clear()

	fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
	sh = logging.StreamHandler(sys.stdout)
	sh.setFormatter(fmt)
	logger.addHandler(sh)

	if LOG_TO_FILE:
		fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
		fh.setFormatter(fmt)
		logger.addHandler(fh)
	return logger


def _headers() -> Dict[str, str]:
	return {
		"Authorization": f"Bearer {AUTH_TOKEN}",
		"Accept": "application/json",
		"Content-Type": "application/json",
	}


def _retryable_request(method: str, url: str, **kwargs) -> requests.Response:
	"""Simple retry wrapper for transient failures (429/5xx)."""
	timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
	# Extract logger so it isn't forwarded to requests
	logger = kwargs.pop("logger", None)
	for attempt in range(1, MAX_RETRIES + 1):
		resp = requests.request(method, url, timeout=timeout, **kwargs)
		if resp.status_code in (429, 500, 502, 503, 504):
			if attempt < MAX_RETRIES:
				retry_after = resp.headers.get("Retry-After")
				try:
					wait = float(retry_after) if retry_after else (1.8 ** attempt)
				except ValueError:
					wait = 1.8 ** attempt
				msg = f"WARN: {resp.status_code} on {url}; retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})"
				(logger.info if logger else print)(msg)
				time.sleep(wait)
				continue
		return resp
	return resp  # type: ignore[UnboundLocalVariable]


def get_form_details(project_id: str, form_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/forms/{form_id}"
		f"?include=tableMetadata&include=nativeValues"
	)
	resp = _retryable_request("GET", url, headers=_headers(), logger=logger)
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	if resp.status_code != 200:
		raise RuntimeError(
			f"Failed to fetch form {form_id}: {resp.status_code} {resp.text}"
		)
	return resp.json()


def find_field_value(form_json: Dict[str, Any], field_id: str) -> Tuple[bool, Optional[str]]:
	"""Return (exists_in_form, textVal_or_None)."""
	native = form_json.get("nativeForm") or {}
	values = native.get("customValues") or []
	for entry in values:
		if entry.get("fieldId") == field_id:
			return True, entry.get("textVal")
	return False, None


def patch_form_status(project_id: str, template_id: str, form_id: str, status: str, logger: Optional[logging.Logger] = None) -> bool:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/form-templates/{template_id}/forms/{form_id}"
	)
	resp = _retryable_request("PATCH", url, headers=_headers(), json={"status": status}, logger=logger)
	if resp.status_code in (200, 204):
		return True
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	if logger:
		logger.error(f"Failed to set status={status} for {form_id}: {resp.status_code} {resp.text}")
	else:
		print(f"ERROR: Failed to set status={status} for {form_id}: {resp.status_code} {resp.text}")
	return False


def put_batch_update_value(project_id: str, form_id: str, field_id: str, text_value: str, toggle_val: str, logger: Optional[logging.Logger] = None) -> bool:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/forms/{form_id}/values:batch-update"
	)
	payload = {
		"customValues": [
			{"fieldId": field_id, "toggleVal": toggle_val, "textVal": text_value}
		]
	}
	resp = _retryable_request("PUT", url, headers=_headers(), json=payload, logger=logger)
	if resp.status_code in (200, 201, 202, 204):
		return True
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	if logger:
		logger.error(f"Failed to update field {field_id} on form {form_id}: {resp.status_code} {resp.text}")
	else:
		print(f"ERROR: Failed to update field {field_id} on form {form_id}: {resp.status_code} {resp.text}")
	return False


def _load_input_records() -> list[dict[str, Any]]:
	"""Load records from work-inspections-disclaimers.json in the same folder."""
	json_path = os.path.join(os.path.dirname(__file__), "work-inspections-disclaimers.json")
	with open(json_path, "r", encoding="utf-8") as f:
		data = json.load(f)
	if not isinstance(data, list):
		raise RuntimeError("Input JSON is not a list")
	return data


def _extract_disclaimer_text(rec: Dict[str, Any]) -> Optional[str]:
	"""Return disclaimerResponses[0].response if present and non-empty."""
	responses = rec.get("disclaimerResponses")
	if not isinstance(responses, list) or not responses:
		return None
	first = responses[0] or {}
	txt = (first.get("response") or "").strip()
	return txt or None


def _update_one_form(project_id: str, form_id: str, text_value: str, logger: Optional[logging.Logger] = None) -> bool:
	"""Reopen to inProgress if needed, update value, then restore original status."""
	try:
		form = get_form_details(project_id, form_id, logger=logger)
	except Exception as e:
		if logger:
			logger.error(f"get_form_details failed for {form_id}: {e}")
		else:
			print(f"ERROR: get_form_details failed for {form_id}: {e}")
		return False

	template_id = form.get("formTemplateId") or ""
	if not template_id:
		if logger:
			logger.error(f"formTemplateId missing for {form_id}")
		else:
			print(f"ERROR: formTemplateId missing for {form_id}")
		return False

	original_status = (form.get("status") or "").strip() or "inProgress"

	# Ensure inProgress
	if original_status != "inProgress":
		if not patch_form_status(project_id, template_id, form_id, "inProgress", logger=logger):
			return False

	# Update value
	if not put_batch_update_value(project_id, form_id, FIELD_ID, text_value, TOGGLE_VALUE, logger=logger):
		# Try to restore status if we changed it
		if original_status != "inProgress":
			patch_form_status(project_id, template_id, form_id, original_status, logger=logger)
		return False

	# Restore original status if needed
	if original_status != "inProgress":
		if not patch_form_status(project_id, template_id, form_id, original_status, logger=logger):
			return False

	return True


def main() -> int:
	logger = _setup_logging()
	if not AUTH_TOKEN or AUTH_TOKEN == "REPLACE_WITH_BEARER_TOKEN":
		logger.error("Please set AUTH_TOKEN or ACC_AUTH_TOKEN env var to a valid Bearer token.")
		return 2

	project_id = PROJECT_ID

	try:
		records = _load_input_records()
	except Exception as e:
		logger.error(f"{e}")
		return 2

	# Filter records that have accFormId and disclaimer text
	candidates: list[tuple[str, str, Any]] = []  # (form_id, text_value, identifier)
	for rec in records:
		form_id = (rec.get("accFormId") or "").strip()
		if not form_id:
			continue
		text_val = _extract_disclaimer_text(rec)
		if not text_val:
			continue
		candidates.append((form_id, text_val, rec.get("identifier")))

	if not candidates:
		logger.info("No records with accFormId and disclaimerResponses[0].response found.")
		return 0

	total = len(candidates) if MAX_FORMS_TO_UPDATE in (0, None) else min(len(candidates), int(MAX_FORMS_TO_UPDATE))
	logger.info(f"Updating {total} of {len(candidates)} eligible forms…")

	success = 0
	fail = 0

	for idx, (form_id, text_val, ident) in enumerate(candidates[:total], start=1):
		label = f"id={form_id} ident={ident}"
		logger.info(f"[{idx}/{total}] Updating {label}…")
		ok = _update_one_form(project_id, form_id, text_val, logger=logger)
		if ok:
			success += 1
			# print identifier and response after each update
			logger.info(f"Updated identifier={ident} response={text_val}")
		else:
			fail += 1
		if idx % 25 == 0 or idx == total:
			logger.info(f"Progress: {idx}/{total} | OK={success} FAIL={fail}")

	logger.info(f"Done. OK={success} FAIL={fail} (processed {total}).")
	return 0 if fail == 0 else 1


if __name__ == "__main__":
	sys.exit(main())

