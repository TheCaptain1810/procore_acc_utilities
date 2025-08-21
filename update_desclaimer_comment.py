"""
Update Disclaimer (custom field) on an ACC Form and manage status transitions.

What it does
- Fetches a form's details (includes native custom values)
- Checks for target fieldId existence and whether it already has a text value
- If text is missing/empty, reopens the form (status=inProgress), updates the field
  via values:batch-update, then closes the form (status=closed)

Usage (Windows bash):
- Ensure Python 3.9+ and requests is available
- Set AUTH_TOKEN below or export ACC_AUTH_TOKEN environment variable
- Edit HARD_CODED_FORM_ID and HARD_CODED_TEXT_VALUE below
- Run:  python update_desclaimer_comment.py

Notes
- Project and Template IDs default to the ones in the prompt; templateId is
  fetched from the form details to avoid hardcoding.
- Only updates when the field has no existing non-empty text value.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests


# ========================
# Configuration
# ========================
# Prefer environment variable; fallback to constant.
AUTH_TOKEN = os.environ.get("ACC_AUTH_TOKEN", "eyJhbGciOiJSUzI1NiIsImtpZCI6IlZiakZvUzhQU3lYODQyMV95dndvRUdRdFJEa19SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoiZVFOT3BSWGQ4WnRxeDRZOHlvZkFjSnJ1S2RRSzdHWk5wSks1SnR2cWoxVWxoZmlSS3ZpN1N0Wk5vWG1rVzRrVSIsInVzZXJpZCI6IlBMVUpYVTJFUERQRlJOUk0iLCJleHAiOjE3NTU3NjMyNTh9.VPyXHVydcb0ZXBs9e5H3m2u8odA9Cao3Rg3rnVTr2sRNifiEcbp4zTi2ihy9iqxDSQeoYRwnJbcq0jEP10Mg9WjS60vAI9Eg5Yq9pdrI1wFjpHwYX7N1cgqmt8MyqMwkf9aWwgUXdBLtUcrBZ-uFuD7KIV0kP-DzRXdrNCFUzNqdlj-9mm-nwyKihYIgxrsk-9sMWmaz4Nm1O4dn66og1b6mlQ_fsjVSiyAlN6KXoksV0C5El7trGvBq7Q5f3s3zQwjGUbO2DYfqyV0JiSz7Cm6vL5EBt_5azHyIvGf0tkVg3OebMiYLqnECmL6ff7ZKkCLo11660oe26PGn_ILK8Q")

# Default project; can be overridden with --project argument if needed
PROJECT_ID = "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"

# Target custom field to update
FIELD_ID = "b28119b1-5c4b-45ef-a2ad-ff303e33273c"

# Toggle value to send alongside text (accepted values vary by template; 'Yes' and 'True' are commonly accepted)
TOGGLE_VALUE = "Yes"

# Hardcoded inputs (edit these values)
HARD_CODED_FORM_ID = "cde0435a-dfa3-4e1e-a524-67ca9c396672"  # e.g. "cbc1611b-38a5-4df0-b706-beafd7194495"
HARD_CODED_TEXT_VALUE = "yes"  # e.g. "yes"

BASE_FORMS_URL = "https://developer.api.autodesk.com/construction/forms/development/v2"

# Request tuning
DEFAULT_TIMEOUT = 40  # seconds
MAX_RETRIES = 4


def _headers() -> Dict[str, str]:
	return {
		"Authorization": f"Bearer {AUTH_TOKEN}",
		"Accept": "application/json",
		"Content-Type": "application/json",
	}


def _retryable_request(method: str, url: str, **kwargs) -> requests.Response:
	"""Simple retry wrapper for transient failures (429/5xx)."""
	timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
	for attempt in range(1, MAX_RETRIES + 1):
		resp = requests.request(method, url, timeout=timeout, **kwargs)
		if resp.status_code in (429, 500, 502, 503, 504):
			if attempt < MAX_RETRIES:
				retry_after = resp.headers.get("Retry-After")
				try:
					wait = float(retry_after) if retry_after else (1.8 ** attempt)
				except ValueError:
					wait = 1.8 ** attempt
				print(
					f"WARN: {resp.status_code} on {url}; retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})"
				)
				time.sleep(wait)
				continue
		return resp
	return resp  # type: ignore[UnboundLocalVariable]


def get_form_details(project_id: str, form_id: str) -> Dict[str, Any]:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/forms/{form_id}"
		f"?include=tableMetadata&include=nativeValues"
	)
	resp = _retryable_request("GET", url, headers=_headers())
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


def patch_form_status(project_id: str, template_id: str, form_id: str, status: str) -> bool:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/form-templates/{template_id}/forms/{form_id}"
	)
	resp = _retryable_request("PATCH", url, headers=_headers(), json={"status": status})
	if resp.status_code in (200, 204):
		return True
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	print(f"ERROR: Failed to set status={status} for {form_id}: {resp.status_code} {resp.text}")
	return False


def put_batch_update_value(project_id: str, form_id: str, field_id: str, text_value: str, toggle_val: str) -> bool:
	url = (
		f"{BASE_FORMS_URL}/projects/{project_id}/forms/{form_id}/values:batch-update"
	)
	payload = {
		"customValues": [
			{"fieldId": field_id, "toggleVal": toggle_val, "textVal": text_value}
		]
	}
	resp = _retryable_request("PUT", url, headers=_headers(), json=payload)
	if resp.status_code in (200, 201, 202, 204):
		return True
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	print(
		f"ERROR: Failed to update field {field_id} on form {form_id}: {resp.status_code} {resp.text}"
	)
	return False


def main() -> int:
	if not AUTH_TOKEN or AUTH_TOKEN == "REPLACE_WITH_BEARER_TOKEN":
		print("ERROR: Please set AUTH_TOKEN or ACC_AUTH_TOKEN env var to a valid Bearer token.")
		return 2

	# Use hardcoded values only
	form_id = HARD_CODED_FORM_ID.strip()
	text_value = HARD_CODED_TEXT_VALUE
	if not form_id or not text_value:
		print(
			"ERROR: Please set HARD_CODED_FORM_ID and HARD_CODED_TEXT_VALUE at the top of the script."
		)
		return 2

	project_id = PROJECT_ID
	toggle_val = TOGGLE_VALUE

	try:
		form = get_form_details(project_id, form_id)
	except Exception as e:
		print(f"ERROR: {e}")
		return 1

	template_id = form.get("formTemplateId") or ""
	if not template_id:
		print("ERROR: formTemplateId not found in form details; cannot PATCH status.")
		return 1

	status = (form.get("status") or "").strip()
	exists, current_text = find_field_value(form, FIELD_ID)
	has_value = bool((current_text or "").strip())

	if not exists:
		print(
			f"INFO: Field {FIELD_ID} not present in nativeForm; proceeding to set it via batch-update."
		)

	if has_value:
		print(
			f"No update needed. Field already has a value: '{current_text}'. Status remains '{status}'."
		)
		return 0

	# Need to update: ensure inProgress
	if status != "inProgress":
		print(f"Reopening form {form_id} (current status '{status}')…")
		if not patch_form_status(project_id, template_id, form_id, "inProgress"):
			return 1

	# Update value
	print(f"Updating field {FIELD_ID} with text='{text_value}' and toggle='{toggle_val}'…")
	if not put_batch_update_value(project_id, form_id, FIELD_ID, text_value, toggle_val):
		return 1

	# Close again
	print(f"Closing form {form_id}…")
	if not patch_form_status(project_id, template_id, form_id, "closed"):
		return 1

	print("Done.")
	return 0


if __name__ == "__main__":
	sys.exit(main())

