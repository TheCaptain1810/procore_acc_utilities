"""
Close Forms Script

Fetch up to 5 in-progress forms for a specific template, check if each form
has attachments via the attachments API, and close the form (status=closed)
only if it has two or more attachments. Uses a hardcoded bearer token.

Notes:
- This script does not persist any state; it prints actions/decisions.
- Replace AUTH_TOKEN with a valid Autodesk Construction Cloud token.
"""

from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional

import requests


# ========================
# Configuration
# ========================
AUTH_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlZiakZvUzhQU3lYODQyMV95dndvRUdRdFJEa19SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoieE82OW9GOGR6OVM1bWc0OG5RcXZMOGVpNHpEOW11c2YzWnVQYmcyYWxOdEhpQ2ViTWNpVzNCVjd2MDhFY2h5OCIsInVzZXJpZCI6IlBMVUpYVTJFUERQRlJOUk0iLCJleHAiOjE3NTU3MTgwNDJ9.gwlfxbmthdK0-n-hkrMzpsva3fmsWMD-JGJys51RhNximOjQAQVlWt7SNucab5uhcvh5AmRtPSMQBigkKizlGDj9rlKmoo-M5rlw5eK_RImNI8sq0rcJ1H1NDW9J8Bx02Co07t80UZAjE5uFtNqUtcGDjDNVrCrM-kzvKU8spDRN9WLLBWFPsfR97dQkMtDk-dH9KFdjvAUe3K26PdxPOyc-PMNGcDr3lffTZNpjzi_j8HBu6MEt-dSLoG6h6o7gxbJyfjtJw-FWALylvYMPd3yyED9Gxxl0cSZYwN2leHGEEveDbtU4JRuSeYsDITd-LmixXIxyBp3l5rbMYT7FXQ"  # <-- Put your Bearer token here
PROJECT_ID = "6aeb3477-6b0e-4f54-b13d-26ae6de87c36"
TEMPLATE_ID = "72b9c3e9-a5b8-59d1-9a3d-77daaffac1d0"

BASE_FORMS_URL = (
	"https://developer.api.autodesk.com/construction/forms/development/v2"
)
BASE_ATTACH_URL = (
	"https://developer.api.autodesk.com/construction/forms/attachment/v1"
)

# Request tuning
DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 3


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
		# Retry on rate limit or server errors
		if resp.status_code in (429, 500, 502, 503, 504):
			if attempt < MAX_RETRIES:
				# Use Retry-After if provided, else exponential backoff
				retry_after = resp.headers.get("Retry-After")
				try:
					wait = float(retry_after) if retry_after else (2 ** attempt)
				except ValueError:
					wait = 2 ** attempt
				print(f"WARN: {resp.status_code} on {url}; retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
				time.sleep(wait)
				continue
		return resp
	return resp  # type: ignore[UnboundLocalVariable]


def fetch_in_progress_forms(limit: int = 5, offset: int = 0) -> List[Dict[str, Any]]:
	"""Fetch in-progress forms for the given template, capped by limit.

	Returns a list of form dicts.
	"""
	url = (
		f"{BASE_FORMS_URL}/projects/{PROJECT_ID}/forms"
		f"?assigneeIncludeMembers=true"
		f"&include=inactiveFormTemplates&include=layoutInfo&include=sublocations"
		f"&limit={limit}&offset={offset}&search="
		f"&sort=formNum%20asc%2CupdatedAt%20desc"
		f"&statuses=inProgress&templateId={TEMPLATE_ID}"
	)

	resp = _retryable_request("GET", url, headers=_headers())
	if resp.status_code != 200:
		raise RuntimeError(f"Failed to fetch forms: {resp.status_code} {resp.text}")

	data = resp.json()
	# ACC APIs commonly return one of these structures; support all defensively
	if isinstance(data, list):
		return data
	for key in ("results", "items", "data", "forms"):
		if isinstance(data, dict) and key in data and isinstance(data[key], list):
			return data[key]
	# Fallback: wrap dict as single item if it looks like a form
	if isinstance(data, dict) and data.get("id"):
		return [data]
	return []


def count_form_attachments(form_id: str) -> int:
	"""Return the number of attachments for a form by hitting the attachments API."""
	url = (
		f"{BASE_ATTACH_URL}/projects/{PROJECT_ID}/attachments/{form_id}/items?customResponse=true"
	)
	resp = _retryable_request("GET", url, headers=_headers())
	if resp.status_code == 404:
		# No attachments endpoint for this form or not found
		print(f"INFO: Attachments not found for form {form_id} (404)")
		return 0
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	if resp.status_code != 200:
		raise RuntimeError(
			f"Failed to fetch attachments for {form_id}: {resp.status_code} {resp.text}"
		)

	payload = resp.json()
	if isinstance(payload, list):
		return len(payload)
	for key in ("results", "items", "data", "attachments"):
		if isinstance(payload, dict) and key in payload and isinstance(payload[key], list):
			return len(payload[key])
	# Could be empty object
	return 0


def close_form(form_id: str) -> bool:
	"""PATCH a form to closed status. Returns True if closed or already closed."""
	url = (
		f"{BASE_FORMS_URL}/projects/{PROJECT_ID}/form-templates/{TEMPLATE_ID}/forms/{form_id}"
	)
	resp = _retryable_request("PATCH", url, headers=_headers(), json={"status": "closed"})

	if resp.status_code in (200, 204):
		return True
	if resp.status_code == 409:
		# Conflict, maybe already closed or state transition disallowed
		print(f"WARN: Conflict closing form {form_id}: {resp.text}")
		return False
	if resp.status_code == 401:
		raise RuntimeError("Unauthorized (401). Check AUTH_TOKEN.")
	print(f"ERROR: Failed to close form {form_id}: {resp.status_code} {resp.text}")
	return False


def main() -> int:
	if not AUTH_TOKEN or AUTH_TOKEN == "REPLACE_WITH_BEARER_TOKEN":
		print("ERROR: Please set AUTH_TOKEN to a valid Bearer token.")
		return 2

	try:
		forms = fetch_in_progress_forms(limit=31, offset=0)
	except Exception as e:
		print(f"ERROR: {e}")
		return 1

	if not forms:
		print("No in-progress forms found for the given template.")
		return 0

	processed = 0
	closed = 0
	skipped = 0

	for form in forms:
		form_id: Optional[str] = None
		# Common keys: id, formId
		for key in ("id", "formId"):
			if isinstance(form, dict) and key in form and isinstance(form[key], str):
				form_id = form[key]
				break
		if not form_id:
			print(f"WARN: Skipping item without id: {form}")
			skipped += 1
			continue

		print(f"Processing form: {form_id}")
		try:
			attach_count = count_form_attachments(form_id)
		except Exception as e:
			print(f"ERROR: Could not fetch attachments for {form_id}: {e}")
			skipped += 1
			continue

		if attach_count >= 1:
			print(f"Form {form_id} has {attach_count} attachment(s). Closing…")
			try:
				if close_form(form_id):
					print(f"Closed form {form_id}")
					closed += 1
				else:
					print(f"Did not close form {form_id}")
					skipped += 1
			except Exception as e:
				print(f"ERROR: Failed to close form {form_id}: {e}")
				skipped += 1
		else:
			print(f"Form {form_id} has {attach_count} attachment(s). Skipping.")
			skipped += 1

		processed += 1

	print(
		f"Done. Processed: {processed}, Closed: {closed}, Skipped: {skipped}."
	)
	return 0


if __name__ == "__main__":
	sys.exit(main())
