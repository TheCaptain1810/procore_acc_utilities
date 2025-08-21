import json
import sys
from typing import Any, Dict, List, Optional

import requests


# =========================
# Hardcoded configuration
# =========================
# NOTE: Replace the token value below with a valid Procore API token.
# Per the request, values are hardcoded here (not read from params or env).
PROCORE_BEARER_TOKEN = "eyJhbGciOiJFUzUxMiJ9.eyJhbXIiOltdLCJhaWQiOiJXbXNlLTAzaU90WXFqVE05QkQ3SkZuWUU4QUdYZXRCeVI3VjhuN0ZBZlVnIiwiYW91aWQiOjEzMjQxNzUyLCJhb3V1aWQiOiJlMzg3MmU1OC03OTE5LTQzNTgtOTZlMi01Y2M2ZWU1ODAzZTQiLCJleHAiOjE3NTU3NzQ5NjYsInNpYXQiOjE3NTQ4OTAyNjEsInVpZCI6MTMyNDE3NTIsInV1aWQiOiJlMzg3MmU1OC03OTE5LTQzNTgtOTZlMi01Y2M2ZWU1ODAzZTQiLCJsYXN0X21mYV9jaGVjayI6MTc1NTc2MTQ2OH0.AMLbRzqH08g9VXxdEV-wf1l_IGOho62V74d35ByUsPZktNYkYTbshsfDSof6nIlbZjvm80YL8waxRILIvZZdTTr_AARBVywBkZSE79WaZ-gDXA0c2WR_sTUaA0dg_ozAJqmR-Pj9ZSH8gkN2pChIafPigSDogVMrPpwePeUgxnpXBPCE"

# Project and template context (as provided in the prompt)
PROJECT_ID = 2450798
LIST_TEMPLATE_ID = "9897027"

# Total pages and items to fetch (per user's note: 630 items across 7 pages)
TOTAL_PAGES = 7
TOTAL_ITEMS = 630

# Output file (saved locally in the repo root)
OUTPUT_JSON = "procore_forms_with_disclaimer.json"


HEADERS = {
    "Authorization": f"Bearer {PROCORE_BEARER_TOKEN}",
    "Accept": "application/json",
}


def _grouped_index_url(page: int) -> str:
    # Matches the exact style/params shown in the prompt
    return (
    f"https://app.procore.com/rest/v1.0/projects/{PROJECT_ID}/checklist/lists/grouped_index"
        f"?page={page}&filters%5Blist_template_id%5D=%5B%22{LIST_TEMPLATE_ID}%22%5D&sort=identifier&group_by=undefined&view=list"
    )


def _sections_url(list_id: int) -> str:
    return (
        f"https://app.procore.com/rest/v1.1/projects/{PROJECT_ID}/checklist/list_sections"
        f"?filters[list_id]={list_id}&sort=position&view=flat_v2&per_page=2000&page=1"
    )


def _items_url(list_id: int) -> str:
    return (
        f"https://app.procore.com/rest/v1.1/projects/{PROJECT_ID}/checklist/list_items"
        f"?filters[list_id]={list_id}&view=extended&sort=position_by_section&per_page=2000&page=1"
    )


_SESSION = requests.Session()
_SESSION.headers.update(HEADERS)


def _get_json(url: str) -> Any:
    resp = _SESSION.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_forms_id_and_identifier_all_pages() -> List[Dict[str, Any]]:
    """Fetch forms across all pages and return a list of dicts with id and identifier."""
    all_forms: List[Dict[str, Any]] = []
    seen_ids = set()

    print(f"Fetching forms across {TOTAL_PAGES} pages...", flush=True)
    for page in range(1, TOTAL_PAGES + 1):
        url = _grouped_index_url(page)
        payload = _get_json(url)
        page_count_before = len(all_forms)

        if not isinstance(payload, list):
            raise ValueError(f"Unexpected grouped_index response format on page {page} (expected list)")

        for group in payload:
            data = (group or {}).get("data", [])
            if not isinstance(data, list):
                continue
            for item in data:
                if not isinstance(item, dict):
                    continue
                list_id = item.get("id")
                identifier = item.get("identifier")
                if list_id is None or list_id in seen_ids:
                    continue
                seen_ids.add(list_id)
                all_forms.append({
                    "id": list_id,
                    "identifier": identifier,
                    # place-holders to be filled later
                    "turners_disclaimer_item_id": None,
                    "turners_disclaimer_text_value": None,
                })

        page_added = len(all_forms) - page_count_before
        total_so_far = len(all_forms)
        pct = (total_so_far / TOTAL_ITEMS * 100) if TOTAL_ITEMS else 0
        print(f"  Page {page}/{TOTAL_PAGES}: +{page_added} forms (total={total_so_far}/{TOTAL_ITEMS}, {pct:.1f}%)", flush=True)

    print(f"Discovered {len(all_forms)} unique forms.", flush=True)
    return all_forms


def fetch_turners_disclaimer_item_id(list_id: int) -> Optional[int]:
    """Return the first item_id from the section named "Turner's Disclaimer", or None if not found."""
    url = _sections_url(list_id)
    sections = _get_json(url)
    if not isinstance(sections, list):
        return None

    for section in sections:
        if not isinstance(section, dict):
            continue
        name = (section.get("name") or "").strip()
        if name == "Turner's Disclaimer":
            item_ids = section.get("item_ids") or []
            if isinstance(item_ids, list) and item_ids:
                return item_ids[0]
            return None
    return None


def fetch_item_text_value(list_id: int, item_id: int) -> Optional[str]:
    """Fetch list items and return item_response.payload.text_value for the given item_id (if present)."""
    url = _items_url(list_id)
    items = _get_json(url)
    if not isinstance(items, list):
        return None

    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("id") == item_id:
            item_response = item.get("item_response") or {}
            payload = item_response.get("payload") or {}
            # As requested, specifically extract payload -> text_value
            return payload.get("text_value")
    return None


def main() -> int:
    if not PROCORE_BEARER_TOKEN or PROCORE_BEARER_TOKEN.startswith("REPLACE_"):
        print("Error: Please set PROCORE_BEARER_TOKEN to a valid Procore Bearer token inside this script.", file=sys.stderr)
        return 2

    try:
        forms = fetch_forms_id_and_identifier_all_pages()

            # Enrich each form with Turner's Disclaimer first item_id and its text value
        for idx, f in enumerate(forms, start=1):
                list_id = f["id"]
                ident = f.get("identifier")
                print(f"Processing form {idx}/{len(forms)} (id={list_id}, identifier={ident})...", flush=True)
                disclaimer_item_id = fetch_turners_disclaimer_item_id(list_id)
                f["turners_disclaimer_item_id"] = disclaimer_item_id

                if disclaimer_item_id is not None:
                    text_value = fetch_item_text_value(list_id, disclaimer_item_id)
                    f["turners_disclaimer_text_value"] = text_value
                    print(f"  - Turner's Disclaimer item_id={disclaimer_item_id}, text_value={text_value!r}", flush=True)
                else:
                    f["turners_disclaimer_text_value"] = None
                    print("  - Turner's Disclaimer section not found or empty.", flush=True)

            # Save to JSON
            with open(OUTPUT_JSON, "w", encoding="utf-8") as fp:
                json.dump(forms, fp, ensure_ascii=False, indent=2)

        print(f"Saved {len(forms)} records to {OUTPUT_JSON}.", flush=True)
        return 0
    except requests.HTTPError as http_err:
        print(f"HTTP error: {http_err}", file=sys.stderr)
        # If server returned a message body, surface it for easier troubleshooting
        if hasattr(http_err, "response") and http_err.response is not None:
            try:
                print(http_err.response.text, file=sys.stderr)
            except Exception:
                pass
        return 1
    except Exception as ex:
        print(f"Unhandled error: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
