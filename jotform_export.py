import json
import os
import re
import sys
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  –  loaded from environment variables (set as GitHub Secrets)
# ══════════════════════════════════════════════════════════════════════════════

def _require_env(name):
    val = os.environ.get(name)
    if not val:
        sys.exit(f"Missing required environment variable: {name}")
    return val

API_KEY               = _require_env("JOTFORM_API_KEY")
FORM_ID               = _require_env("JOTFORM_FORM_ID")
FILTER_VALUE          = _require_env("JOTFORM_FILTER_VALUE")
SPREADSHEET_ID        = _require_env("GOOGLE_SPREADSHEET_ID")
SHEET_NAME            = _require_env("GOOGLE_SHEET_NAME")
SERVICE_ACCOUNT_FILE  = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")

SCOPES               = ["https://www.googleapis.com/auth/spreadsheets"]
FILTER_QUESTION_ID   = int(os.environ.get("JOTFORM_FILTER_QUESTION_ID", "17"))
CUTOFF_DATE          = os.environ.get("CUTOFF_DATE", "2025-04-01")

# Normalize CUTOFF_DATE — accept either a "YYYY-MM-DD" string or a datetime object
if isinstance(CUTOFF_DATE, str):
    CUTOFF_DATE = datetime.strptime(CUTOFF_DATE.strip(), "%Y-%m-%d")

# Speed knobs
PAGE_SIZE        = int(os.environ.get("PAGE_SIZE", "1000"))   # JotForm's max page size
MAX_PARALLEL     = int(os.environ.get("MAX_PARALLEL", "8"))   # concurrent page fetches per round
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT", "60"))

TARGET_COLUMNS = [
    "Category",
    "Submission Date",
    "Request ID",
    "Items",
    "Full Name",
    "Contact Number",
    "Alt Contact Number",
    "PW Mail ID",
    "PWID / CAND_ID",
    "Address",
    "Approval Status",
    "Last Update Date",
]

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def normalize(text):
    """Lowercase + collapse whitespace for fuzzy matching."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip().casefold()


def clean_phone(raw):
    """Extract phone from JSON like {"full": "70072 82257"} or plain string."""
    if not raw:
        return ""
    try:
        d = json.loads(raw)
        if isinstance(d, dict):
            return d.get("full", "").strip()
    except (json.JSONDecodeError, TypeError):
        pass
    return str(raw).strip()


def format_date(dt_str):
    """'2025-04-03 05:30:37'  →  '4/3/2025'"""
    if not dt_str:
        return ""
    try:
        dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return dt_str


def parse_items(raw_answer):
    """
    Parse a JSON array of item dicts into a list of readable strings.
    e.g. [{"Items":"Laptop","Specification":"8Gb RAM","Quantity":"1"}]
         → ["Items: Laptop, Specification: 8Gb RAM, Quantity: 1"]
    Each array element becomes one output row (fan-out).
    """
    if not raw_answer or raw_answer.strip() in ("", "[]"):
        return []
    try:
        parsed = json.loads(raw_answer)
        if isinstance(parsed, list):
            rows = []
            for entry in parsed:
                if isinstance(entry, dict):
                    parts = [f"{k}: {v}" for k, v in entry.items()
                             if v not in (None, "", "null")]
                    if parts:
                        rows.append(", ".join(parts))
            return rows
        if isinstance(parsed, dict):
            parts = [f"{k}: {v}" for k, v in parsed.items()
                     if v not in (None, "", "null")]
            return [", ".join(parts)] if parts else []
    except (json.JSONDecodeError, TypeError):
        pass
    text = raw_answer.strip()
    return [text] if text else []


def detect_category_and_items(flat_row):
    """
    Find which inventory category column has data and parse its items.
    Returns list of (category_str, items_str) tuples — one per item line.
    """
    ITEM_COLUMNS = [
        ("IT Asset",    "IT Asset"),
        ("Electronics", "Electronics"),
        ("Software",    "Software"),
        ("Apparel",     "Apparel"),
        ("Furniture",   "Furniture"),
        ("Stationary",  "Stationary"),
        ("Others",      "Others"),
        ("Setup",       "Setup"),
    ]
    for col, category in ITEM_COLUMNS:
        raw = flat_row.get(col, "").strip()
        if raw and raw != "[]":
            items = parse_items(raw)
            if items:
                return [(category, item) for item in items]
    return [("Others", flat_row.get("Type of Request", "").strip())]


def get_address(flat_row):
    """Return the first non-empty address field found."""
    for col in ("Personal Address", "Delivery Address", "Full Address",
                "Delivery_Address", "Full form"):
        val = str(flat_row.get(col) or "").strip()
        if val:
            return val
    return ""


def map_workflow_status(raw_status):
    """Map JotForm workflow status codes to human-readable labels."""
    STATUS_MAP = {
        "ACTIVE":   "Pending",
        "APPROVED": "Approve",
        "DENIED":   "Denied wrt SOP",
        "DELETED":  "Deleted",
        "ARCHIVED": "Archived",
    }
    if not raw_status:
        return ""
    return STATUS_MAP.get(str(raw_status).strip().upper(), raw_status.strip())


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 – Fetch submissions from JotForm  (parallel, connection-pooled)
# ══════════════════════════════════════════════════════════════════════════════

_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_PARALLEL, pool_maxsize=MAX_PARALLEL)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def fetch_page(base_url, offset, filter_json, max_retries=5):
    """GET a single page with exponential backoff on timeouts / 5xx errors."""
    params = {
        "apiKey":            API_KEY,
        "limit":             PAGE_SIZE,
        "offset":            offset,
        "orderby":           "created_at",
        "direction":         "ASC",
        "addWorkflowStatus": 1,
        "filter":            filter_json,
    }
    last_status = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = _session.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return offset, resp.json().get("content", [])
        except (requests.ReadTimeout, requests.ConnectionError) as e:
            wait = 2 ** attempt
            print(f"  [offset {offset}] attempt {attempt} failed ({e}); retrying in {wait}s...")
            time.sleep(wait)
        except requests.HTTPError as e:
            last_status = e.response.status_code if e.response is not None else None
            if last_status and last_status >= 500:
                wait = 2 ** attempt
                print(f"  [offset {offset}] server error {last_status}; retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Failed to fetch offset {offset} after {max_retries} retries.")


def fetch_submissions():
    """
    Two-stage filtering strategy:

    STAGE 1 — Server-side date filter (reliable):
        filter={"created_at:gt": "YYYY-MM-DD+00:00:00"}
        JotForm honours this for plain metadata fields like created_at.
        This cuts 500k+ records down to only those after the cutoff date.

    STAGE 2 — Client-side text filter (necessary):
        JotForm cannot filter answer fields that contain JSON arrays/objects
        (dropdowns, checkboxes, etc.) server-side — it silently ignores those
        filters. So text matching is done in Python on the date-filtered subset.

    Speed: pages are fetched MAX_PARALLEL at a time over a pooled HTTP session,
    at JotForm's max page size, instead of one-by-one sequential requests.
    Result ordering doesn't matter since write_to_sheets sorts by date anyway,
    so pages can safely be fetched out of order and reassembled.
    """
    base_url    = f"https://pw.jotform.com/API/form/{FORM_ID}/submissions"
    cutoff_str  = CUTOFF_DATE.strftime("%Y-%m-%d")
    filter_json = json.dumps({"created_at:gt": f"{cutoff_str}+00:00:00"})
    norm_filter = normalize(FILTER_VALUE)

    all_submissions = []
    next_offset = 0
    done = False

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        while not done:
            round_offsets = [next_offset + i * PAGE_SIZE for i in range(MAX_PARALLEL)]
            futures = {pool.submit(fetch_page, base_url, off, filter_json): off
                       for off in round_offsets}

            results = {}
            for fut in as_completed(futures):
                off, batch = fut.result()
                results[off] = batch

            # Process in offset order so we stop at the true end of data.
            for off in round_offsets:
                batch = results[off]
                if not batch:
                    done = True
                    break

                for sub in batch:
                    created_at = sub.get("created_at", "")
                    try:
                        dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        continue
                    if dt < CUTOFF_DATE:
                        continue

                    answer = sub.get("answers", {}).get(str(FILTER_QUESTION_ID), {}).get("answer", "")
                    answer_str = json.dumps(answer) if isinstance(answer, (list, dict)) else str(answer)
                    if norm_filter not in normalize(answer_str):
                        continue

                    all_submissions.append(sub)

                if len(batch) < PAGE_SIZE:
                    done = True
                    break

            next_offset = round_offsets[-1] + PAGE_SIZE
            print(f"  Fetched through offset {next_offset} | running total matched: {len(all_submissions)}")

    return all_submissions


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 – Flatten a raw JotForm submission into a key→value dict
# ══════════════════════════════════════════════════════════════════════════════

def flatten_submission(sub):
    """Convert one raw JotForm submission dict into a flat label→value dict."""
    answers = sub.get("answers", {})

    row = {
        "Created At":      sub.get("created_at") or "",
        "Updated At":      sub.get("updated_at") or "",
        "Workflow Status": sub.get("workflowStatus") or "",
        "Status":          sub.get("status") or "",
    }

    for qid, qdata in sorted(
        answers.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999
    ):
        label  = qdata.get("text") or qdata.get("name") or f"Q{qid}"
        answer = qdata.get("answer", "")
        if isinstance(answer, (dict, list)):
            answer = json.dumps(answer)
        row[label] = str(answer) if answer is not None else ""

    return row


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 – Transform flat dict into 1-N target rows (one per item)
# ══════════════════════════════════════════════════════════════════════════════

def transform_row(flat_row):
    """
    Convert one flat submission dict → list of output dicts (one per item).
    A submission with 2 items in its JSON array produces 2 output rows.
    """
    cat_items = detect_category_and_items(flat_row)

    def safe(key):
        return str(flat_row.get(key) or "").strip()

    submission_date = format_date(safe("Created At"))
    last_update     = safe("Updated At") or safe("Created At")
    request_id      = safe("Unique ID")
    full_name       = safe("Full Name")
    contact         = clean_phone(safe("Phone Number"))
    alt_contact     = clean_phone(safe("Alternate Phone Number"))
    pw_email        = safe("PW Email")
    pw_id           = safe("Employee ID")
    address         = get_address(flat_row)

    raw_wf  = safe("Workflow Status")
    raw_st  = safe("Status")
    approval_status = map_workflow_status(raw_wf) if raw_wf else map_workflow_status(raw_st)

    return [
        {
            "Category":           category,
            "Submission Date":    submission_date,
            "Request ID":         request_id,
            "Items":              items_str,
            "Full Name":          full_name,
            "Contact Number":     contact,
            "Alt Contact Number": alt_contact,
            "PW Mail ID":         pw_email,
            "PWID / CAND_ID":     pw_id,
            "Address":            address,
            "Approval Status":    approval_status,
            "Last Update Date":   last_update,
        }
        for category, items_str in cat_items
    ]


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 – Write transformed data to Google Sheets
#           (single clear + single combined header/data write)
# ══════════════════════════════════════════════════════════════════════════════

def get_or_create_sheet(service, meta, sheet_name):
    """Return the sheetId of sheet_name, creating the tab if it doesn't exist.
    Takes an already-fetched spreadsheet `meta` to avoid a redundant API call."""
    existing = {
        s["properties"]["title"]: s["properties"]["sheetId"]
        for s in meta.get("sheets", [])
    }

    if sheet_name not in existing:
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        resp = service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body
        ).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
        print(f"  Created new tab: '{sheet_name}'  (id={sheet_id})")
    else:
        sheet_id = existing[sheet_name]
        print(f"  Using existing tab: '{sheet_name}'  (id={sheet_id})")

    return sheet_id


def write_to_sheets(submissions):
    """Flatten → transform → sort → clear everything → write header+data in one shot."""
    if not submissions:
        print("No matching submissions found. Nothing written to Sheets.")
        return

    flat_rows = [flatten_submission(s) for s in submissions]

    transformed = []
    for flat in flat_rows:
        transformed.extend(transform_row(flat))

    if not transformed:
        print("Transformation produced no rows. Check field name mappings.")
        return

    # Sort oldest → newest
    transformed.sort(key=lambda r: r.get("Last Update Date") or "")

    values = [[row.get(h, "") for h in TARGET_COLUMNS] for row in transformed]

    creds   = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds)

    # One metadata fetch, reused for tab lookup/creation.
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = get_or_create_sheet(service, meta, SHEET_NAME)

    # ── Clear existing data rows (A2:L), leaving the header row untouched ──
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A2:L",
        body={},
    ).execute()
    print("  Cleared existing sheet contents.")

    # ── Write header + all data rows together in a single call ──────────────
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": [TARGET_COLUMNS] + values},
    ).execute()

    print(f"\n✅  Written {len(transformed)} rows → '{SHEET_NAME}' (header + data, single call)")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("JotForm IRF Extractor (fast)")
    print(f"Form     : {FORM_ID}")
    print(f"Filter   : Q{FILTER_QUESTION_ID} contains '{FILTER_VALUE}'")
    print(f"Since    : {CUTOFF_DATE.strftime('%Y-%m-%d')}")
    print(f"Sheet    : {SHEET_NAME}")
    print(f"Page size: {PAGE_SIZE}  |  Parallel workers: {MAX_PARALLEL}")
    print("=" * 60)

    t0 = time.time()

    print("\n[1/2] Fetching & filtering submissions...")
    submissions = fetch_submissions()
    print(f"\n  → {len(submissions)} submissions matched.\n")

    print("[2/2] Transforming & writing to Google Sheets...")
    write_to_sheets(submissions)