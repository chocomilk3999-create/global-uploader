import os
import time
import uuid
import json
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# --- CONFIG ---
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "Sheet1").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEASE_SECONDS = 300  # 5 min

# --- STORAGE (RAM) ---
JOBS_QUEUE = []
INFLIGHT = {}

def now_ts(): 
    return int(time.time())

def _get_service():
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        return None, "ERROR: ENV 'GOOGLE_APPLICATION_CREDENTIALS_JSON' is empty."

    try:
        info = json.loads(creds_json)
    except Exception as e:
        return None, f"ERROR: JSON Parse Failed: {str(e)}"

    try:
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return svc, f"OK: {info.get('client_email')}"
    except Exception as e:
        return None, f"ERROR: Google Auth/Build Failed: {str(e)}"

def to_a1(col0, row0):
    c = col0 + 1
    s = ""
    while c > 0:
        c, m = divmod(c - 1, 26)
        s = chr(65 + m) + s
    return f"{s}{row0 + 1}"

def sheet_get_all(svc):
    return svc.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_TAB}!A1:ZZ"
    ).execute()

def sheet_update_cell(svc, row0, col0, value):
    rng = f"{GSHEET_TAB}!{to_a1(col0, row0)}"
    svc.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": [[value]]},
    ).execute()

def sheet_update_by_header(svc, row0, header, updates: dict):
    """
    updates: {"market_listing_url": "...", "market_listing_id": "..."}
    header: row0=0에 있는 헤더 리스트
    """
    hmap = {str(h).strip().lower(): i for i, h in enumerate(header)}
    for k, v in (updates or {}).items():
        key = str(k).strip().lower()
        if key in hmap:
            sheet_update_cell(svc, row0, hmap[key], v)

def requeue_expired():
    expired = [k for k, v in INFLIGHT.items() if v["expires_at"] <= now_ts()]
    for lid in expired:
        rec = INFLIGHT.pop(lid)
        JOBS_QUEUE.append(rec["job"])
    return len(expired)

@app.route("/")
def home():
    return f"Empire Brain Online. Queue={len(JOBS_QUEUE)} Inflight={len(INFLIGHT)}"

@app.route("/debug/google")
def debug_google():
    svc, msg = _get_service()
    return jsonify({"ok": svc is not None, "message": msg, "sheet_id": GSHEET_ID, "tab": GSHEET_TAB})

@app.route("/jobs/push-from-sheet", methods=["GET", "POST"])
def push_from_sheet():
    svc, msg = _get_service()
    if not svc:
        return jsonify({"ok": False, "error": "Google Auth Failed", "debug_msg": msg}), 500

    limit = int(request.args.get("limit", 20))
    try:
        resp = sheet_get_all(svc)
    except Exception as e:
        return jsonify({"ok": False, "error": "Sheet Read Failed", "detail": str(e)}), 400

    rows = resp.get("values", [])
    if len(rows) < 2:
        return jsonify({"ok": True, "pushed": 0, "queue_len": len(JOBS_QUEUE), "msg": "Sheet Empty"}), 200

    header = rows[0]
    try:
        status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except:
        return jsonify({"ok": False, "error": "No 'status' column in header"}), 400

    pushed = 0
    for i in range(1, len(rows)):
        if pushed >= limit:
            break
        row = rows[i]
        curr = row[status_idx] if len(row) > status_idx else ""
        if str(curr).strip().upper() == "NEW":
            job = {
                "id": str(uuid.uuid4()),
                "sheet_row": i,                 # 0-based row index in fetched array
                "sheet_status_col": status_idx, # status col index
                "header": header,
                "data": row
            }
            JOBS_QUEUE.append(job)
            # status -> QUEUED
            try:
                sheet_update_cell(svc, i, status_idx, "QUEUED")
            except Exception as e:
                print("Update QUEUED failed:", e)
            pushed += 1

    return jsonify({"ok": True, "pushed": pushed, "queue_len": len(JOBS_QUEUE)})

@app.route("/jobs/lease", methods=["GET"])
def lease():
    requeue_expired()
    if not JOBS_QUEUE:
        return jsonify({"ok": True, "job": None})

    job = JOBS_QUEUE.pop(0)
    lid = str(uuid.uuid4())
    INFLIGHT[lid] = {"job": job, "expires_at": now_ts() + LEASE_SECONDS}

    svc, _ = _get_service()
    if svc:
        try:
            sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], "INFLIGHT")
        except Exception as e:
            print("Update INFLIGHT failed:", e)

    return jsonify({"ok": True, "job": job, "lease_id": lid, "lease_seconds": LEASE_SECONDS})

@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lid = data.get("lease_id")
    status = data.get("status", "DRAFTED")
    updates = data.get("updates", {})  # ✅ 추가

    if lid not in INFLIGHT:
        return jsonify({"ok": False, "error": "No Lease"}), 400

    rec = INFLIGHT.pop(lid)
    job = rec["job"]
    header = job.get("header", [])

    svc, _ = _get_service()
    if svc:
        try:
            # status write
            sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], status)
            # ✅ additional fields write
            sheet_update_by_header(svc, job["sheet_row"], header, updates)
        except Exception as e:
            return jsonify({"ok": False, "error": "Sheet Update Failed", "detail": str(e)}), 500

    return jsonify({"ok": True})

@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lid = data.get("lease_id")

    if lid not in INFLIGHT:
        return jsonify({"ok": False, "error": "No Lease"}), 400

    rec = INFLIGHT.pop(lid)
    job = rec["job"]
    JOBS_QUEUE.append(job)

    svc, _ = _get_service()
    if svc:
        try:
            sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], "FAILED")
        except Exception as e:
            print("Update FAILED failed:", e)

    return jsonify({"ok": True, "res": "Requeued"})

@app.route("/cookies", methods=["GET"])
def cookies():
    return jsonify({"cookies": []})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
