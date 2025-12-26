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
EBAY_PRESETS_TAB = os.getenv("EBAY_PRESETS_TAB", "EBAY_PRESETS").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEASE_SECONDS = 300

# --- STORAGE ---
JOBS_QUEUE = []
INFLIGHT = {}
PRESETS_CACHE = []
LAST_PRESET_LOAD = 0

def now_ts(): return int(time.time())

def _get_service():
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
    if not creds_json: return None, "No Creds"
    try:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return svc, "OK"
    except Exception as e: return None, str(e)

def to_a1(col0, row0):
    c = col0 + 1
    s = ""
    while c > 0:
        c, m = divmod(c - 1, 26)
        s = chr(65 + m) + s
    return f"{s}{row0 + 1}"

def sheet_get_tab(svc, tab_name: str):
    return svc.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range=f"{tab_name}!A1:ZZ"
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

def rows_to_dicts(values):
    if not values or len(values) < 2: return []
    header = [str(x).strip().lower() for x in values[0]]
    out = []
    for r in values[1:]:
        d = {}
        for i, h in enumerate(header):
            d[h] = r[i] if i < len(r) else ""
        out.append(d)
    return out

# --- ROUTES ---
@app.route("/")
def home():
    return f"Empire Brain Online. Queue={len(JOBS_QUEUE)}"

@app.route("/debug/google")
def debug_google():
    svc, msg = _get_service()
    return jsonify({"ok": svc is not None, "msg": msg})

@app.route("/presets/ebay", methods=["GET"])
def presets_ebay():
    """[Auditor] EBAY_PRESETS 시트를 읽어서 반환"""
    svc, msg = _get_service()
    if not svc: return jsonify({"ok": False, "error": "Auth Failed"}), 500
    try:
        resp = sheet_get_tab(svc, EBAY_PRESETS_TAB)
        values = resp.get("values", [])
        presets = rows_to_dicts(values)
        
        # enabled 필터
        filtered = []
        for p in presets:
            enabled = str(p.get("enabled", "TRUE")).strip().upper()
            if enabled in ("TRUE", "1", "YES", "Y"):
                filtered.append(p)
                
        return jsonify({"ok": True, "count": len(filtered), "presets": filtered})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/jobs/push-from-sheet", methods=["GET", "POST"])
def push_from_sheet():
    svc, msg = _get_service()
    if not svc: return jsonify({"ok": False}), 500
    limit = int(request.args.get("limit", 20))
    try: resp = sheet_get_tab(svc, GSHEET_TAB)
    except Exception as e: return jsonify({"ok": False, "err": str(e)}), 400

    rows = resp.get("values", [])
    if len(rows) < 2: return jsonify({"ok": True, "pushed": 0}), 200

    header = rows[0]
    try: status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except: return jsonify({"ok": False, "err": "no status col"}), 400

    pushed = 0
    for i in range(1, len(rows)):
        if pushed >= limit: break
        row = rows[i]
        curr = row[status_idx] if len(row) > status_idx else ""
        if str(curr).strip().upper() == "NEW":
            job = {"id": str(uuid.uuid4()), "sheet_row": i, "sheet_status_col": status_idx, "header": header, "data": row}
            JOBS_QUEUE.append(job)
            try: sheet_update_cell(svc, i, status_idx, "QUEUED")
            except: pass
            pushed += 1
    return jsonify({"ok": True, "pushed": pushed})

@app.route("/jobs/lease", methods=["GET"])
def lease():
    requeue_expired()
    if not JOBS_QUEUE: return jsonify({"ok": True, "job": None})
    job = JOBS_QUEUE.pop(0)
    lid = str(uuid.uuid4())
    INFLIGHT[lid] = {"job": job, "expires_at": now_ts() + LEASE_SECONDS}
    svc, _ = _get_service()
    if svc:
        try: sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], "INFLIGHT")
        except: pass
    return jsonify({"ok": True, "job": job, "lease_id": lid})

@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lid = data.get("lease_id")
    if lid not in INFLIGHT: return jsonify({"ok": False}), 400
    rec = INFLIGHT.pop(lid)
    svc, _ = _get_service()
    if svc:
        try:
            sheet_update_cell(svc, rec["job"]["sheet_row"], rec["job"]["sheet_status_col"], data.get("status", "DRAFTED"))
            sheet_update_by_header(svc, rec["job"]["sheet_row"], rec["job"]["header"], data.get("updates", {}))
        except: pass
    return jsonify({"ok": True})

@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lid = data.get("lease_id")
    if lid not in INFLIGHT: return jsonify({"ok": False}), 400
    rec = INFLIGHT.pop(lid)
    svc, _ = _get_service()
    if svc:
        try: sheet_update_cell(svc, rec["job"]["sheet_row"], rec["job"]["sheet_status_col"], "FAILED")
        except: pass
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
