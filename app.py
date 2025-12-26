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
PRESETS_TAB = "HQ_CATEGORY_PRESETS" # [Auditor] 프리셋 탭 이름 고정
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEASE_SECONDS = 300  # 5 min

# --- STORAGE (RAM) ---
JOBS_QUEUE = []
INFLIGHT = {}
# [Auditor] 프리셋 캐싱 (서버 뜰 때 한 번 읽거나, 필요시 갱신)
PRESETS_CACHE = []
LAST_PRESET_LOAD = 0

def now_ts(): 
    return int(time.time())

def _get_service():
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        return None, "ERROR: ENV 'GOOGLE_APPLICATION_CREDENTIALS_JSON' is empty."
    try:
        info = json.loads(creds_json)
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

def sheet_get_all(svc, tab_name):
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
    return len(expired)

def load_presets_if_needed(svc):
    """[Auditor] 프리셋 시트를 읽어서 RAM에 저장 (60초 캐시)"""
    global PRESETS_CACHE, LAST_PRESET_LOAD
    if now_ts() - LAST_PRESET_LOAD < 60 and PRESETS_CACHE:
        return

    try:
        resp = sheet_get_all(svc, PRESETS_TAB)
        rows = resp.get("values", [])
        if len(rows) < 2:
            PRESETS_CACHE = []
            return
        
        header = [str(h).strip().lower() for h in rows[0]]
        temp_list = []
        for r in rows[1:]:
            item = {}
            for i, val in enumerate(r):
                if i < len(header):
                    item[header[i]] = val
            # Active=TRUE 인 것만
            if str(item.get("active", "")).upper() in ["TRUE", "YES", "1"]:
                temp_list.append(item)
        
        PRESETS_CACHE = temp_list
        LAST_PRESET_LOAD = now_ts()
        print(f"Loaded {len(PRESETS_CACHE)} presets.")
    except Exception as e:
        print("Preset Load Failed:", e)

# --- ROUTES ---

@app.route("/")
def home():
    return f"Empire Brain Online. Queue={len(JOBS_QUEUE)} Inflight={len(INFLIGHT)} Presets={len(PRESETS_CACHE)}"

@app.route("/debug/google")
def debug_google():
    svc, msg = _get_service()
    return jsonify({"ok": svc is not None, "message": msg, "sheet_id": GSHEET_ID})

@app.route("/jobs/push-from-sheet", methods=["GET", "POST"])
def push_from_sheet():
    svc, msg = _get_service()
    if not svc: return jsonify({"ok": False, "error": "Auth Failed"}), 500

    limit = int(request.args.get("limit", 20))
    try:
        resp = sheet_get_all(svc, GSHEET_TAB)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    rows = resp.get("values", [])
    if len(rows) < 2: return jsonify({"ok": True, "pushed": 0, "msg": "Empty"}), 200

    header = rows[0]
    try:
        status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except:
        return jsonify({"ok": False, "error": "No status col"}), 400

    pushed = 0
    for i in range(1, len(rows)):
        if pushed >= limit: break
        row = rows[i]
        curr = row[status_idx] if len(row) > status_idx else ""
        if str(curr).strip().upper() == "NEW":
            job = {
                "id": str(uuid.uuid4()),
                "sheet_row": i,
                "sheet_status_col": status_idx,
                "header": header,
                "data": row
            }
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

@app.route("/presets", methods=["GET"])
def get_presets():
    """[Auditor] 에이전트에게 프리셋 목록 제공"""
    svc, _ = _get_service()
    if svc: load_presets_if_needed(svc)
    return jsonify({"ok": True, "presets": PRESETS_CACHE})

@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lid = data.get("lease_id")
    status = data.get("status", "DRAFTED")
    updates = data.get("updates", {}) 

    if lid not in INFLIGHT: return jsonify({"ok": False}), 400
    rec = INFLIGHT.pop(lid)
    job = rec["job"]
    svc, _ = _get_service()
    if svc:
        try:
            sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], status)
            sheet_update_by_header(svc, job["sheet_row"], job["header"], updates)
        except: pass
    return jsonify({"ok": True})

@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lid = data.get("lease_id")
    if lid not in INFLIGHT: return jsonify({"ok": False}), 400
    rec = INFLIGHT.pop(lid)
    job = rec["job"]
    # [Auditor] 실패 시 재큐 없이 FAILED 처리
    svc, _ = _get_service()
    if svc:
        try: sheet_update_cell(svc, job["sheet_row"], job["sheet_status_col"], "FAILED")
        except: pass
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
