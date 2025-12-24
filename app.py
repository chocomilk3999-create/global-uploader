import os
import time
import uuid
import json
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# ======================================================
# [최종] Secret File 방식 (가장 안전하고 확실함)
# ======================================================

# 1. Render에 올린 비밀 파일 경로 (고정)
CREDENTIALS_PATH = "/etc/secrets/google_key.json"

# 2. 구글 시트 ID (회장님 시트 ID)
DIRECT_SHEET_ID = "1eZXsPLw7fpw9czIpZtXa73dO5nUiFKFcWYxk6kIxMEE"

# 3. 탭 이름
DIRECT_SHEET_TAB = "Sheet1"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEASE_SECONDS = 300
JOBS_QUEUE = []
INFLIGHT = {}

def now_ts(): return int(time.time())

# [핵심] 파일에서 인증 정보 로드
def _get_service():
    if not os.path.exists(CREDENTIALS_PATH):
        return None, f"ERROR: Key file not found at {CREDENTIALS_PATH}. Did you add it to Secret Files?"

    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return svc, f"OK (File Auth). Email: {creds.service_account_email}"
    except Exception as e:
        return None, f"Auth Error: {str(e)}"

# --- UTILS (기존 동일) ---
def to_a1(col0, row0):
    c = col0 + 1
    s = ""
    while c > 0: c, m = divmod(c - 1, 26); s = chr(65 + m) + s
    return f"{s}{row0 + 1}"

def _update_status(service, row0, col0, new_status):
    rng = f"{DIRECT_SHEET_TAB}!{to_a1(col0, row0)}"
    try:
        service.spreadsheets().values().update(
            spreadsheetId=DIRECT_SHEET_ID, range=rng,
            valueInputOption="USER_ENTERED", body={"values": [[new_status]]},
        ).execute()
    except Exception as e: print(f"Update Error: {e}")

def requeue_expired():
    expired = [k for k, v in INFLIGHT.items() if v["expires_at"] <= now_ts()]
    for k in expired:
        rec = INFLIGHT.pop(k)
        JOBS_QUEUE.append(rec["job"])
    return len(expired)

# --- ROUTES ---
@app.route("/")
def home():
    return f"Empire Brain Online (File Mode). Queue={len(JOBS_QUEUE)}"

@app.route("/debug/google")
def debug_google():
    svc, msg = _get_service()
    return jsonify({
        "ok": svc is not None,
        "message": msg,
        "target_sheet_id": DIRECT_SHEET_ID
    })

@app.route("/jobs/push-from-sheet", methods=["GET", "POST"])
def push_from_sheet():
    svc, msg = _get_service()
    if not svc: return jsonify({"ok": False, "error": msg}), 500

    limit = int(request.args.get("limit", 20))
    try:
        resp = svc.spreadsheets().values().get(spreadsheetId=DIRECT_SHEET_ID, range=f"{DIRECT_SHEET_TAB}!A1:ZZ").execute()
    except Exception as e:
        return jsonify({"ok": False, "error": "Sheet Read Failed", "detail": str(e)}), 400
        
    rows = resp.get("values", [])
    if len(rows) < 2: return jsonify({"ok": False, "msg": "Sheet Empty"}), 200

    header = rows[0]
    try:
        status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except: return jsonify({"ok": False, "error": "No 'status' column"}), 400

    pushed = 0
    pushed_jobs = []
    for i in range(1, len(rows)):
        if pushed >= limit: break
        row = rows[i]
        curr = row[status_idx] if len(row) > status_idx else ""
        if str(curr).strip().upper() == "NEW":
            job = { "id": str(uuid.uuid4()), "sheet_row": i, "sheet_status_col": status_idx, "header": header, "data": row }
            JOBS_QUEUE.append(job)
            _update_status(svc, i, status_idx, "QUEUED")
            pushed += 1
            pushed_jobs.append(job["id"])

    return jsonify({"ok": True, "pushed": pushed})

# (Lease/Ack/Fail/Cookies 라우트 유지)
@app.route("/jobs/lease", methods=["GET"])
def lease():
    requeue_expired()
    if not JOBS_QUEUE: return jsonify({"ok": True, "job": None})
    job = JOBS_QUEUE.pop(0)
    lid = str(uuid.uuid4())
    INFLIGHT[lid] = {"job": job, "expires_at": now_ts() + LEASE_SECONDS}
    svc, _ = _get_service()
    if svc: _update_status(svc, job["sheet_row"], job["sheet_status_col"], "INFLIGHT")
    return jsonify({"ok": True, "job": job, "lease_id": lid})

@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lid = data.get("lease_id")
    status = data.get("status", "DRAFTED")
    if lid in INFLIGHT:
        rec = INFLIGHT.pop(lid)
        svc, _ = _get_service()
        if svc: _update_status(svc, rec["job"]["sheet_row"], rec["job"]["sheet_status_col"], status)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "No Lease"}), 400

@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lid = data.get("lease_id")
    if lid in INFLIGHT:
        rec = INFLIGHT.pop(lid)
        JOBS_QUEUE.append(rec["job"])
        svc, _ = _get_service()
        if svc: _update_status(svc, rec["job"]["sheet_row"], rec["job"]["sheet_status_col"], "FAILED")
        return jsonify({"ok": True, "res": "Requeued"})
    return jsonify({"ok": False}), 400

@app.route("/cookies", methods=["GET"])
def cookies(): return jsonify({"cookies": []})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
