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
LEASE_SECONDS = 300  # 5분

# --- STORAGE ---
JOBS_QUEUE = []
INFLIGHT = {}
DONE_LOG = []

# --- HELPERS ---
def _get_service():
    # 환경변수 JSON 문자열을 로드
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json: return None
    try:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"Auth Error: {e}"); return None

def _update_status(service, row_idx_0based, col_idx_0based, new_status):
    # A1 표기법 변환 로직
    def to_a1(c, r):
        c += 1
        s = ""
        while c > 0: c, m = divmod(c - 1, 26); s = chr(65 + m) + s
        return f"{s}{r + 1}"
    
    range_name = f"{GSHEET_TAB}!{to_a1(col_idx_0based, row_idx_0based)}"
    try:
        service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID, range=range_name,
            valueInputOption="USER_ENTERED", body={"values": [[new_status]]}
        ).execute()
    except Exception as e: print(f"Update Error: {e}")

def now_ts(): return int(time.time())

def requeue_expired():
    expired = [k for k, v in INFLIGHT.items() if v["expires_at"] <= now_ts()]
    count = 0
    for k in expired:
        rec = INFLIGHT.pop(k)
        JOBS_QUEUE.append(rec["job"]) # 대기열 복귀
        count += 1
    return count

# --- ROUTES ---
@app.route("/")
def home(): return f"Empire Brain Online. Queue: {len(JOBS_QUEUE)}"

@app.route("/jobs/push-from-sheet", methods=["POST", "GET"])
def push_from_sheet():
    svc = _get_service()
    if not svc: return jsonify({"ok": False, "error": "Google Auth Failed"}), 500
    
    limit = int(request.args.get("limit", 20))
    resp = svc.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=f"{GSHEET_TAB}!A1:ZZ").execute()
    rows = resp.get("values", [])
    if len(rows) < 2: return jsonify({"ok": False, "msg": "Sheet Empty"})
    
    header = rows[0]
    # 'status' 컬럼 찾기 (대소문자 무관)
    try:
        status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except: return jsonify({"ok": False, "error": "No 'status' column"}), 400
    
    pushed = 0
    for i in range(1, len(rows)):
        if pushed >= limit: break
        row = rows[i]
        curr = row[status_idx] if len(row) > status_idx else ""
        
        if str(curr).strip().upper() == "NEW":
            # Job 패키징 (행 데이터 전체 + 헤더)
            job = {
                "id": str(uuid.uuid4()),
                "sheet_row": i,
                "sheet_status_col": status_idx,
                "header": header,
                "data": row
            }
            JOBS_QUEUE.append(job)
            _update_status(svc, i, status_idx, "QUEUED")
            pushed += 1
            
    return jsonify({"ok": True, "pushed": pushed, "queue": len(JOBS_QUEUE)})

@app.route("/jobs/lease", methods=["GET"])
def lease():
    requeue_expired()
    if not JOBS_QUEUE: return jsonify({"ok": True, "job": None})
    
    job = JOBS_QUEUE.pop(0)
    lid = str(uuid.uuid4())
    INFLIGHT[lid] = {"job": job, "expires_at": now_ts() + LEASE_SECONDS}
    
    # 상태: INFLIGHT
    svc = _get_service()
    if svc: _update_status(svc, job["sheet_row"], job["sheet_status_col"], "INFLIGHT")
    
    return jsonify({"ok": True, "job": job, "lease_id": lid})

@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lid = data.get("lease_id")
    status = data.get("status", "DRAFTED")
    
    if lid in INFLIGHT:
        rec = INFLIGHT.pop(lid)
        job = rec["job"]
        # 상태: 완료 (DRAFTED 등)
        svc = _get_service()
        if svc: _update_status(svc, job["sheet_row"], job["sheet_status_col"], status)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "No Lease"}), 400

@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lid = data.get("lease_id")
    
    if lid in INFLIGHT:
        rec = INFLIGHT.pop(lid)
        job = rec["job"]
        JOBS_QUEUE.append(job) # 재시도 위해 복귀
        # 상태: FAILED (잠시 표시)
        svc = _get_service()
        if svc: _update_status(svc, job["sheet_row"], job["sheet_status_col"], "FAILED")
        return jsonify({"ok": True, "res": "Requeued"})
    return jsonify({"ok": False}), 400

@app.route("/cookies", methods=["GET"])
def cookies(): return jsonify({"cookies": []})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
