import os
import time
import json
import uuid
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# =========================
# 1. QUEUE STORAGE (In-Memory)
# =========================
JOBS_QUEUE = []            # 대기 중인 작업 (READY)
INFLIGHT = {}              # 진행 중인 작업 (LEASED) -> {lease_id: {job, expires_at}}
DONE_LOG = []              # 완료된 작업 로그

LEASE_SECONDS_DEFAULT = 300  # 5분 임대

# Google Sheets Config
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# =========================
# 2. GOOGLE SHEETS HELPER
# =========================
def _gsheets_service():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json: return None
    try:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"Auth Error: {e}")
        return None

def _get_sheet_values(service, spreadsheet_id, sheet_name, a1_range="A:ZZ"):
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{sheet_name}!{a1_range}", valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        return resp.get("values", [])
    except: return []

def _batch_update_cells(service, spreadsheet_id, updates):
    if not updates: return
    try:
        data = [{"range": rng, "values": [[val]]} for rng, val in updates]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"valueInputOption": "USER_ENTERED", "data": data}
        ).execute()
    except: pass

def _col_to_a1(n):
    s = ""
    while n > 0: n, r = divmod(n - 1, 26); s = chr(65 + r) + s
    return s

def _find_header_map(values):
    return {str(name).strip(): idx for idx, name in enumerate(values[0]) if str(name).strip()} if values else {}

def _row_get(row, hmap, key):
    idx = hmap.get(key)
    return row[idx] if idx is not None and idx < len(row) else None

def _sheet_row_to_job(row_data, hmap):
    if not row_data: return None
    # 필수 필드 매핑
    title = _row_get(row_data, hmap, "title") or _row_get(row_data, hmap, "market_title")
    price = _row_get(row_data, hmap, "price") or _row_get(row_data, hmap, "sell_price")
    
    # 사진 추출
    photos = []
    for k in ["photo_url_1", "photo_url_2", "image_url_main", "photos"]:
        v = _row_get(row_data, hmap, k)
        if v and isinstance(v, str) and v.startswith("http"): photos.append(v)

    if not title: return None

    # 가격 안전 변환
    try: final_price = float(price) if price else 0.0
    except: final_price = 0.0

    return {
        "id": str(_row_get(row_data, hmap, "item_id") or int(time.time()*1000)),
        "title": str(title),
        "price": final_price,
        "photos": list(set(photos)),
        "status": "NEW"
    }

# =========================
# 3. QUEUE LOGIC (Lease/Ack)
# =========================
def now_ts():
    return int(time.time())

def requeue_expired_internal():
    """만료된 임대 작업을 READY로 복귀 (부활)"""
    t = now_ts()
    # 만료된 lease_id 찾기
    expired_ids = [lid for lid, rec in INFLIGHT.items() if rec.get("expires_at", 0) <= t]
    count = 0
    for lid in expired_ids:
        rec = INFLIGHT.pop(lid, None)
        if rec and rec.get("job"):
            JOBS_QUEUE.append(rec["job"]) # 대기열 맨 뒤로 복귀
            count += 1
    return count

# =========================
# 4. API ROUTES
# =========================
@app.route('/')
def home():
    return f"Empire Server vFinal. Ready: {len(JOBS_QUEUE)}, In-Flight: {len(INFLIGHT)}"

@app.route("/jobs/stats")
def jobs_stats():
    requeued = requeue_expired_internal()
    return jsonify({
        "ok": True,
        "queue_ready": len(JOBS_QUEUE),
        "queue_inflight": len(INFLIGHT),
        "requeued_now": requeued,
        "done_total": len(DONE_LOG)
    })

# --- [CRITICAL] Missing Part Added Back ---
@app.route("/jobs/push-from-sheet", methods=["POST", "GET"])
def jobs_push_from_sheet():
    svc = _gsheets_service()
    if not svc: return jsonify({"ok": False, "error": "Google Auth Failed"}), 500
    
    sid = os.environ.get("GSHEET_SPREADSHEET_ID")
    sname = os.environ.get("GSHEET_SHEET_NAME")
    limit = int(request.args.get("limit") or 10)
    
    values = _get_sheet_values(svc, sid, sname)
    if not values: return jsonify({"ok": False, "error": "Sheet Empty or Read Fail"}), 400

    hmap = _find_header_map(values)
    if "status" not in hmap: return jsonify({"ok": False, "error": "No 'status' column"}), 400
    
    pushed_jobs = []
    updates = []
    count = 0
    
    # 헤더 제외하고 순회
    for i in range(1, len(values)):
        if count >= limit: break
        row = values[i]
        
        # 'NEW' 상태인 것만 가져옴
        status_val = str(_row_get(row, hmap, "status")).strip().upper()
        if status_val != "NEW": continue
        
        job = _sheet_row_to_job(row, hmap)
        if not job: continue
        
        # 큐에 추가
        JOBS_QUEUE.append(job)
        pushed_jobs.append(job)
        count += 1
        
        # 시트 상태 업데이트 (NEW -> QUEUED)
        # i번째 행 -> 실제 시트 행번호는 i+1
        updates.append((f"{sname}!{_col_to_a1(hmap['status']+1)}{i+1}", "QUEUED"))
            
    if updates: _batch_update_cells(svc, sid, updates)
    
    return jsonify({
        "ok": True, 
        "pushed_count": count, 
        "queue_ready": len(JOBS_QUEUE)
    })

# --- Lease Logic ---
@app.route("/jobs/lease", methods=["GET"])
def jobs_lease():
    lease_seconds = int(request.args.get("lease_seconds", LEASE_SECONDS_DEFAULT))
    
    # 1. 만료된 것 먼저 회수
    requeued = requeue_expired_internal()

    # 2. 대기열 확인
    if not JOBS_QUEUE:
        return jsonify({"ok": True, "job": None, "requeued": requeued})

    # 3. 작업 꺼내기 (FIFO)
    job = JOBS_QUEUE.pop(0)
    
    # 4. 임대 장부(INFLIGHT)에 기록
    lease_id = str(uuid.uuid4())
    INFLIGHT[lease_id] = {
        "job": job,
        "expires_at": now_ts() + lease_seconds
    }

    return jsonify({
        "ok": True,
        "job": job,
        "lease_id": lease_id,
        "expires_at": INFLIGHT[lease_id]["expires_at"]
    })

# --- Ack Logic ---
@app.route("/jobs/ack", methods=["POST"])
def jobs_ack():
    data = request.json or {}
    lease_id = data.get("lease_id")
    status = data.get("status", "DONE")

    if lease_id in INFLIGHT:
        rec = INFLIGHT.pop(lease_id) # 장부에서 제거 (완전 처리됨)
        DONE_LOG.append({"id": rec["job"]["id"], "status": status, "at": now_ts()})
        return jsonify({"ok": True, "result": "ACK_ACCEPTED"})
    
    return jsonify({"ok": False, "error": "LEASE_NOT_FOUND_OR_EXPIRED"}), 400

@app.route("/jobs/fail", methods=["POST"])
def jobs_fail():
    data = request.json or {}
    lease_id = data.get("lease_id")
    
    if lease_id in INFLIGHT:
        rec = INFLIGHT.pop(lease_id)
        # 실패했으므로 다시 대기열로 복귀
        JOBS_QUEUE.append(rec["job"])
        return jsonify({"ok": True, "result": "REQUEUED"})
        
    return jsonify({"ok": False, "error": "LEASE_NOT_FOUND"}), 400

@app.route("/cookies", methods=["GET"])
def get_cookies():
    # 쿠키 로직이 필요하다면 여기에 구현 (현재는 빈 리스트 반환 예시)
    return jsonify({"cookies": []})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
