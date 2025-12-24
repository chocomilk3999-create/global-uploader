import os
import time
import uuid
import json
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# =========================
# CONFIG & STATE
# =========================
# 구글 시트 설정
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "Sheet1").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# 큐 설정
LEASE_SECONDS_DEFAULT = 300  # 5분 임대
JOBS_QUEUE = []              # 대기열 (READY)
INFLIGHT = {}                # 진행중 (LEASED) -> {lease_id: {job, expires_at}}
DONE_LOG = []                # 완료 로그

# =========================
# GOOGLE SHEETS HELPER
# =========================
def _get_service():
    # Render 환경변수에서 JSON 문자열을 로드
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json: return None
    try:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"GSheet Auth Error: {e}")
        return None

def _update_cell(service, row_idx, col_idx, value):
    # row_idx: 0-based (0 -> 1행), col_idx: 0-based (0 -> A열)
    # A1 표기법 변환
    def to_a1(c, r):
        s = ""
        c += 1 # 1-based 변환
        while c > 0: c, m = divmod(c - 1, 26); s = chr(65 + m) + s
        return f"{s}{r + 1}"
    
    range_name = f"{GSHEET_TAB}!{to_a1(col_idx, row_idx)}"
    try:
        service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID, range=range_name,
            valueInputOption="USER_ENTERED", body={"values": [[value]]}
        ).execute()
    except Exception as e:
        print(f"Sheet Update Error: {e}")

# =========================
# QUEUE LOGIC
# =========================
def now_ts(): return int(time.time())

def requeue_expired():
    # 만료된 작업 회수
    expired = [lid for lid, rec in INFLIGHT.items() if rec["expires_at"] <= now_ts()]
    count = 0
    for lid in expired:
        rec = INFLIGHT.pop(lid)
        # 다시 대기열로 복귀 (Queue Loss 방지)
        JOBS_QUEUE.append(rec["job"])
        count += 1
        # (옵션) 시트 상태를 다시 QUEUED로 돌릴 수도 있음
    return count

# =========================
# API ENDPOINTS
# =========================
@app.route("/")
def home():
    return f"Empire Server vFinal. Ready: {len(JOBS_QUEUE)}, In-Flight: {len(INFLIGHT)}"

@app.route("/jobs/stats")
def stats():
    return jsonify({
        "ok": True,
        "ready": len(JOBS_QUEUE),
        "inflight": len(INFLIGHT),
        "requeued": requeue_expired(),
        "done": len(DONE_LOG)
    })

# [1] 시트 읽기 -> 큐 주입 (Make 대체)
@app.route("/jobs/push-from-sheet", methods=["POST", "GET"])
def push_from_sheet():
    svc = _get_service()
    if not svc: return jsonify({"ok": False, "error": "Auth Failed"}), 500
    
    limit = int(request.args.get("limit", 20))
    
    # 전체 데이터 읽기 (헤더 포함)
    resp = svc.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=f"{GSHEET_TAB}!A1:ZZ").execute()
    rows = resp.get("values", [])
    if len(rows) < 2: return jsonify({"ok": False, "msg": "Empty Sheet"})
    
    header = rows[0]
    # 헤더 매핑 (status 컬럼 찾기)
    try:
        status_idx = next(i for i, h in enumerate(header) if str(h).strip().lower() == "status")
    except:
        return jsonify({"ok": False, "error": "No 'status' column found"}), 400
    
    pushed = 0
    
    for i in range(1, len(rows)): # 데이터 행 반복
        if pushed >= limit: break
        row = rows[i]
        
        # status 확인 (안전하게 가져오기)
        current_status = row[status_idx] if len(row) > status_idx else ""
        if str(current_status).strip().upper() != "NEW": continue
        
        # Job 생성 (필요한 컬럼 매핑 - 형 시트에 맞춰 수정 가능)
        # 여기서는 단순히 행 전체와 인덱스를 저장
        job = {
            "id": str(uuid.uuid4()),
            "sheet_row_idx": i,       # 중요: 0-based 행 인덱스
            "sheet_status_col": status_idx,
            "data": row,              # 행 데이터 통째로 (Agent가 파싱)
            "header": header          # 헤더 정보도 전달
        }
        
        JOBS_QUEUE.append(job)
        
        # 시트 상태 업데이트: NEW -> QUEUED
        _update_cell(svc, i, status_idx, "QUEUED")
        pushed += 1
        
    return jsonify({"ok": True, "pushed": pushed, "queue_len": len(JOBS_QUEUE)})

# [2] 작업 임대 (Agent가 가져감)
@app.route("/jobs/lease", methods=["GET"])
def lease():
    requeue_expired() # 만료 회수 먼저
    
    if not JOBS_QUEUE:
        return jsonify({"ok": True, "job": None})
    
    # 큐에서 하나 꺼냄 (FIFO)
    job = JOBS_QUEUE.pop(0)
    
    lease_id = str(uuid.uuid4())
    INFLIGHT[lease_id] = {
        "job": job,
        "expires_at": now_ts() + LEASE_SECONDS_DEFAULT
    }
    
    # 시트 상태 업데이트: QUEUED -> INFLIGHT
    svc = _get_service()
    if svc:
        _update_cell(svc, job["sheet_row_idx"], job["sheet_status_col"], "INFLIGHT")
        
    return jsonify({
        "ok": True,
        "job": job,
        "lease_id": lease_id
    })

# [3] 성공 보고 (Ack)
@app.route("/jobs/ack", methods=["POST"])
def ack():
    data = request.json or {}
    lease_id = data.get("lease_id")
    status = data.get("status", "DRAFTED") # 기본값 DRAFTED
    
    if lease_id in INFLIGHT:
        rec = INFLIGHT.pop(lease_id)
        job = rec["job"]
        DONE_LOG.append({"id": job["id"], "ts": now_ts()})
        
        # 시트 상태 업데이트: INFLIGHT -> DRAFTED (또는 형이 보낸 status)
        svc = _get_service()
        if svc:
            _update_cell(svc, job["sheet_row_idx"], job["sheet_status_col"], status)
            
        return jsonify({"ok": True, "result": "ACK"})
    
    return jsonify({"ok": False, "error": "Invalid Lease"}), 400

# [4] 실패 보고 (Fail)
@app.route("/jobs/fail", methods=["POST"])
def fail():
    data = request.json or {}
    lease_id = data.get("lease_id")
    reason = data.get("reason", "Error")
    
    if lease_id in INFLIGHT:
        rec = INFLIGHT.pop(lease_id)
        job = rec["job"]
        
        # 다시 큐에 넣음 (재시도) - 또는 영구 실패 처리 가능
        # 여기서는 '재시도'를 위해 큐에 넣고 시트는 FAILED로 표시
        JOBS_QUEUE.append(job) 
        
        svc = _get_service()
        if svc:
            _update_cell(svc, job["sheet_row_idx"], job["sheet_status_col"], "FAILED")
            # 에러 메시지 컬럼이 있다면 거기에 reason을 적을 수도 있음
            
        return jsonify({"ok": True, "result": "REQUEUED"})
        
    return jsonify({"ok": False, "error": "Invalid Lease"}), 400

# [5] 쿠키 (옵션)
@app.route("/cookies", methods=["GET"])
def cookies(): return jsonify({"cookies": []}) # 필요 시 구현

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
