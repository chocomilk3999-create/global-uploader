import os
import time
import json
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, request, jsonify

# Google Sheets 라이브러리 (Step 3에서 설치됨)
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# =========================
# 🏗️ 데이터 저장소 (Memory)
# =========================
JOBS_QUEUE = []          # 대기열
JOBS_BY_ID = {}          # ID 조회용
JOB_REPORTS = []         # 리포트 로그
COOKIES_STORE = {}       # 쿠키 저장소

# =========================
# 🔐 Google Sheets Helpers
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _gsheets_service():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        # 로컬 테스트나 환경변수 없을 때를 위한 방어 로직
        print("⚠️ [Warning] GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
        return None
    try:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ [Sheet Auth Error] {e}")
        return None

def _get_sheet_values(service, spreadsheet_id, sheet_name, a1_range="A:ZZ"):
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!{a1_range}",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        return resp.get("values", [])
    except Exception as e:
        print(f"❌ [Sheet Read Error] {e}")
        return []

def _batch_update_cells(service, spreadsheet_id, updates):
    """
    updates: [("Sheet!C5", "QUEUED"), ...]
    """
    if not updates: return
    try:
        data = [{"range": rng, "values": [[val]]} for rng, val in updates]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data}
        ).execute()
    except Exception as e:
        print(f"❌ [Sheet Update Error] {e}")

# =========================
# 🛠️ Utility Functions
# =========================
def _now(): return int(time.time())

def _col_to_a1(col_idx_1based: int) -> str:
    s = ""
    n = col_idx_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _find_header_map(values: List[List[str]]) -> Dict[str, int]:
    if not values or not values[0]: return {}
    header = [str(x).strip() for x in values[0]]
    return {name: idx for idx, name in enumerate(header) if name}

def _row_get(row, header_map, key, default=None):
    idx = header_map.get(key)
    if idx is None or idx >= len(row): return default
    v = row[idx]
    return v if v is not None else default

def _to_str(v) -> str: return "" if v is None else str(v)

def _to_float(v, default=0.0) -> float:
    try: return float(v) if v else default
    except: return default

def _to_int(v, default=0) -> int:
    try: return int(float(v)) if v else default
    except: return default

def _normalize_job(job: dict) -> dict:
    # (기존 로직 유지 + 시트 데이터 호환 강화)
    job = dict(job or {})
    job.setdefault("id", str(_to_str(job.get("id") or job.get("job_id") or job.get("item_id") or _now())))
    job.setdefault("market", _to_str(job.get("market") or "US").upper())
    job.setdefault("target_marketplace", _to_str(job.get("target_marketplace") or "ebay"))
    job.setdefault("origin_model", _to_str(job.get("origin_model") or "RESELL"))
    
    # Title & Price
    job.setdefault("title", _to_str(job.get("title") or job.get("market_title") or ""))
    job["price"] = _to_float(job.get("price") or job.get("sell_price") or job.get("price_usd"), 0.0)
    job.setdefault("currency", _to_str(job.get("currency") or "USD"))
    job.setdefault("qty", _to_int(job.get("qty") or 1))

    # Photos
    photos = job.get("photos") or job.get("images") or []
    if not isinstance(photos, list): photos = []
    # 개별 필드 흡수
    for k in ["photo_url_1", "photo_url_2", "photo_url_3", "image_url_main", "image_url_alt"]:
        v = job.get(k)
        if v and isinstance(v, str) and v.startswith("http"): photos.append(v)
    job["photos"] = list(set(photos))

    # Description
    job.setdefault("description_html", _to_str(job.get("description_html") or job.get("market_description") or job.get("SmartStore_HTML") or ""))
    
    job.setdefault("created_at", _now())
    return job

def sheet_row_to_job(row, header_map) -> dict:
    # 시트의 1행 데이터를 job 딕셔너리로 변환
    # (형님 시트의 다양한 컬럼명에 대응하도록 설계됨)
    raw_job = {
        "id": _row_get(row, header_map, "item_id") or _row_get(row, header_map, "id"),
        "market": _row_get(row, header_map, "market"),
        "target_marketplace": _row_get(row, header_map, "target_marketplace") or _row_get(row, header_map, "marketplace"),
        "origin_model": _row_get(row, header_map, "origin_model") or _row_get(row, header_map, "engine"),
        "title": _row_get(row, header_map, "market_title") or _row_get(row, header_map, "source_title") or _row_get(row, header_map, "title"),
        "price": _row_get(row, header_map, "Sell_Price") or _row_get(row, header_map, "sell_price") or _row_get(row, header_map, "expected_sale_price"),
        "qty": _row_get(row, header_map, "qty") or _row_get(row, header_map, "quantity"),
        "currency": _row_get(row, header_map, "currency"),
        "shipping_policy": _row_get(row, header_map, "shipping_policy"),
        "description_html": _row_get(row, header_map, "market_description") or _row_get(row, header_map, "SmartStore_HTML") or _row_get(row, header_map, "description_html"),
        
        # 사진들
        "photo_url_1": _row_get(row, header_map, "photo_url_1"),
        "photo_url_2": _row_get(row, header_map, "photo_url_2"),
        "photo_url_3": _row_get(row, header_map, "photo_url_3"),
        "image_url_main": _row_get(row, header_map, "image_url_main"),
        "image_url_alt": _row_get(row, header_map, "image_url_alt"),
    }
    return _normalize_job(raw_job)

# =========================
# 🌐 Routes
# =========================
@app.route('/')
def home():
    return f"Empire Server Running. Queue: {len(JOBS_QUEUE)} | Reports: {len(JOB_REPORTS)}"

# [NEW] 시트에서 일감 가져오기 (Make 대체)
@app.route("/jobs/push-from-sheet", methods=["POST", "GET"])
def jobs_push_from_sheet():
    svc = _gsheets_service()
    if not svc:
        return jsonify({"ok": False, "error": "Service Account not configured"}), 500

    spreadsheet_id = os.environ.get("GSHEET_SPREADSHEET_ID")
    sheet_name = os.environ.get("GSHEET_SHEET_NAME")
    if not spreadsheet_id or not sheet_name:
        return jsonify({"ok": False, "error": "Sheet env vars missing"}), 400

    # 파라미터 처리
    if request.method == "POST":
        payload = request.json or {}
        limit = int(payload.get("limit", 5))
        market_filter = (payload.get("market") or "").upper().strip()
        status_value = (payload.get("status_value") or "NEW").upper().strip()
    else:
        limit = int(request.args.get("limit", 5))
        market_filter = (request.args.get("market") or "").upper().strip()
        status_value = (request.args.get("status_value") or "NEW").upper().strip()

    # 1. 시트 읽기
    values = _get_sheet_values(svc, spreadsheet_id, sheet_name)
    header_map = _find_header_map(values)
    
    if "status" not in header_map:
        return jsonify({"ok": False, "error": "'status' column missing in sheet"}), 400

    # 2. NEW -> QUEUED 처리
    pushed = []
    updates = []
    queued_count = 0

    for i in range(1, len(values)):
        if queued_count >= limit: break
        
        row = values[i]
        row_status = _to_str(_row_get(row, header_map, "status")).upper().strip()
        
        if row_status != status_value: continue # NEW가 아니면 스킵
        if market_filter: # 마켓 필터 있으면 체크
            if _to_str(_row_get(row, header_map, "market")).upper() != market_filter: continue

        # Job 변환
        job = sheet_row_to_job(row, header_map)
        
        # 필수값 체크 (제목/사진 없으면 위험하니까 스킵)
        if not job["title"] or not job["photos"]: continue

        # 큐에 넣기
        job_id = job["id"]
        if job_id not in JOBS_BY_ID: # 중복 아니면
            JOBS_BY_ID[job_id] = job
            JOBS_QUEUE.append(job_id)
            pushed.append(job)
            queued_count += 1
            
            # 시트 업데이트 예약
            row_num = i + 1
            status_col = header_map["status"] + 1
            updates.append((f"{sheet_name}!{_col_to_a1(status_col)}{row_num}", "QUEUED"))

    # 3. 시트 일괄 업데이트
    if updates:
        _batch_update_cells(svc, spreadsheet_id, updates)

    return jsonify({
        "ok": True,
        "pushed_count": queued_count,
        "queue_len": len(JOBS_QUEUE),
        "pushed_jobs": [j["id"] for j in pushed]
    })

# 기존 API들 (에이전트용)
@app.route("/jobs/next", methods=["GET"])
def jobs_next():
    market = (request.args.get("market") or "").upper().strip()
    picked_id = None
    picked_job = None

    for idx, job_id in enumerate(list(JOBS_QUEUE)):
        job = JOBS_BY_ID.get(str(job_id))
        if not job: continue
        if market and job.get("market") != market: continue
        
        picked_id = str(job_id)
        picked_job = job
        JOBS_QUEUE.pop(idx)
        break

    if picked_job:
        picked_job["status"] = "DISPATCHED"
        JOBS_BY_ID[picked_id] = picked_job

    return jsonify({"ok": True, "job": picked_job, "queue_len": len(JOBS_QUEUE)})

@app.route("/jobs/report", methods=["POST"])
def jobs_report():
    data = request.json or {}
    JOB_REPORTS.append({**data, "server_ts": _now()})
    
    job_id = str(data.get("job_id") or "")
    if job_id in JOBS_BY_ID:
        JOBS_BY_ID[job_id]["last_status"] = data.get("status")
        
        # [NEW] 완료 시 시트 status 업데이트 (LISTED, ERROR 등)
        # (이건 에이전트가 리포트할 때마다 시트도 업데이트하는 옵션인데,
        #  너무 빈번한 API 호출을 막으려면 나중에 배치로 처리하거나,
        #  일단 지금은 QUEUED까지만 서버가 하고, 완료 처리는 Make가 하거나
        #  형님이 원하시면 여기에도 시트 업데이트 로직 추가 가능합니다.)
        
    if len(JOB_REPORTS) > 1000: del JOB_REPORTS[:-1000]
    return jsonify({"ok": True})

# [LEGACY] 구형 호환
@app.route("/queue/next", methods=["GET"])
def legacy_next():
    res = jobs_next().get_json()
    if not res.get("job"): return '', 204
    j = res["job"]
    return jsonify({"job_id": j["id"], "market_title": j["title"], "market_description_html": j["description_html"], "price_usd": j["price"], "photo_urls": j["photos"]})

@app.route('/queue/report', methods=['POST'])
def legacy_report(): return jobs_report()

@app.route('/upload-global', methods=['POST']) # Make용 구형 엔드포인트도 일단 유지
def legacy_upload(): return jsonify({"ok": True, "msg": "Use /jobs/push-from-sheet instead"}) 

@app.route('/cookies', methods=['POST', 'GET'])
def cookies_handler():
    if request.method == 'POST':
        COOKIES_STORE['default'] = request.json
        return jsonify({"status": "saved"})
    data = COOKIES_STORE.get('default', {"cookies": [], "origins": []})
    if "origins" not in data: data["origins"] = []
    return jsonify(data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
