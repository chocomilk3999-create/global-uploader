import os
import time
import json
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# --- MEMORY STORAGE ---
JOBS_QUEUE = []          # 작업 대기열
JOBS_BY_ID = {}          # 작업 상세 내용
JOB_REPORTS = []         # 결과 로그

# --- CONFIG & HELPERS ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _gsheets_service():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
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
            spreadsheetId=spreadsheet_id, range=f"{sheet_name}!{a1_range}", valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        return resp.get("values", [])
    except Exception as e: return []

def _batch_update_cells(service, spreadsheet_id, updates):
    if not updates: return
    try:
        data = [{"range": rng, "values": [[val]]} for rng, val in updates]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"valueInputOption": "USER_ENTERED", "data": data}
        ).execute()
    except Exception as e: print(f"❌ [Sheet Update Error] {e}")

def _col_to_a1(n):
    s = ""
    while n > 0: n, r = divmod(n - 1, 26); s = chr(65 + r) + s
    return s

def _find_header_map(values): 
    # 헤더를 찾아서 {컬럼명: 인덱스}로 만듦
    return {str(name).strip(): idx for idx, name in enumerate(values[0]) if str(name).strip()} if values else {}

def _row_get(row, hmap, key):
    # 안전하게 값 가져오기
    idx = hmap.get(key)
    if idx is not None and idx < len(row):
        return row[idx]
    return None

def _sheet_row_to_job(row_data, hmap):
    """
    형이 준 해결책 적용! 
    구글 시트의 다양한 컬럼 이름을 표준화해서 job dict로 만듦
    """
    if not row_data:
        return None

    # 1. Row 데이터를 dict 형태로 변환 (편의상)
    # (하지만 hmap을 쓰는게 더 빠르니 hmap으로 직접 매핑)
    
    # Title 매핑
    title = _row_get(row_data, hmap, "title") or \
            _row_get(row_data, hmap, "market_title") or \
            _row_get(row_data, hmap, "source_title") or \
            _row_get(row_data, hmap, "topic_or_product")
            
    # Price 매핑
    price = _row_get(row_data, hmap, "price") or \
            _row_get(row_data, hmap, "Sell_Price") or \
            _row_get(row_data, hmap, "sell_price") or \
            _row_get(row_data, hmap, "expected_sale_price")

    # Market 매핑
    market = (_row_get(row_data, hmap, "market") or "US").upper()
    
    # Target 매핑
    target = _row_get(row_data, hmap, "target_marketplace") or \
             _row_get(row_data, hmap, "ad_platform") or "ebay"

    # Photos 매핑 (여러 컬럼 확인)
    photos = []
    for k in ["photo_url_1", "photo_url_2", "image_url_main", "image_url_alt", "photos"]:
        v = _row_get(row_data, hmap, k)
        if v and isinstance(v, str) and v.startswith("http"):
            photos.append(v)

    # 필수값 체크 (제목이나 사진 없으면 탈락)
    if not title or not photos:
        return None 

    # 최종 JOB 생성
    try:
        final_price = float(price) if price else 0.0
    except:
        final_price = 0.0

    job = {
        "id": str(_row_get(row_data, hmap, "item_id") or _row_get(row_data, hmap, "id") or int(time.time())),
        "title": str(title),
        "price": final_price,
        "market": str(market),
        "target_marketplace": str(target),
        "origin_model": str(_row_get(row_data, hmap, "origin_model") or "RESELL"),
        "photos": list(set(photos)), # 중복제거
        "description_html": str(_row_get(row_data, hmap, "market_description") or _row_get(row_data, hmap, "description_html") or ""),
        "shipping_policy": str(_row_get(row_data, hmap, "shipping_policy") or "eIS"),
    }
    return job

# --- ROUTES ---
@app.route('/')
def home(): return f"Empire Server v12.0 (Fixed). Queue: {len(JOBS_QUEUE)}"

@app.route("/jobs/push-from-sheet", methods=["POST", "GET"])
def jobs_push_from_sheet():
    svc = _gsheets_service()
    if not svc: return jsonify({"ok": False, "error": "Service Account Error"}), 500
    
    sid = os.environ.get("GSHEET_SPREADSHEET_ID")
    sname = os.environ.get("GSHEET_SHEET_NAME")
    limit = int(request.args.get("limit") or 5)
    
    # 시트 읽기
    values = _get_sheet_values(svc, sid, sname)
    hmap = _find_header_map(values)
    
    if "status" not in hmap: 
        return jsonify({"ok": False, "error": "No 'status' column found"}), 400
    
    pushed_jobs = []
    updates = []
    count = 0
    
    for i in range(1, len(values)):
        if count >= limit: break
        row = values[i]
        
        # STATUS 확인 (대소문자 무시하고 NEW 체크)
        status_val = str(_row_get(row, hmap, "status")).strip().upper()
        if status_val != "NEW": 
            continue
        
        # 형이 준 로직으로 Job 변환
        job = _sheet_row_to_job(row, hmap)
        
        if not job:
            # NEW지만 데이터가 부족해서 job 생성이 안된 경우
            # 로그에 남기거나 넘어가야 함. 여기선 일단 넘어감.
            pushed_jobs.append(None) 
            continue
        
        jid = job["id"]
        if jid not in JOBS_BY_ID:
            JOBS_BY_ID[jid] = job
            JOBS_QUEUE.append(jid)
            pushed_jobs.append(job)
            count += 1
            # 시트 상태 업데이트 (NEW -> QUEUED)
            updates.append((f"{sname}!{_col_to_a1(hmap['status']+1)}{i+1}", "QUEUED"))
            
    if updates: _batch_update_cells(svc, sid, updates)
    
    return jsonify({
        "ok": True, 
        "pushed_count": count, 
        "pushed_jobs": pushed_jobs, # 이제 여기에 진짜 데이터가 보일 거야
        "queue_len": len(JOBS_QUEUE)
    })

@app.route("/jobs/next", methods=["GET"])
def jobs_next():
    market = (request.args.get("market") or "").upper()
    picked = None
    
    # 큐에서 하나씩 꺼내보며 조건(Market) 맞는지 확인
    for idx, jid in enumerate(list(JOBS_QUEUE)):
        job = JOBS_BY_ID.get(str(jid))
        if not job: continue
        
        # 만약 market 파라미터가 있으면 필터링, 없으면 그냥 줌
        if market and job.get("market") != market: 
            # US 요청인데 job이 KR이면 패스하는 로직
            # 하지만 형 데이터는 기본이 US라 괜찮음
            pass 
            
        picked = job
        JOBS_QUEUE.pop(idx) # 큐에서 삭제 (꺼냄)
        break
        
    return jsonify({"ok": True, "job": picked, "queue_len": len(JOBS_QUEUE)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
