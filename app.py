import os
import time
import uuid
import json
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# ======================================================
# [최종 설정] 환경변수 무시하고 직접 입력 (Hardcoding)
# ======================================================

# 1. 구글 시트 ID (회장님이 주신 ID 적용 완료)
DIRECT_SHEET_ID = "1eZXsPLw7fpw9czIpZtXa73dO5nUiFKFcWYxk6kIxMEE"

# 2. 탭 이름 (기본값 Sheet1 - 필요시 수정하세요)
DIRECT_SHEET_TAB = "Sheet1"

# 3. 인증키 (자동 주입 완료)
DIRECT_CREDENTIALS = {
  "type": "service_account",
  "project_id": "empire-server-482109",
  "private_key_id": "dcbc5b2cfcbedffcc88854eca40a8d00166a709e",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC40jqgROsr15F1\nsqQyHpjwehAwzrnW96pGi4RRLlBdAB2luR6IM6VXqqd9XzJ/JBbUW1lwYiGukF88\n+6vvIXlA/zy5s2HSa9PqYnlVHnRTG2qZ07fBVA0JvwHF3B7Tb5aDs9O2+gYSXIaL\nn9NUQ94Oqx2zAqFPOuilRkLUZnfOW5WZAJ75+H01J/nXthh/K/RII2psKPgtRrRd\nKl8T+HovAgTuJGcAt77uVsKh4KapSIz7JazETnjfUqrhiYZdujnyop0mIFi+VmQR\nUPxgTmlFcxLYGKdw2Cik7vOXAe9fiss2hiReOLKsnzOe8Erco4KQv+ieI+nF2JOm\ndoeJBQfNAgMBAAECgf9WExASgAZbt46G9D+7Sf0shE0pLK9lKnBSb3VVwuk2/WXg\nYaRbkyVhVt9FyAOb2HuwR4WPkqa4skzpxwCZur5wAdgvlQN+4HewpGMt2kTaH1GD\ntvrVyxB5miNy9fBgRuqsmMqVFQ5QasH4Nb/ZNep4vTOhvwjFj6xgM3RQvXwih5WA\ngDTgfdaMF4U4l8LSo1/SOhp/1knlbJ0a4Mxxp53uhGu0KF9zQCtFJFKtdK9MP+xt\nXvs/wJM8Rl02WmVBkafJazDCBYQiSDKXn9TP8pZVdx6rxddHRG/Ni0xqhJory2NO\nlNHPBAGErIE8Mj0bQA4ZFtBjfiu37DMErbnfNssCgYEA9Mm4FZ/HebXtYZGyX8Oo\nQ+rmDeU8Jl7XQKAtMDwDdqSb3xQ5KXICDcnUuVpzc+VlJAXG8JWr6GEMtrzkZBYR\nhzxzaOSYNYN9Kl0WgVG23rp3hDskAhKdY/chnoEmTJHHvkcOxZQMQh5gKgV6iKwc\njkBG49aIuB8X/3L2DL7TifcCgYEAwUldadP3cODdswfnIX6ZWRx67babTXFVSSGs\nBzqYs4JHqtF/odgEdgd8HFFafCa8mdZyANhUbA88b3c+2PpJ+DHTWECymSj3tgF9\nFNf0l2Pcy8SdUoHN/97e5xMYKPnquyqZ5U9GPwjGxCiTCItX8A0YT26N+QNH5v6x\n2Hvtq1sCgYA11n/kUaX/wOGayf6fTVsexPUgLUDTd5yEHDaUGz7vwzh9EeeYk/ib\nq75bnecyoEtkZtjgZSrQCzhOoLDiym/EfKktcsl/S5Il1R90BdLgncZXkOJUil+P\ncvUz9VfFE3MJCHvZPLyNdjzUQSw4DxKgvsZYqgCb7krK5i/zkazY9QKBgQCesc2w\nhggy9W0RAPwT1A2zzF5hrfv0qYiMcsj7ZnDZca3F4hwYlXOUNLEBzwmrxWI0LI2N\nhBBMaHYGTrGbFGSHEuGjI/t/JNO865v28Rgw9BzkcJl6lHi+DA6XSmYbvpWq9l9E\nlsHmHx6TD30pFr8sqJO9I9gNC1SNo7ABPj704QKBgQDjvZzxNXewP+m1je6DT/Vo\nd97WG4MLhsKNg4TdVEvUB96/NUKLkmSQLRtJ/ecnR9EktjjBM9AI1KTJWm7v/74S\n3aSZquq0x59kCx/L/uj/aMF8yDklbaJhPldfCiijrRVgRz37hSJGWdJksoYVtPpv\n8uhqEcpxEYdoYjHMZABmRA==\n-----END PRIVATE KEY-----\n",
  "client_email": "empire-bot@empire-server-482109.iam.gserviceaccount.com",
  "client_id": "100974861333781341058",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/empire-bot%40empire-server-482109.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# ======================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEASE_SECONDS = 300
JOBS_QUEUE = []
INFLIGHT = {}

def now_ts(): return int(time.time())

# [핵심] 환경변수 무시하고 코드 내 키 사용
def _get_service():
    if not DIRECT_SHEET_ID:
        return None, "ERROR: DIRECT_SHEET_ID is empty in app.py code."

    try:
        creds = Credentials.from_service_account_info(DIRECT_CREDENTIALS, scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return svc, f"OK (Hardcoded). Email: {DIRECT_CREDENTIALS.get('client_email')}"
    except Exception as e:
        return None, f"Auth Error: {str(e)}"

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

@app.route("/")
def home():
    return f"Empire Brain Online (Hardcoded). Queue={len(JOBS_QUEUE)}"

@app.route("/debug/google")
def debug_google():
    svc, msg = _get_service()
    return jsonify({
        "ok": svc is not None,
        "message": msg,
        "target_sheet_id": DIRECT_SHEET_ID,
        "target_tab": DIRECT_SHEET_TAB
    })

@app.route("/jobs/push-from-sheet", methods=["GET", "POST"])
def push_from_sheet():
    svc, msg = _get_service()
    if not svc:
        return jsonify({"ok": False, "error": msg}), 500

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
            job = {
                "id": str(uuid.uuid4()), "sheet_row": i, "sheet_status_col": status_idx,
                "header": header, "data": row
            }
            JOBS_QUEUE.append(job)
            _update_status(svc, i, status_idx, "QUEUED")
            pushed += 1
            pushed_jobs.append(job["id"])

    return jsonify({"ok": True, "pushed": pushed, "queue_len": len(JOBS_QUEUE)})

# (Lease, Ack, Fail, Cookies 라우트는 기존 로직과 동일 - 생략 없이 그대로 유지)
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
