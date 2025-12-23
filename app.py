import os
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# 데이터 저장소 (메모리)
JOB_QUEUE = []        # 대기 중인 일감
JOB_RESULTS = {}      # 완료된 일감 결과
COOKIES_STORE = {}    # 쿠키 저장소

@app.route('/')
def home():
    return f"Empire Server Running. Jobs in Queue: {len(JOB_QUEUE)}"

# ==========================================
# ✅ [NEW] 신형 에이전트용 (/jobs)
# ==========================================

# 1. Make에서 일감 던지기 (POST /jobs/add)
@app.route('/jobs/add', methods=['POST'])
def add_job():
    data = request.json
    job_id = data.get("id") or str(int(time.time()))
    
    job = {
        "id": job_id,
        "title": data.get("title"),
        "description_html": data.get("description_html"),
        "photos": data.get("images", []),
        "price": data.get("price_usd"),
        "qty": data.get("qty", 1),
        "status": "QUEUED",
        "created_at": time.time()
    }
    JOB_QUEUE.append(job)
    print(f"📥 [New Job] ID: {job_id}")
    return jsonify({"ok": True, "job_id": job_id})

# 2. 에이전트가 일감 가져가기 (GET /jobs/next)
@app.route('/jobs/next', methods=['GET'])
def get_next_job():
    if not JOB_QUEUE:
        return jsonify({"ok": True, "job": None})
    
    # FIFO: 가장 먼저 들어온 일감 꺼내기
    job = JOB_QUEUE.pop(0)
    return jsonify({"ok": True, "job": job})

# 3. 에이전트가 결과 보고하기 (POST /jobs/report)
@app.route('/jobs/report', methods=['POST'])
def report_job():
    data = request.json
    job_id = data.get("job_id")
    status = data.get("status")
    
    print(f"✅ [Report] {job_id} : {status}")
    JOB_RESULTS[job_id] = data
    return jsonify({"ok": True})

# ==========================================
# ⚠️ [LEGACY] 구형 에이전트 호환용 (유지)
# ==========================================
@app.route('/upload-global', methods=['POST'])
def legacy_enqueue():
    return add_job() # 신형 로직으로 토스

@app.route('/queue/next', methods=['GET'])
def legacy_next():
    if not JOB_QUEUE: return '', 204
    job = JOB_QUEUE.pop(0)
    # 구형 포맷으로 변환
    return jsonify({
        "job_id": job["id"],
        "market_title": job["title"],
        "market_description_html": job["description_html"],
        "price_usd": job["price"]
    })

@app.route('/queue/report', methods=['POST'])
def legacy_report():
    return report_job()

@app.route('/cookies', methods=['POST'])
def save_cookies():
    data = request.json
    COOKIES_STORE['default'] = data
    return jsonify({"status": "saved"})

@app.route('/cookies', methods=['GET'])
def get_cookies():
    return jsonify(COOKIES_STORE.get('default', {"cookies": [], "origins": []}))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
