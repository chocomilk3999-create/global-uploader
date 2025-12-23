import os
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# 작업 대기열 (메모리에 임시 저장)
JOB_QUEUE = []
# 작업 결과 저장소
JOB_RESULTS = {}
# 쿠키 저장소
COOKIES_STORE = {}

@app.route('/')
def home():
    return f"Use /upload-global to enqueue jobs. Current Queue: {len(JOB_QUEUE)}"

# 1. Make에서 일감 던지는 곳 (기존 주소 유지)
@app.route('/upload-global', methods=['POST'])
def enqueue_job():
    data = request.json
    job_id = data.get("id") or str(len(JOB_QUEUE) + 1)
    
    # 큐에 작업 추가
    job = {
        "job_id": job_id,
        "market_title": data.get("title"),
        "market_description_html": data.get("description_html"),
        "photo_urls": data.get("images", []),
        "price_usd": data.get("price_usd"),
        "status": "QUEUED"
    }
    JOB_QUEUE.append(job)
    print(f"[Job Queued] ID: {job_id}")
    return jsonify({"status": "QUEUED", "message": "Job added to queue", "job_id": job_id})

# 2. 로컬 에이전트가 "일감 줘!" 하는 곳
@app.route('/queue/next', methods=['GET'])
def get_next_job():
    if not JOB_QUEUE:
        return '', 204  # 일감 없음 (No Content)
    
    # 가장 오래된 작업 하나 꺼내주기 (FIFO)
    job = JOB_QUEUE.pop(0)
    return jsonify(job)

# 3. 로컬 에이전트가 "다 했어!" 보고하는 곳
@app.route('/queue/report', methods=['POST'])
def report_job():
    data = request.json
    job_id = data.get("job_id")
    status = data.get("status")
    
    print(f"[Job Done] ID: {job_id} Status: {status}")
    
    # 결과 저장 (나중에 Make가 조회할 수 있게)
    JOB_RESULTS[job_id] = data
    return jsonify({"status": "OK"})

# 4. 쿠키 저장 (기존 기능 유지)
@app.route('/cookies', methods=['POST'])
def save_cookies():
    data = request.json
    COOKIES_STORE['default'] = data.get('cookies')
    return jsonify({"status": "saved", "count": len(data.get('cookies', []))})

# 5. 쿠키 제공 (로컬 에이전트용)
@app.route('/cookies', methods=['GET'])
def get_cookies():
    cookies = COOKIES_STORE.get('default', [])
    if not cookies:
        return jsonify({"error": "No cookies found"}), 404
    return jsonify({"cookies": cookies})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
