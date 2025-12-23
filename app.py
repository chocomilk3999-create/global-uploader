import os
from flask import Flask, request, jsonify

app = Flask(__name__)

# 데이터 저장소 (메모리)
JOB_QUEUE = []
JOB_RESULTS = {}
COOKIES_STORE = {}

@app.route('/')
def home():
    return f"Use /upload-global to enqueue jobs. Current Queue: {len(JOB_QUEUE)}"

# 1. Make에서 일감 던지는 곳
@app.route('/upload-global', methods=['POST'])
def enqueue_job():
    data = request.json
    job_id = data.get("id") or str(len(JOB_QUEUE) + 1)
    
    job = {
        "job_id": job_id,
        "market_title": data.get("title"),
        "market_description_html": data.get("description_html"),
        "photo_urls": data.get("images", []),
        "price_usd": data.get("price_usd"),
        "status": "QUEUED"
    }
    JOB_QUEUE.append(job)
    print(f"📥 [Job Queued] ID: {job_id}")
    return jsonify({"status": "QUEUED", "job_id": job_id})

# 2. 에이전트가 일감 가져가는 곳
@app.route('/queue/next', methods=['GET'])
def get_next_job():
    market = request.args.get('market')
    if not JOB_QUEUE:
        return '', 204
    job = JOB_QUEUE.pop(0)
    return jsonify(job)

# 3. 결과 보고
@app.route('/queue/report', methods=['POST'])
def report_job():
    data = request.json
    job_id = data.get("job_id")
    print(f"✅ [Report] {job_id} : {data.get('status')}")
    JOB_RESULTS[job_id] = data
    return jsonify({"status": "OK"})

# 4. 쿠키 저장 (POST)
@app.route('/cookies', methods=['POST'])
def save_cookies():
    data = request.json
    # B안 포맷 그대로 저장
    COOKIES_STORE['default'] = data
    return jsonify({"status": "saved", "keys": list(data.keys())})

# 5. 쿠키 제공 (GET) - 🔴 이게 없어서 404가 떴던 겁니다!
@app.route('/cookies', methods=['GET'])
def get_cookies():
    data = COOKIES_STORE.get('default', {})
    # 없으면 빈 껍데기라도 줘서 에러 방지
    if not data:
        return jsonify({"cookies": [], "origins": []})
    return jsonify(data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
