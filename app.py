import os
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# Job queue (in-memory temporary storage)
JOB_QUEUE = []
# Job results storage
JOB_RESULTS = {}
# Cookies storage
COOKIES_STORE = {}

@app.route('/')
def home():
    return f"Use /upload-global to enqueue jobs. Current Queue: {len(JOB_QUEUE)}"

# 1. Endpoint for Make to enqueue jobs
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
    return jsonify({"status": "QUEUED", "message": "Job added to queue", "job_id": job_id})

# 2. Endpoint for local agent to get next job
@app.route('/queue/next', methods=['GET'])
def get_next_job():
    if not JOB_QUEUE:
        return '', 204  # No content if queue is empty
    
    job = JOB_QUEUE.pop(0)  # FIFO
    return jsonify(job)

# 3. Endpoint for local agent to report job completion
@app.route('/queue/report', methods=['POST'])
def report_job():
    data = request.json
    job_id = data.get("job_id")
    
    JOB_RESULTS[job_id] = data
    return jsonify({"status": "OK"})

# 4. Save cookies
@app.route('/cookies', methods=['POST'])
def save_cookies():
    data = request.json
    COOKIES_STORE['default'] = data.get('cookies')
    return jsonify({"status": "saved", "count": len(data.get('cookies', []))})

# 5. Get cookies
@app.route('/cookies', methods=['GET'])
def get_cookies():
    cookies = COOKIES_STORE.get('default', [])
    if not cookies:
        return jsonify({"error": "No cookies found"}), 404
    return jsonify({"cookies": cookies})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
