# app.py (추가/교체용 코드)
import time
import uuid
from flask import Flask, request, jsonify

app = Flask(__name__)

# -------------------------
# In-memory stores
# -------------------------
JOBS_QUEUE = []            # READY jobs (list of dict)
INFLIGHT = {}              # lease_id -> {"job": job, "expires_at": ts}
DONE_LOG = []              # optional: completed job ids

LEASE_SECONDS_DEFAULT = 300  # 5분 (필요하면 120~600 사이로 조절)

def now_ts():
    return int(time.time())

def normalize_market(m):
    return (m or "US").upper()

def requeue_expired_internal():
    """만료된 임대 작업을 READY로 복귀"""
    t = now_ts()
    expired = [lease_id for lease_id, rec in INFLIGHT.items() if rec.get("expires_at", 0) <= t]
    count = 0
    for lease_id in expired:
        rec = INFLIGHT.pop(lease_id, None)
        if rec and rec.get("job"):
            JOBS_QUEUE.append(rec["job"])
            count += 1
    return count

def pop_ready_job_by_market(market: str):
    """READY에서 market에 맞는 1개를 뽑되 '삭제'는 하지 않고, 밖에서 옮기게 함"""
    for i, job in enumerate(JOBS_QUEUE):
        if normalize_market(job.get("market")) == normalize_market(market):
            return i, job
    return None, None

# -------------------------
# Health / Debug
# -------------------------
@app.route("/jobs/stats", methods=["GET"])
def jobs_stats():
    requeued = requeue_expired_internal()
    return jsonify({
        "ok": True,
        "queue_ready": len(JOBS_QUEUE),
        "queue_inflight": len(INFLIGHT),
        "requeued_now": requeued,
        "done_count": len(DONE_LOG),
    })

# -------------------------
# PUSH (이미 만들어둔 것 사용 가능)
# -------------------------
@app.route("/jobs/push", methods=["POST"])
def jobs_push():
    data = request.json or {}
    job = data.get("job")
    if not isinstance(job, dict):
        return jsonify({"ok": False, "error": "job must be dict"}), 400
    # 최소 필드 보정
    job.setdefault("id", str(int(time.time() * 1000)))
    job.setdefault("market", "US")
    JOBS_QUEUE.append(job)
    return jsonify({"ok": True, "queued": len(JOBS_QUEUE)})

# -------------------------
# LEASE (증발 방지 핵심)
# -------------------------
@app.route("/jobs/lease", methods=["GET"])
def jobs_lease():
    market = normalize_market(request.args.get("market", "US"))
    lease_seconds = int(request.args.get("lease_seconds", LEASE_SECONDS_DEFAULT))

    # 만료 회수 먼저
    requeued = requeue_expired_internal()

    idx, job = pop_ready_job_by_market(market)
    if job is None:
        return jsonify({
            "ok": True,
            "job": None,
            "lease_id": None,
            "queue_ready": len(JOBS_QUEUE),
            "queue_inflight": len(INFLIGHT),
            "requeued_now": requeued
        })

    # READY에서 제거하고 INFLIGHT로 이동
    JOBS_QUEUE.pop(idx)
    lease_id = str(uuid.uuid4())
    INFLIGHT[lease_id] = {
        "job": job,
        "expires_at": now_ts() + max(30, lease_seconds)  # 최소 30초 보장
    }

    return jsonify({
        "ok": True,
        "job": job,
        "lease_id": lease_id,
        "expires_at": INFLIGHT[lease_id]["expires_at"],
        "queue_ready": len(JOBS_QUEUE),
        "queue_inflight": len(INFLIGHT),
        "requeued_now": requeued
    })

# -------------------------
# ACK (작업 확정)
# -------------------------
@app.route("/jobs/ack", methods=["POST"])
def jobs_ack():
    data = request.json or {}
    lease_id = data.get("lease_id")
    status = (data.get("status") or "DONE").upper()

    if not lease_id or lease_id not in INFLIGHT:
        return jsonify({"ok": False, "error": "invalid lease_id"}), 400

    rec = INFLIGHT.pop(lease_id)
    job = rec.get("job") or {}
    DONE_LOG.append({
        "job_id": job.get("id"),
        "status": status,
        "at": now_ts()
    })
    return jsonify({"ok": True, "job_id": job.get("id"), "status": status})

# -------------------------
# FAIL/REQUEUE (수동 회수)
# -------------------------
@app.route("/jobs/fail", methods=["POST"])
def jobs_fail():
    data = request.json or {}
    lease_id = data.get("lease_id")
    reason = data.get("reason", "unknown")

    if not lease_id or lease_id not in INFLIGHT:
        return jsonify({"ok": False, "error": "invalid lease_id"}), 400

    rec = INFLIGHT.pop(lease_id)
    job = rec.get("job") or {}
    job["last_fail_reason"] = reason
    job["last_fail_at"] = now_ts()
    JOBS_QUEUE.append(job)
    return jsonify({"ok": True, "requeued": True, "job_id": job.get("id")})

# -------------------------
# OPTIONAL: 기존 next 유지(레거시 호환)
# - 이제는 "lease"로 내부 위임(증발 방지)
# -------------------------
@app.route("/jobs/next", methods=["GET"])
def jobs_next_legacy():
    # 레거시 호출을 lease로 돌려서 '증발' 없게 만들기
    market = request.args.get("market", "US")
    return jobs_lease()
