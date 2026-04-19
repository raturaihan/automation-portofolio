import requests
import time
import json
from datetime import date
from dateutil.relativedelta import relativedelta

BASE_URL = ""
API_PATH = ""
CLIENT_ID = ""
CLIENT_SECRET = ""

POLL_INTERVAL = 15     # seconds
TIMEOUT = 30           # request timeout
MAX_WAIT = 1800        # 30 minutes safety timeout
# --------------------------------------------------


def safe_json(resp):
    """Safely parse JSON with good error message"""
    resp.raise_for_status()

    if not resp.text.strip():
        raise Exception("Empty API response")

    return resp.json()


# --------------------------------------------------
# TOKEN (once)
# --------------------------------------------------
def get_token():
    url = f"{BASE_URL}/oauth/token"


    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT)
    return safe_json(resp)["access_token"]


# --------------------------------------------------
# TRIGGER JOB (once)
# --------------------------------------------------
def create_job(token, validation_date):
    url = f"{BASE_URL}/dataservice/{API_PATH}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    body = {
        "olapPayload": {
            "expressions": [
                {
                    "parameterName": "validation_date", 
                    "value": validation_date
                }
            ]
        }
    }

    resp = requests.post(url, json=body, headers=headers, timeout=TIMEOUT)

    job_id = safe_json(resp)["jobId"]
    print(f"Job created: {job_id}")

    return job_id


# --------------------------------------------------
# POLL METADATA
# --------------------------------------------------
def wait_until_finish(job_id, token):
    url = f"{BASE_URL}/dataservice/result/{job_id}"
    headers = {"Authorization": f"Bearer {token}"}

    start = time.time()

    while True:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        meta = safe_json(resp)

        status = meta["status"]
        print(f"Job status: {status}")

        if status == "FINISH":
            return meta

        if status == "FAILED":
            raise Exception(meta.get("message", "Job failed"))

        if time.time() - start > MAX_WAIT:
            raise TimeoutError("Job polling timeout")

        time.sleep(POLL_INTERVAL)


# --------------------------------------------------
# FETCH SHARDS
# --------------------------------------------------
def fetch_all_shards(job_id, token, max_shard):
    headers = {"Authorization": f"Bearer {token}"}

    results = []

    for shard in range(max_shard + 1):
        print(f"Fetching shard {shard}/{max_shard}")

        url = f"{BASE_URL}/dataservice/result/{job_id}/{shard}"
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)

        results.append(safe_json(resp))

    return results


# --------------------------------------------------
# MAIN ENTRY
# --------------------------------------------------
def get_job(validation_date):
    token = get_token()               
    job_id = create_job(token, validation_date)          

    meta = wait_until_finish(job_id, token)

    max_shard = meta["maxShard"]
    print(f"Total shards: {max_shard + 1}")

    return fetch_all_shards(job_id, token, max_shard)


# --------------------------------------------------
# 6️⃣ RETURN PARSED DATA
# --------------------------------------------------
def get_api_response(validation_date):
    """
    Returns RAW Datasuite response (JSON string)
    Exactly same structure as original OLAP API

    Compatible with:
        response_data = json.loads(get_api_response())
        rows = response_data.get("rows", [])
    """

    shards = get_job(validation_date)
    print(f"Total shards fetched: {len(shards)}")

    if not shards:
        return json.dumps({"rows": []})

    # Use first shard metadata
    first = shards[0]

    merged = {
        "pageSize": 0,
        "engine": first.get("engine"),
        "contentType": first.get("contentType"),
        "resultSchema": first.get("resultSchema"),
        "rows": []
    }

    # merge all shard rows
    for shard in shards:
        shard_rows = shard.get("rows", [])
        merged["rows"].extend(shard_rows)

    merged["pageSize"] = len(merged["rows"])

    return json.dumps(merged)


# --------------------------------------------------
if __name__ == "__main__":
    print(f"data: {json.loads(get_api_response('2026-01-28'))}")