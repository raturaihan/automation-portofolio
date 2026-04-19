import requests
import time
import json
import pandas as pd 
from datetime import date
import pygsheets
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
# 1️⃣ TOKEN (once)
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
# 2️⃣ TRIGGER JOB (once)
# --------------------------------------------------
def create_job(token, start_date, end_date):
    url = f"{BASE_URL}/dataservice/{API_PATH}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    body = {
        "olapPayload": {
            "expressions": [
                {
                    "parameterName" : "start_date",
                    "value": start_date
                },
                {
                    "parameterName" : "end_date",
                    "value": end_date
                }
            ]
        }
    }

    resp = requests.post(url, json=body, headers=headers, timeout=TIMEOUT)

    job_id = safe_json(resp)["jobId"]
    print(f"Job created: {job_id}")

    return job_id


# --------------------------------------------------
# 3️⃣ POLL METADATA
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
# 4️⃣ FETCH SHARDS
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
# 5️⃣ MAIN ENTRY
# --------------------------------------------------
def get_job(start_date, end_date):
    token = get_token()                 # ✅ once
    job_id = create_job(token, start_date, end_date)          # ✅ once

    meta = wait_until_finish(job_id, token)

    max_shard = meta["maxShard"]
    print(f"Total shards: {max_shard + 1}")

    return fetch_all_shards(job_id, token, max_shard)


# --------------------------------------------------
# 6️⃣ RETURN PARSED DATA
# --------------------------------------------------
def get_api_response(start_date, end_date):
    """
    Returns RAW Datasuite response (JSON string)
    Exactly same structure as original OLAP API

    Compatible with:
        response_data = json.loads(get_api_response())
        rows = response_data.get("rows", [])
    """

    shards = get_job(start_date, end_date)
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


def get_last_updated_date():
    return date.today().strftime("%Y-%m-%d")

def process_api_response(start_date, end_date):
    api_response = get_api_response(start_date, end_date)
    response_data = json.loads(api_response)
    rows = response_data.get("rows", [])
    values_to_write = []
    for row in rows:
        values = row.get("values", {})
        values_to_write.append([
            values.get("idc_region"),
            values.get("table_name"),
            values.get("month"),
            values.get("dqc_fail"),
        ])

    df = pd.DataFrame(values_to_write, columns=[
        "idc_region", "table_name", "month", "dqc_fail"
    ])

    df.sort_values(by=['dqc_fail'], ascending=[False], inplace=True)

    return df

if __name__ == '__main__':
    # Get today's date
    today = date.today()

    # Get the last day of the previous month (as end_date)
    end_date = date(today.year, today.month, 1) - relativedelta(days=1)

    # Get the last day of two months ago (as start_date)
    start_date = date(end_date.year, end_date.month, 1) - relativedelta(days=1)

    # Format as YYYY-MM-DD
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    data = process_api_response(start_date_str, end_date_str)
    
    gc = pygsheets.authorize(service_file="")
    sh = gc.open('')

    sheet_name = ''
    worksheet = sh.worksheet_by_title(sheet_name)

    # Clear existing content and write the dataframe to the worksheet
    worksheet.clear(start='A2', end='D')
    worksheet.set_dataframe(data, start='A2')

    # Add 'business_line' formula in column E
    worksheet.update_value('E2', 'business_line')  # Header for column S

    business_line_formula_range = f'E3:E{len(data) + 2}'
    worksheet.update_values(
        business_line_formula_range,
        [[f"=VLOOKUP(B{r}, 'Raw'!B:D, 3, FALSE)"] for r in range(3, len(data) + 3)]
    )

    worksheet.update_value('A1', f'last updated: {get_last_updated_date()}')