import argparse
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from googleapiclient.errors import HttpError
import requests, base64
from clients.google import GsheetAPI
from collections import defaultdict
from datetime import datetime
import json
import time

# === CONFIG ===
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
RANGE_NAME = "Input!A:E"
RESULT_SHEET = "Result Log"

# API info
DATAHUB_API = ''


valid_email_domains = ["", ""]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["env_check", "check", "update"],
        required=True,
        help="env_check | check | update"
    )
    parser.add_argument(
        "--sheet-id",
        required=True,
        help="Google Spreadsheet ID"
    )
    parser.add_argument(
        "--personal-token",
        required=False,
        help="Data Service personal token (required for update mode)"
    )
    parser.add_argument(
        '--app-key',
        type=str,
        required=False,
        help="App Key"
    )
    parser.add_argument(
        '---app-secret',
        type=str,
        required=False,
        help="App Secret"
    )
    return parser.parse_args()


def validate_spreadsheet_access(service, spreadsheet_id):
    try:
        service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        print("✅ Spreadsheet access OK")

    except HttpError as e:
        if e.resp.status == 404:
            print("❌ Spreadsheet not found. Check Spreadsheet ID.")
        elif e.resp.status == 403:
            print("❌ No permission. Please add service account as editor.")
        else:
            print(f"❌ Spreadsheet access error: {e}")
        sys.exit(1)

def validate_input_sheet(service, sheet_id, input_sheet_name="Input"):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{input_sheet_name}!A1:E1"
        ).execute()

        headers = result.get("values", [])

        if not headers:
            raise ValueError("Input sheet exists but header row is empty")

        expected_headers = ["task_id (required)", "task_name (optional)", "owner (required)", "editors (optional)", "delete_existing_editors (required)"]
        actual_headers = [h.strip().lower() for h in headers[0]]

        if actual_headers != expected_headers:
            raise ValueError(
                f"Invalid header. Expected {expected_headers}, got {actual_headers}"
            )

        print("✅ Input sheet validation passed")

    except Exception as e:
        print(f"❌ Input sheet validation failed: {e}")
        print(f"👉 Please ensure sheet '{input_sheet_name}' exists and follows the template.")
        sys.exit(1)

# === GOOGLE SHEET SETUP ===

def get_gsheet_list(service, sheet_id):
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=RANGE_NAME).execute()
    values = result.get("values", [])

    if not values or len(values) <= 1:
        return []
    # Skip header
    return [tuple(row + ["", "", ""][:5 - len(row)]) for row in values[1:]]

def append_to_sheet(service, sheet_id, sheet_name, rows):
    body = {"values": rows}
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{sheet_name}!A:Z",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

def ensure_sheet_exists(service, sheet_id, sheet_name):
    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_titles = [s["properties"]["title"] for s in sheet_metadata["sheets"]]
    if sheet_name not in sheet_titles:
        requests_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body=requests_body
        ).execute()
        # Add header row
        header = [["timestamp", "task_ids", "task_name", "owner", "editors", "delete_existing_editors", "status", "message"]]
        append_to_sheet(service, sheet_id, sheet_name, header)

# === MAIN LOGIC ===
def group_tasks(task_list):
    """Group valid rows; mark invalid ones."""
    grouped = defaultdict(list)
    logs = []

    for i, row in enumerate(task_list, start=2):
        try:
            task_id, task_name, owner, editors, delete_existing_editors = [x.strip() for x in row]
            if not task_id or not owner or not delete_existing_editors:
                raise ValueError("Missing task_id or owner or delete_existing_editors")
            if delete_existing_editors.upper() not in ["TRUE", "FALSE"]:
                raise ValueError("delete_existing_editors must be TRUE or FALSE")

            key = (owner, editors, delete_existing_editors.upper())
            grouped[key].append((task_id, task_name))
        except Exception as e:
            # Safely extract values if available
            task_id = row[0].strip() if len(row) > 0 else ""
            task_name = row[1].strip() if len(row) > 1 else ""
            owner = row[2].strip() if len(row) > 2 else ""
            editors = row[3].strip() if len(row) > 3 else ""
            delete_existing_editors = row[4].strip() if len(row) > 4 else ""

            logs.append([
                "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                task_id,
                task_name,
                owner,
                editors,
                delete_existing_editors,
                "INVALID_INPUT",
                f"Row {i}: {str(e)}"
        ])

    return grouped, logs

def validate_grouped_tasks(grouped_tasks):
    valid_groups = {}
    logs = []

    for (owner, editors, delete_existing_editors), task_info in grouped_tasks.items():
        # Check owner email
        if not any(owner.endswith(domain) for domain in valid_email_domains):
            for task_id, task_name in task_info:
                logs.append([
                    "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    task_id,
                    task_name,
                    owner,
                    editors,
                    delete_existing_editors,
                    "INVALID_INPUT",
                    f"Owner email '{owner}' is not valid"
                ])
            continue  # skip API call

        # Check editors emails
        editors_list = [x.strip() for x in editors.split(",") if x.strip()]
        invalid_editors = [e for e in editors_list if not any(e.endswith(domain) for domain in valid_email_domains)]
        if invalid_editors:
            for task_id, task_name in task_info:
                logs.append([
                    "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    task_id,
                    task_name,
                    owner,
                    editors,
                    delete_existing_editors,
                    "INVALID_INPUT",
                    f"Editor emails not valid: {', '.join(invalid_editors)}"
                ])
            continue  # skip API call
        
        # ✅ Passed validation
        valid_groups[(owner, editors, delete_existing_editors)] = task_info

    return valid_groups, logs


def update_props_grouped(service, sheet_id, personal_token, grouped_tasks, logs, app_key, app_secret):
    api_failed = False
    def chunked(lst, size=20):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    auth_string = f"{app_key}:{app_secret}".encode("utf-8")
    encoded_auth_token = base64.b64encode(auth_string).decode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_auth_token}",
        "Personal-Token": personal_token,
    }

    for (owner, editors, delete_existing_editors), task_info in grouped_tasks.items():
        # task_info = [(task_id, task_name), ...]
        task_pairs = [(str(tid), tname) for tid, tname in task_info]

        for chunk in chunked(task_pairs, 20):
            task_ids = [tid for tid, _ in chunk]

            body = {
                "taskOwnerEditorsUpdateInfos": [
                    {
                        "taskIds": task_ids,
                        "owner": owner,
                        "editors": [x.strip() for x in editors.split(",") if x.strip()],
                        "deleteExistingEditorsFlag": delete_existing_editors == "TRUE",
                    }
                ]
            }

            try:
                response = requests.post(DATAHUB_API, json=body, headers=headers)
                print(f"=== API RESPONSE for owner: {owner} ===")
                print(response.text)

                timestamp = "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if response.status_code == 200:
                    try:
                        data = response.json()

                        if isinstance(data, list):
                            # Map response by taskId
                            resp_map = {str(item.get("taskId")): item for item in data}

                            for task_id, task_name in chunk:
                                item = resp_map.get(task_id, {})
                                success = item.get("success", False)
                                error_msg = item.get("errorMsg", "")
                                status = "SUCCESS" if success else "FAILED"

                                if not success:
                                    api_failed = True

                                logs.append([
                                    timestamp,
                                    task_id,
                                    task_name,
                                    owner,
                                    editors,
                                    delete_existing_editors,
                                    status,
                                    error_msg or json.dumps(item)
                                ])
                        else:
                            # Unexpected JSON structure
                            api_failed = True
                            for task_id, task_name in chunk:
                                logs.append([
                                    timestamp,
                                    task_id,
                                    task_name,
                                    owner,
                                    editors,
                                    delete_existing_editors,
                                    "FAILED",
                                    json.dumps(data)
                                ])

                    except Exception as parse_error:
                        api_failed = True
                        for task_id, task_name in chunk:
                            logs.append([
                                timestamp,
                                task_id,
                                task_name,
                                owner,
                                editors,
                                delete_existing_editors,
                                "ERROR",
                                f"JSON parse error: {parse_error}"
                            ])
                else:
                    # HTTP error (e.g. 400 / 504)
                    api_failed = True
                    for task_id, task_name in chunk:
                        logs.append([
                            timestamp,
                            task_id,
                            task_name,
                            owner,
                            editors,
                            delete_existing_editors,
                            "FAILED",
                            response.text[:300]
                        ])

            except Exception as e:
                api_failed = True
                for task_id, task_name in chunk:
                    logs.append([
                        "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        task_id,
                        task_name,
                        owner,
                        editors,
                        delete_existing_editors,
                        "ERROR",
                        str(e)
                    ])

            # Avoid API overload
            time.sleep(0.3)
    return api_failed

# === MAIN ===
if __name__ == "__main__":
    args = parse_args()
    
    SHEET_ID = args.sheet_id
    PERSONAL_TOKEN = args.personal_token or os.getenv("PERSONAL_TOKEN")
    MODE = args.mode
    APP_KEY = args.access_update_app_key
    APP_SECRET = args.access_update_app_secret

    service = GsheetAPI.get_service()

    # === 1. Spreadsheet access check (fatal if fails) ===
    validate_spreadsheet_access(service, SHEET_ID)
    # === 2. Validate Input sheet (user responsibility) ===
    validate_input_sheet(service, SHEET_ID, input_sheet_name="Input")
    # === 3. Ensure Result Log sheet exists (tool responsibility) ===
    ensure_sheet_exists(service, SHEET_ID, RESULT_SHEET)

    if MODE == "env_check":
        print("✅ ENV CHECK PASSED")
        print(" - Spreadsheet accessible")
        print(" - Input sheet exists")
        print(" - Result Log ready")
        exit(0)

    # === 4. Read input data ===
    task_list = get_gsheet_list(service, SHEET_ID)
    if not task_list:
        print("❌ Input sheet has no data rows")
        sys.exit(1)

    logs = []

    # === 5. Row-level validation + grouping ===
    grouped_tasks, row_logs = group_tasks(task_list)
    logs.extend(row_logs)

    # === 6. Group-level validation (email checks) ===
    valid_groups, group_logs = validate_grouped_tasks(grouped_tasks)
    logs.extend(group_logs)

    if MODE == "check":
        has_invalid = any(log[6] == "INVALID_INPUT" for log in logs)

        if logs:
            append_to_sheet(service, SHEET_ID, RESULT_SHEET, logs)
            for log_row in logs:
                print(log_row)
        
        if has_invalid:
            print("❌ CHECK failed — invalid input detected")
            sys.exit(1)

        print("CHECK completed — no API calls executed")
        exit(0)

    if MODE == "update":
        personal_token = os.getenv("PERSONAL_TOKEN")
        if not personal_token:
            print("❌ PERSONAL_TOKEN is required for update mode")
            exit(1)
        
        api_failed = False

        if valid_groups:
            api_failed = update_props_grouped(
                service,
                SHEET_ID,
                PERSONAL_TOKEN,
                valid_groups,
                logs, 
                APP_KEY,
                APP_SECRET
            )

        # Write ALL logs once
        if logs:
            append_to_sheet(service, SHEET_ID, RESULT_SHEET, logs)
            for row in logs:
                print(row)
        
        # ❌ Fail Jenkins if any API failed
        if api_failed:
            print("❌ UPDATE completed with failures — check Result Log")
            sys.exit(1)

        print("✅ UPDATE completed")
        sys.exit(0)