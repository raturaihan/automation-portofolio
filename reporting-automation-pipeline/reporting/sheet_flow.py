import pygsheets
import requests
from datetime import datetime, timedelta, date

def duplicate_previous_month_sheet(additional_sheet_name):
    """Duplicate an existing sheet with formatting and return the new worksheet."""
    existing_sheet_name, sheet_name = get_dynamic_sheet_names(additional_sheet_name)
    
    gc = pygsheets.authorize(service_file="")
    sh = gc.open('')

    try:
        # Get the sheet ID
        existing_sheet = sh.worksheet_by_title(existing_sheet_name)
        sheet_id = existing_sheet.id  # Get the sheet's numeric ID
    except pygsheets.WorksheetNotFound:
        raise ValueError(f"❌ Existing sheet '{existing_sheet_name}' not found.")

    # Get Spreadsheet ID
    spreadsheet_id = sh.id  

    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }

    # Duplicate Sheet Request
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate"
    body = {
        "requests": [
            {
                "duplicateSheet": {
                    "sourceSheetId": sheet_id,
                    "newSheetName": sheet_name
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 200:
        print(f"✅ Successfully duplicated '{existing_sheet_name}' as '{sheet_name}'!")
        return sh.worksheet_by_title(sheet_name)  # Return the new worksheet
    else:
        raise ValueError(f"❌ Error duplicating sheet: {response.text}")

def get_access_token():
    """Generate an OAuth2 token using service account credentials."""
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request

    creds = service_account.Credentials.from_service_account_file(
        "", scopes=["https://www.googleapis.com/auth/drive"]
    )
    creds.refresh(Request())  # Refresh token
    return creds.token  # Return OAuth token


def get_dynamic_sheet_names(additional_sheet_name):
    """
    Get sheet names based on the last two months.
    - Existing sheet = Two months ago ("Jan25" if today is March25)
    - New sheet = Last month ("Feb25" if today is March25)
    """
    today = datetime.today()
    
    # Two months ago (existing sheet)
    two_months_ago = today.replace(day=1) - timedelta(days=1)
    two_months_ago = two_months_ago.replace(day=1) - timedelta(days=1)
    existing_sheet_name = two_months_ago.strftime("%b%y") + additional_sheet_name  # "Jan25"
    
    # Last month (new sheet)
    last_month = today.replace(day=1) - timedelta(days=1)
    new_sheet_name = last_month.strftime("%b%y") + additional_sheet_name # "Feb25"
    
    return existing_sheet_name, new_sheet_name