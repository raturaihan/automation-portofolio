from typing import List


# ======================================================
# Get existing tab names
# ======================================================

def get_tab_names(service, spreadsheet_id: str):
    """
    Return all sheet(tab) names inside spreadsheet.
    """

    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    sheets = meta.get("sheets", [])

    return {
        s["properties"]["title"]
        for s in sheets
    }


# ======================================================
# Create sheet if not exists + write header
# ======================================================

def ensure_sheet_with_header(
    service,
    spreadsheet_id: str,
    tab_name: str,
    header: List[str],
):
    """
    If tab doesn't exist:
        - create it
        - write header row
    """

    existing_tabs = get_tab_names(service, spreadsheet_id)

    if tab_name in existing_tabs:
        return

    print(f"Creating new tab: {tab_name}")

    # 1️⃣ create tab
    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": tab_name
                    }
                }
            }
        ]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

    # 2️⃣ write header
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()


# ======================================================
# Append rows
# ======================================================

def append_to_sheet(
    service,
    spreadsheet_id: str,
    tab_name: str,
    rows: List[List],
):
    """
    Append multiple rows into sheet.
    """

    if not rows:
        return

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=tab_name,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

    print(f"Appended {len(rows)} rows → {tab_name}")
