import os
import sys
import argparse
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.sheet_utils import (ensure_sheet_with_header, append_to_sheet)
from processing.alert_processor import (process_api_response, get_existing_keys, filter_new_alerts, build_alert_rows, ALERT_HEADER)
from services.ticket_service import TicketService, TOKEN, PM_LIST

# =========================================
# CONFIG
# =========================================

SHEET_ID = ""

# =========================================
# Helpers
# =========================================

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--validation_date",
        required=False,
        help="YYYY-MM-DD. Default = today"
    )
    return parser.parse_args()

def get_year():
    return datetime.now().strftime("%Y")

def get_fetch_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_run_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# =========================================
# MAIN
# =========================================

def main():
    print(" MODEL DQC ALERT START ")

    # ---------------------------------
    # Init
    # ---------------------------------
    service = GsheetAPI.get_service()
    args = parse_args()

    run_id = get_run_id()
    api_fetch_time = get_fetch_time()
    year = get_year()

    validation_date = (
        args.validation_date
        if args.validation_date
        else datetime.now().strftime("%Y-%m-%d")
    )

    ticket_service = TicketService(
        token=TOKEN,
        pm_list=PM_LIST,
    )

    ALERT_TAB = f"log_alert_{year}"

    print(f"run_id: {run_id}")
    print(f"validation_date: {validation_date}")
    print(f"tab: {ALERT_TAB}")

    # ---------------------------------
    # Ensure sheet exists
    # ---------------------------------
    ensure_sheet_with_header(
        service,
        SHEET_ID,
        ALERT_TAB,
        ALERT_HEADER
    )

    # ---------------------------------
    # Fetch API
    # ---------------------------------

    df = process_api_response(validation_date)
    print(f"API rows fetched: {len(df)}")

    # ---------------------------------
    # Load existing keys (same date only)
    # ---------------------------------
    existing_keys = get_existing_keys(
        service,
        SHEET_ID,
        ALERT_TAB,
        validation_date
    )

    print(f"Existing logged alerts: {len(existing_keys)}")

    # ---------------------------------
    # Filter NEW alerts only
    # ---------------------------------
    df_new = filter_new_alerts(df, existing_keys)

    print(f"New alerts detected: {len(df_new)}")

    if df_new.empty:
        print("No new alerts. Skip append.")
        return
    
    # ---------------------------------
    # Raise tickets
    # ---------------------------------
    print("Raising Jira tickets...")
    df_new = df_new.copy()

    results = [
        ticket_service.raise_ticket(
            row, 
            run_date=validation_date
        )
        for row in df_new.itertuples(index=False, name=None)
    ]

    key, statuses = zip(*results)

    df_new["jira_ticket"] = key
    df_new["status"] = statuses
    
    # ---------------------------------
    # Build rows
    # ---------------------------------
    rows = build_alert_rows(
        df_new,
        run_id,
        api_fetch_time
    )

    # ---------------------------------
    # Append
    # ---------------------------------
    append_to_sheet(
        service,
        SHEET_ID,
        ALERT_TAB,
        rows
    )

    # fail job AFTER sheet is updated
    failed_count = sum(s == "failed" for s in statuses)

    if failed_count:
        raise RuntimeError(f"{failed_count} Jira tickets failed")

    print("Done ✅")


# =========================================

if __name__ == "__main__":
    main()