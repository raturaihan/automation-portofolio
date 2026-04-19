import ingestion.alert_api as alert_api
import json 
import pandas as pd 
from typing import Set, Tuple, List

# =========================
# Headers
# =========================

ALERT_HEADER = [
    "run_id",
    "api_fetch_time",
    "schema",
    "idc",
    "model_table",
    "rule_name",
    "pic",
    "validation_timestamp",
    "validation_date",
    "execution_date",
    "error_msg",
    "dqc_link",
    "jira_ticket",
    "status"
]

# =========================
# API → DataFrame
# =========================

def process_api_response(validation_date):
    """
    Fetch DQC alerts from API and return normalized dataframe.
    No logging logic here.
    """

    api_response = alert_api.get_api_response(validation_date)

    # Parse the API response 
    response_data = json.loads(api_response)

    # Extract data 
    rows = response_data.get("rows", [])
    values_to_write = []
    for row in rows:
        values = row.get("values", {})
        schema = values.get("schema")
        idc = values.get("idc")
        model_table = values.get("model_table")
        rule_name = values.get("rule_name")
        pic = values.get("pic")
        validation_timestamp = values.get("validation_timestamp")
        validation_date = values.get("validation_date")
        execution_date = values.get("execution_date")
        error_msg = values.get("error_msg")
        dqc_link = values.get("dqc_link")
        values_to_write.append([schema, idc, model_table, rule_name, pic, validation_timestamp, validation_date, execution_date, error_msg, dqc_link])

    # Create a dataframe
    df = pd.DataFrame(values_to_write, columns=["schema", "idc", "model_table", "rule_name", "pic", "validation_timestamp", "validation_date", "execution_date", "error_msg", "dqc_link"])
    return df

# =========================
# Read sheet → existing keys (same validation_date only)
# =========================
def get_existing_keys(service, spreadsheet_id, tab_name, validation_date):
    """
    Read existing alerts for SAME validation_date only.
    """

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!C:I")
            .execute()
        )

        values = result.get("values", [])

        if len(values) <= 1:
            return set()

        header = values[0]
        df = pd.DataFrame(values[1:], columns=header)

        # only today's date (VERY IMPORTANT for performance)
        df = df[df["validation_date"] == validation_date]

        return set(zip(
            df["schema"],
            df["idc"],
            df["model_table"],
            df["rule_name"],
            df["validation_timestamp"],
        ))

    except:
        return set()

# =========================
# Filter new alerts
# =========================

def filter_new_alerts(df, existing_keys):
    """
    Remove alerts already logged in sheet.
    """
    if df.empty: 
        return df

    df = df.copy()

    df["__key__"] = list(zip(
        df.schema,
        df.idc,
        df.model_table,
        df.rule_name,
        df.validation_timestamp
    ))

    df = df[~df["__key__"].isin(existing_keys)]

    return df.drop(columns="__key__")

# =========================
# DataFrame → Sheet rows
# =========================

def build_alert_rows(df, run_id, api_fetch_time):
    """
    Convert dataframe into rows for alert history sheet.
    """

    rows = []

    if df.empty:
        return []

    for r in df.itertuples(index=False):
        rows.append([
            run_id,
            api_fetch_time,
            r.schema,
            r.idc,
            r.model_table,
            r.rule_name,
            r.pic,
            r.validation_timestamp,
            r.validation_date,
            r.execution_date,
            r.error_msg,
            r.dqc_link,
            getattr(r, "jira_ticket", ""),
            getattr(r, "status", "")
        ])

    return rows