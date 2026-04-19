from jira import JIRA
from datetime import datetime, time, timedelta
from dateutil.relativedelta import WE, TH
import pandas as pd
import pygsheets
from reporting.sheet_flow import duplicate_previous_month_sheet, get_dynamic_sheet_names
 
import config.config as config

def get_issue_transitions():
    # Get today's date
    today = datetime.today()

    # Calculate the first and last day of the previous month
    first_day_prev_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day_prev_month = today.replace(day=1) - timedelta(days=1)

    # Format the dates as YYYY-MM-DD
    first_day_str = first_day_prev_month.strftime("%Y-%m-%d")
    last_day_str = (last_day_prev_month + timedelta(days=1)).strftime("%Y-%m-%d")

    jira = JIRA(server=config.url, token_auth=config.token_auth)
 
    # Use the JQL query to get the relevant issues and expand the changelog field
    jql_query = f'labels in () and status not in (Icebox, Closed) and createdDate >= "{first_day_str}" and createdDate < "{last_day_str}"'
    issue_list = jira.search_issues(jql_query, expand='changelog', maxResults=1000)
 
    result = []
 
    # Map JIRA datas
    for issue in issue_list:
        issue_link = issue.permalink()
        issue_key = issue.key
        issue_summary = issue.raw['fields']['summary']
        issue_assignee = issue.raw['fields']['assignee']['name']
        issue_created = issue.fields.created
        labels = issue.fields.labels
        dqc_flag = next((label for label in labels if label in ['True', 'False']), None)
        transitions = issue.changelog.histories
        status_end_time = None

        # Initialize due_date from issue fields (if available)
        due_date = issue.raw['fields'].get('duedate', None)
        if due_date:
            try:
                due_date = datetime.strptime(due_date.split()[0], "%Y-%m-%d").date()
            except Exception as e:
                print(f"⚠️ Failed to parse initial due_date: {due_date} for issue {issue_key}, error: {e}")
                due_date = None
        latest_due_date = due_date  # Store the latest due date

 
        for transition in transitions:
            transition_date = transition.created
            transition_author = transition.author.name
            transition_from = None
            transition_to = None
            # New: Flag to track if due date changed in this transition
            transition_due_date_changed = False
            for item in transition.items:
                if item.field == 'status':
                    if not transition_from:
                        transition_from = item.fromString
                        current_status_end_time = status_end_time
                    transition_to = item.toString

            if transition_from and transition_to:
                thisdict = dict(
                    link=issue_link, key=issue_key, summary=issue_summary, assignee=issue_assignee,created_date=issue_created, due_date=latest_due_date,
                    transition_date=transition_date, transition_author=transition_author,
                    transition_from=transition_from, transition_to=transition_to, dqc_flag = dqc_flag
                )
 
                if current_status_end_time is not None:
                    start = pd.to_datetime(current_status_end_time)
                else:
                    start = pd.to_datetime(issue_created)

                end = pd.to_datetime(transition_date)

                # Calculate durations
                wd_seconds = calculate_working_duration(start, end)
                cd_seconds = calculate_calendar_duration(start, end)

                # Add to dict
                thisdict['duration_wd'] = get_formatted_time(wd_seconds)
                thisdict['duration_cd'] = get_formatted_time(cd_seconds)
                thisdict['total_seconds_wd'] = wd_seconds
                thisdict['total_seconds_cd'] = cd_seconds

                 
                status_end_time = transition_date
                current_status_end_time = transition_date
                 
                result.append(thisdict)
    
    result_df = pd.DataFrame(result)
    print(result_df)
 
    return result_df
 
def get_formatted_time(seconds):
    days = int(seconds / 86400)
    hours = int((seconds % 86400) / 3600)
    minutes = int((seconds % 3600) / 60)
    seconds = int(seconds % 60)
     
    if days > 0:
        if days == 1:
            return f"{days} day, {hours} hours, {minutes} minutes"
        else:
            return f"{days} days, {hours} hours, {minutes} minutes"
    elif hours > 0:
        return f"{hours} hours, {minutes} minutes"
    elif minutes > 0:
        return f"{minutes} minutes"
    else:
        return f"{seconds} seconds"
 
def calculate_working_duration(start_date, end_date):
    # Convert to datetime and remove timezone (if present)
    start_date = pd.to_datetime(start_date).tz_localize(None)
    end_date = pd.to_datetime(end_date).tz_localize(None)
 
    # Initialize total seconds
    total_seconds = 0
 
    # Iterate through each day in the range
    current_date = start_date
    while current_date.date() <= end_date.date():
        # Check if current_date is a weekday (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Weekday (Monday to Friday)
            # Define the start and end of the working day
            start_of_day = datetime.combine(current_date.date(), time(0))  # 00:00:00
            end_of_day = datetime.combine(current_date.date(), time(23, 59, 59))  # 23:59:59
             
            if current_date.date() == start_date.date():
                # Handle the first day
                effective_start = max(start_date, start_of_day)
            else:
                # For intermediate days
                effective_start = start_of_day
             
            if current_date.date() == end_date.date():
                # Handle the last day
                effective_end = min(end_date, end_of_day)
            else:
                # For intermediate days
                effective_end = end_of_day
 
            # Only add to total_seconds if there is a valid working duration
            if effective_start < effective_end:
                total_seconds += (effective_end - effective_start).total_seconds()
         
        # Move to the next day
        current_date += timedelta(days=1)
     
    return total_seconds
 
def calculate_calendar_duration(start_date, end_date):
    """Calculate total duration in seconds including weekends."""
    start_date = pd.to_datetime(start_date).tz_localize(None)
    end_date = pd.to_datetime(end_date).tz_localize(None)
    return (end_date - start_date).total_seconds()
 
if __name__ == '__main__':
    data = get_issue_transitions()
    
    additional_sheet_name = '_transition'
    existing_sheet_name , sheet_name = get_dynamic_sheet_names(additional_sheet_name=additional_sheet_name)
    
    gc = pygsheets.authorize(service_file="creds.json")
    sh = gc.open('')

     # Check if sheet already exists
    try:
        worksheet = sh.worksheet_by_title(sheet_name)
    except pygsheets.WorksheetNotFound:
        worksheet = duplicate_previous_month_sheet(additional_sheet_name=additional_sheet_name)

 
    # Clear existing content and write the dataframe to the worksheet
    worksheet.clear(start='A1', end='S')
    worksheet.set_dataframe(data, start='A1')

    # Add 'concat' formula in column P
    worksheet.update_value('P1', 'concat')  # Header for column P
    formula_range = f'P2:P{len(data) + 1}'
    worksheet.update_values(formula_range, [[f'=CONCAT(I{r},J{r})'] for r in range(2, len(data) + 2)])

    # Add 'model' formula in column Q
    worksheet.update_value('Q1', 'model')  # Header for column Q

    model_formula_range = f'Q2:Q{len(data) + 1}'
    worksheet.update_values(model_formula_range, [
        [f'=TRIM(REGEXEXTRACT(C{r}, "(?:\\s-\\s(?:sg#|useast#)?)([^-]+)"))'] for r in range(2, len(data) + 2)
    ])

    # Add 'business_line' formula in column R
    worksheet.update_value('R1', 'business_line')  # Header for column S

    business_line_formula_range = f'R2:R{len(data) + 1}'
    worksheet.update_values(
        business_line_formula_range,
        [[f"=VLOOKUP(Q{r}, 'Raw'!B:D, 3, FALSE)"] for r in range(2, len(data) + 2)]
    )

    # Add 'biz_line' formula in column S
    worksheet.update_value('S1', 'biz_line')  # Header for column T

    biz_line_formula_range = f'S2:S{len(data) + 1}'
    worksheet.update_values(
    f'S2:S{len(data) + 1}',
    [
        [
            '=IF(OR(R{0}="biz_line_1", R{0}="biz_line_2", R{0}="biz_line_3"), "main_biz_line_1", '
            'IF(OR(R{0}="biz_line_4", R{0}="biz_line_5"), "main_biz_line_2", '
            'IF(OR(R{0}="bix&biz_line_6", R{0}="biz_line_7", R{0}="biz_line_8"), "main_biz_line_3", "main_biz_line_4")))'.format(r)
        ]
        for r in range(2, len(data) + 2)
    ]
)