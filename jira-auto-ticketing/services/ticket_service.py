import jira
from datetime import datetime, timedelta

URL = 'https://jira.com/'
PROJECT_NAME = ''
TOKEN = ""


PRIORITY = 'Medium'
PM_LIST = ['']

# =====================================================
# JIRA CLIENT (API only)
# =====================================================

class JiraClient:

    def __init__(self, token: str):
        self.jira = jira.JIRA(server=URL, token_auth=TOKEN)

    # ---------- search ----------
    def search(self, jql: str):
        return self.jira.search_issues(jql)

    def find_open_issue(self, summary_prefix):
        jql = (
            f'summary ~ "\\"{summary_prefix}\\"" '
            f'AND status not in (Closed,Done,Icebox)'
        )
        issues = self.search(jql)
        return issues[0] if issues else None

    def find_today_issue(self, summary_prefix):
        jql = (
            f'summary ~ "\\"{summary_prefix}\\"" '
            f'AND updated > startOfDay()'
        )
        issues = self.search(jql)
        return issues[0] if issues else None

    # ---------- actions ----------
    def create_issue(self, fields):
        return self.jira.create_issue(fields=fields)

    def add_comment(self, issue, text):
        self.jira.add_comment(issue, text)

    def add_watchers(self, issue_key, watchers):
        for w in watchers:
            self.jira.add_watcher(issue_key, w)

# =====================================================
# BUSINESS SERVICE (logic layer)
# =====================================================

class TicketService: 

    def __init__(self, token: str, pm_list=None):
        self.client = JiraClient(token)
        self.pm_list = set(pm_list or [])

    def normalize_schema(self, schema:str): 
        if schema == "":
            return ""
        if schema in ("", ""):
            return ""
        return schema 
    
    def build_summary_prefix(self, schema:str, model_table:str, rule_name:str, idc:str):
        schema = self.normalize_schema(schema)
        model_table = f"{idc.lower()}#{model_table}"
        return schema, model_table, f"{schema} - {model_table} - {rule_name}"
    
    def craft_description(self, error_msg, alert_link, idc, execution_date):
        description = f"""
        h2. Investigation Result
        h3. PM Check

        Check for alert Execution Time - {execution_date}

        {error_msg}

        {alert_link}

        IDC Region - {idc}

        h3. DEV Check
        (Please indicate your detailed investigation result here)
        """
        return description
    
    def build_comment(self, validation_timestamp, execution_date):
        return f"Same Alert triggered on {validation_timestamp} for execution date: {execution_date}"

    def raise_ticket(self, row, run_date):
        """
        row order must match dataframe:
        [schema,idc,model_table,rule_name,pic,
         validation_timestamp,validation_date,
         execution_date,error_msg,alert_link]
        """
        try:
            (
                schema,
                idc,
                model_table,
                rule_name,
                pic,
                validation_ts,
                validation_date,
                execution_date,
                error_msg,
                alert_link,
            ) = row

            reporter = ""
            assignee = f"{pic}@gmail.com"

            schema_norm, model_fmt, summary_prefix = \
                self.build_summary_prefix(schema, model_table, rule_name, idc)
            
            # There is an open ticket? -> Comment 
            open_issue = self.client.find_open_issue(summary_prefix)

            if open_issue:
                comment = self.build_comment(validation_ts, execution_date)
                self.client.add_comment(open_issue, comment)
                return open_issue.key, "commented_existing"
            
            # create new ticket
            due_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

            summary = f"{summary_prefix} - {run_date}"
            description = self.craft_description(error_msg, alert_link, idc, execution_date)

            people = {reporter, assignee, *self.pm_list}

            fields = {
                "project": PROJECT_NAME,
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Task"},
                "priority": {"name": PRIORITY},
                "components": [{"name": "Operation"}],
                "labels": ["alert"],
                "reporter": {"name": reporter},
                "assignee": {"name": assignee},
                "duedate": due_date,
                "customfield_10202": [{"name": p} for p in people],
            }

            issue = self.client.create_issue(fields)

            # special watchers 
            if schema_norm == "" and "" in model_fmt:
                self.client.add_watchers(issue.key, ["", ""])

            return issue.key, "created"
        
        except Exception as e: 
            print (f"[JIRA ERROR] {e}")
            return None, "failed"