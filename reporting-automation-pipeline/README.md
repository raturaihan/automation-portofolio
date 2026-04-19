# Reporting Automation Pipeline

## 📌 Overview

This project automates reporting and analytical workflows by integrating data from OLAP-based data warehouse APIs and Jira APIs, then publishing structured outputs into Google Sheets dashboards.

The pipeline focuses on transforming raw operational and analytical data into actionable insights, enabling better monitoring of data quality alerts and Jira ticket performance.

---

## ⚙️ Key Features

- Fetch alert data from OLAP API (data warehouse)
- Analyze Jira ticket lifecycle using changelog history
- Calculate transition durations (working time vs calendar time)
- Automatically generate and update Google Sheets reports
- Dynamically create monthly reporting sheets with pre-defined format
- Apply formulas for downstream analysis (model extraction, business mapping)
- Support scheduled execution via Jenkins and cron

---

## 🏗️ System Flow

1. Fetch alert data from OLAP API  
2. Fetch Jira ticket data (including transition history)  
3. Analyze ticket transitions and compute duration metrics  
4. Transform results into structured datasets  
5. Generate or duplicate monthly Google Sheets tabs  
6. Write results and apply formulas for reporting  
7. Scheduled via Jenkins (cron-based execution)  

---

## 🧩 Project Structure

```bash
src/

├── ingestion/
│   └── data_alert.py        # Fetch alert data from OLAP API

├── analysis/
│   └── jira_transition_duration.py  # Analyze Jira transitions & durations

├── reporting/
│   └── sheet_flow.py                # Sheet duplication and formatting automation

├── config/
│   └── config.py    # Jira credentials and configuration