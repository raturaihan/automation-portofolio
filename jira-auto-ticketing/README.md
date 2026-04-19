# Jira Auto Ticketing Automation

## 📌 Overview

This project automates Jira ticket creation based on alerts fetched from an OLAP-based data warehouse API. It integrates multiple systems to build a reliable, end-to-end automation workflow for alert monitoring, ticketing, and logging.

The pipeline is designed to reduce manual effort, ensure consistent incident tracking, and improve response time for data quality issues.

---

## ⚙️ Key Features

- Fetch alerts from OLAP API (analytical data source)
- Deduplicate alerts to ensure idempotent processing
- Automatically create or update Jira tickets
- Add comments to existing tickets for recurring alerts
- Log alert activity and ticket status into Google Sheets
- Support scheduled execution via Jenkins and cron

---

## 🏗️ System Flow

1. Fetch alerts from OLAP API  
2. Transform and normalize API response  
3. Filter out previously processed alerts (deduplication)  
4. Create or update Jira tickets  
5. Append results into Google Sheets for tracking  
6. Scheduled via Jenkins (cron-based execution)  

---

## 🧩 Project Structure

```bash
src/
├── main.py

├── ingestion/
│   └── alert_api.py          # Fetch alerts from OLAP API

├── processing/
│   └── alert_processor.py    # Transform, deduplicate, and prepare data

├── services/
│   └── ticket_service.py     # Jira ticket creation & update logic

├── utils/
│   └── sheet_utils.py        # Google Sheets helper functions