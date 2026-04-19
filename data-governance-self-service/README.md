# Self-Service Data Governance Tool

## 📌 Overview

This project enables bulk management of data ownership and access control through a self-service workflow powered by Google Sheets and Jenkins.

It is designed to allow users to safely update Data task owners and editors at scale, while ensuring proper validation, auditability, and controlled execution.

The tool reduces manual effort in governance operations and minimizes the risk of incorrect updates through structured validation and staged execution.

---

## ⚙️ Key Features

- Bulk update of Data task owners and editors via Google Sheets  
- Self-service interface for non-technical users  
- Multi-level validation (input, grouping, email format)  
- Safe execution with staged modes: `env_check → check → update`  
- Batch API processing with chunking to handle large-scale updates  
- Full audit logging of all operations and API responses  
- Integrated with Jenkins for controlled and repeatable execution  

---

## 🏗️ System Flow

1. User prepares input data in Google Sheets  
2. Jenkins pipeline is triggered with required parameters  
3. Validate environment and spreadsheet access (`env_check`)  
4. Validate input data without API execution (`check`)  
5. Group and validate tasks (owner, editors, flags)  
6. Execute bulk update via DataHub API (`update`)  
7. Log all results into Google Sheets (Result Log)  
8. Fail pipeline if any API operation fails  

---

## 🧩 Project Structure

```bash
├── src/
│   └── update_data_owner.py   # Main script for validation, grouping, and API updates

├── jenkins/
│   └── jenkins.groovy      # Pipeline orchestration (env_check → check → update)