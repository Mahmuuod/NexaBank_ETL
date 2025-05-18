# NexaBank Real-Time Customer Churn Prediction Data Pipeline

> **DISCLAIMER:** All data used in this project is artificially generated using a Python script and does not reflect any real-world information.

## 📌 Project Overview

NexaBank is facing a high rate of customer churn and is seeking to develop a Machine Learning model to predict customers at risk of leaving. These customers will then be targeted with marketing offers to reduce churn.

As the **Data Engineer**, your role is to build a real-time, production-grade data pipeline that:

- Ingests messy, multi-source customer data from multiple formats (CSV, TXT, JSON)
- Transforms the raw data into clean, structured datasets
- Encrypts sensitive data
- Adds engineered features to support ML models
- Stores the results as **Parquet** files and uploads them to **HDFS**
- Ensures reliability with proper logging, error handling, schema validation, and email notifications

---

## 📁 Data Sources

| Source | Format | Description |
|--------|--------|-------------|
| Customer Profiles | CSV | Contains demographic and product usage data |
| Credit Card Billing | CSV | Monthly credit card payments per customer |
| Support Tickets | CSV | Complaint records |
| Loans | TXT | Loan utilization data |
| Transactions | JSON | Money transfers and purchases |

---

Each hour contains files for that timeframe.

---

## 🧠 Transformations

Each file type is transformed as follows:

### 1. **Customer Profiles**
- `tenure`: Years since account open date
- `customer_segment`: Categorized as "Loyal", "Newcomer", or "Normal"

### 2. **Credit Card Billing**
- `fully_paid`: True/False
- `debt`: Remaining unpaid amount
- `late_days`: Days past due
- `fine`: `late_days * 5.15`
- `total_amount`: `amount_due + fine`

### 3. **Support Tickets**
- `age`: Days since ticket issue

### 4. **Loans**
- `age`: Days since utilization
- `total_cost`: `20% of amount + 1000`
- `loan_reason`: Encrypted using Caesar cipher with **random key per file**

### 5. **Transactions**
- `cost`: `0.50 + 0.1% of transaction_amount`
- `total_amount`: transaction + cost

---

## 🔒 Security: Loan Reason Encryption

- Caesar Cipher with a **random key per file**
- Validated using a dictionary of English words
- Supports **keyless decryption** based on word matching heuristics

---

## 📊 Enrichment Columns (All Tables)

- `processing_time`: Timestamp of processing
- `partition_date`: Folder date
- `partition_hour`: Folder hour

---

## 🚀 Pipeline Features

✅ Real-time processing  
✅ Schema validation and rejection of malformed files  
✅ Logging of every action with metadata  
✅ Error handling with automatic retry  
✅ Email notifications for failures or rejections  
✅ Archiving mechanism to avoid reprocessing  
✅ Written using Object-Oriented Python with SOLID principles  
✅ Final output stored in **Parquet format**, uploaded to **HDFS**

---

## 📈 Optional Analysis (Optional but Recommended)

- **Churn vs Segment:** Loyalty and tenure insights
- **Geographical Trends:** Churn by city or region
- **Credit Behavior:** Impact of late payments on churn
- **Spending Patterns:** Differences between churned and loyal customers
- **ARPU:** Average revenue per user by segment

---

## 🧪 Requirements

- Python 3.10+
- pandas, pyarrow, pyhdfs, json, smtplib, email, logging
- HDFS CLI access
- Hive for downstream consumption

---

## 📜 Logging

- Timestamped logs of all pipeline steps are saved in the `logs/` directory.
- Each log entry includes:
  - ✅ Timestamp
  - ✅ Action performed
  - ✅ Status (Success/Failure)
  - ✅ Number of rows processed
  - ✅ Extracted schema
  - ✅ File paths
  - ✅ Error details (if any)

## ✉️ Email Notifications

- Email alerts are sent in the following cases:
  - ❌ Schema validation failures
  - ❌ Exceptions during transformation or loading
- Emails are sent via SMTP.
- Passwords are **not hardcoded**. They are stored securely in:


