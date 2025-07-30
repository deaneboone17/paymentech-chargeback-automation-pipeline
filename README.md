[![Python CI](https://github.com/deaneboone17/paymentech-chargeback-automation-pipeline/actions/workflows/python-app.yml/badge.svg)](https://github.com/deaneboone17/paymentech-chargeback-automation-pipeline/actions/workflows/python-app.yml)



# Chargeback Automation Pipeline (Paymentech + GCP)  
Automates daily chargeback submissions to JPMorgan Chase using Python, GCP, and AWS. Built for scale, compliance, and audit readiness.

---

## 🚀 Overview

This project automates the end-to-end workflow for preparing and submitting chargeback evidence files to JPMorgan Chase’s Merchant Services platform (Stratus/CBIS). It follows the strict format and protocol required by Paymentech and ensures reliable, compliant delivery of transaction data and supporting documentation.

Developed while serving as Associate Director of Finance Data Enablement at SiriusXM, this pipeline replaced a time-consuming manual process with a secure, scheduled, and audit-friendly solution.

---

## ❗ Problem

Manual preparation of daily chargeback submissions was inefficient, error-prone, and inconsistent. Submissions required:

- Parsing flat files from Paymentech’s DFR system
- Matching PDF evidence files
- Building structured headers, indexes, and binary content in a specific format (v2.11.0.0)
- Uploading to both GCP and AWS for ingestion by JPMC’s CBIS system

Failures in formatting or delivery could result in rejected claims or compliance gaps.

---

## 🛠️ Architecture & Tools

**Languages & Frameworks:**
- Python  
- Jupyter & Databricks Notebooks (for initial dev and testing)

**Cloud Services:**
- Google Cloud Functions (execution)  
- Google Cloud Scheduler (daily trigger)  
- Google Cloud Storage (DFR file ingestion, output ZIP delivery)  
- AWS S3 (dual-upload redundancy)  
- Google Secret Manager (credential handling)  
- Google Cloud Logging (audit trail, error handling)

---

## 🔄 Pipeline Flow

1. Cloud Scheduler triggers the daily Cloud Function
2. Secret Manager retrieves GCP and AWS credentials securely
3. Paymentech DFR files are pulled from GCP Storage
4. Files are parsed for chargeback transactions with specific RTM codes
5. PDF documents are generated from a base template and matched to transactions
6. Header and index files are generated per JPMC spec v2.11.0.0
7. Files are zipped with binary header prepended
8. Final ZIP is uploaded to both GCP and AWS endpoints
9. Cloud Logs are updated for traceability
10. Last-run timestamp is written to prevent duplicate processing

---

## 📥 Inputs / 📤 Outputs

**Inputs:**
- DFR flat files from Paymentech (via GCP Storage)  
- Customer agreement PDF template  
- Timestamp file for incremental loads

**Outputs:**
- ZIP file containing:
  - Header-prepended binary file
  - `.txt` index with H1, D, and T records
  - Evidence PDFs  
- Logs and run metadata for audit

---

## 🔐 Compliance & Controls

This pipeline adheres to:
- *JPMC Chargeback Multiple Document Upload Stratus Supplemental Guide – v2.11.0.0*

It enforces:
- Accurate naming and formatting for index and image files  
- Size and encoding standards for CBIS compatibility  
- Secure handling of credentials and sensitive data  
- Traceable logs for audit compliance  

---

## 💡 Business Impact

- Saved hours of manual processing time weekly  
- Reduced submission errors and file rejections  
- Enabled reliable daily submission with no human intervention  
- Delivered full audit traceability for Finance and Risk teams  
- Recovered $1.7M+ in chargebacks via automated and compliant submission

---

## 🧩 Engineering Notes

- Custom parsing logic was needed to handle schema drift in Paymentech DFR files  
- Dual cloud uploads required isolated, credentialed sessions and timeout handling  
- Binary header injection into a ZIP stream required low-level byte manipulation  
- Logging and retry logic ensured resilience and alerting via Cloud Logs  

---

## 🔄 Current Status

As of April 2025, this pipeline is actively in production and adaptable to other high-compliance submission workflows (e.g. legal, collections, or B2B settlements).

---

**Author**: Deane Boone  
**Role**: Director | Data Strategy & Automation  
**Contact**: [LinkedIn](https://www.linkedin.com/in/deaneboone/)
