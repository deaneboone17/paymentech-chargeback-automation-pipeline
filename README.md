# Automated Chargeback Document Packaging and Delivery

This project automates the end-to-end process of preparing and submitting chargeback evidence files to JPMorgan Chase’s Merchant Services platform (Stratus/CBIS) according to their strict format and protocol requirements.

## 📌 Project Overview

Developed while serving as Associate Director of Finance Data Enablement at SiriusXM, this pipeline was designed to streamline and secure the submission of chargeback evidence by automating the extraction, formatting, packaging, and upload of structured data and supporting PDF documentation.

## 🔍 Problem Statement

Manually preparing daily chargeback submissions was time-consuming, error-prone, and inconsistent. The process required converting transaction data into a complex format accepted by Paymentech (JPMC), then bundling evidence PDFs and uploading them to both AWS and GCP cloud storage endpoints.

## 🧱 Architecture

**Tools and Technologies**:
- Jupyter & Databricks Notebooks (initial development and testing)
- Python
- Google Cloud Functions
- Cloud Scheduler (trigger)
- Google Cloud Storage
- AWS S3
- Google Secret Manager (credentials)

**Pipeline Flow**:
1. Cloud Scheduler triggers Cloud Function daily
2. Secret Manager retrieves credentials securely for GCP and AWS
3. GCP Storage pulls Paymentech DFR chargeback files
4. Files are parsed to extract RTM records with specific reason codes
5. Matching PDFs are generated from a base template
6. Index and header records are built per JPMC's format spec (v2.11.0.0)
7. Files are zipped and prefixed with the required header
8. Final ZIP is uploaded to both GCP and AWS
9. Logs are written to GCP for auditing
10. Last run timestamp is updated to prevent duplicates

## 📄 Input/Output

**Input**:  
- Paymentech DFR flat files from GCP  
- PDF template (customer agreement)  
- Last run time file (used for filtering)

**Output**:  
- A ZIP file with:
  - Header record prepended to the binary
  - A `.txt` index file (with H1, D, and T records)
  - PDF copies of the evidence
- Logs and metadata for traceability

## ✅ Compliance

This automation strictly follows the formatting rules outlined in:
- *Chargeback Multiple Document Upload Stratus Supplemental Guide – v2.11.0.0*

All image files and index records meet the size, naming, and content rules defined by JPMorgan Chase for successful ingestion by CBIS.

## 📈 Impact

- Saved hours of manual preparation per week
- Reduced submission errors and rejections
- Enabled hands-free daily submissions
- Provided clear audit trail via structured logs
- Increased transparency for finance and audit teams

## 🧠 Lessons Learned

- Schema drift and inconsistent record layouts in source files required robust parsing logic
- GCP + AWS dual uploads needed secure, isolated credential handling
- Packaging binary headers with ZIP content required custom file handling
- Scheduling and error monitoring through Cloud Logs helped ensure reliability

## 🛠️ Project Status

As of 2025-04-11, this automation is active and scalable, and can be adapted for other high-compliance submission use cases across finance or legal ops.

---

**Author**: Deane Boone  
**Role**: Director | Data Strategy & Automation | AI Strategy (in progress)

