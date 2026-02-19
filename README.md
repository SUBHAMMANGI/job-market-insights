# Job Market Intelligence Platform 📊

A real-time data analytics platform that collects, processes, and analyzes U.S. job market data to generate actionable labor market insights using Python, SQL, PostgreSQL, and Power BI.

This project simulates an end-to-end production analytics pipeline — from data ingestion to business-ready dashboards — similar to real-world data engineering and business intelligence workflows.

---

## 🚀 Project Overview

The Job Market Intelligence Platform automates the process of:

- Collecting job postings from external APIs
- Cleaning and transforming raw datasets
- Building analytical data models
- Generating KPIs and market insights
- Powering interactive BI dashboards

The goal is to help answer questions such as:

- Which skills are most in demand?
- Salary trends across locations
- Hiring trends by role and industry
- Emerging technologies in job postings

---

## 🏗️ Architecture

Data Source APIs
↓
Ingestion Layer (Python)
↓
Processing & Cleaning
↓
PostgreSQL Data Warehouse
↓
Feature Engineering (SQL)
↓
Power BI Dashboards

---

## 📂 Project Structure
job-market-intelligence/
│
├── ingestion/        # API data collection scripts
├── processing/       # Data cleaning & transformation
├── features/         # Feature engineering logic
├── sql/              # Database schema & analytics queries
├── warehouse/        # Data warehouse logic
├── monitoring/       # Pipeline health checks
├── dashboards/       # BI dashboard assets
├── config/           # Config & pipeline state files
├── logs/             # Pipeline execution logs
│
├── run_pipeline.py   # Main pipeline orchestrator
├── requirements.txt  # Python dependencies
└── README.md

---

## ⚙️ Tech Stack

### Data Engineering
- Python
- PostgreSQL
- SQL
- Docker

### Analytics & BI
- Power BI
- Data Modeling
- KPI Development

### Libraries
- Pandas
- SQLAlchemy
- Requests
- Psycopg2

---

## 🔄 Pipeline Workflow

1. Fetch job postings from APIs
2. Store raw data
3. Clean and normalize datasets
4. Load structured data into PostgreSQL
5. Generate analytical tables
6. Create business-ready metrics
7. Visualize insights in Power BI

---

## 📊 Example Insights

- Top demanded technical skills by region
- Salary distribution trends
- Hiring volume over time
- Role-based demand forecasting

---

## 🧠 Key Skills Demonstrated

- End-to-end data pipeline design
- ETL development
- Data warehousing concepts
- SQL analytics modeling
- Dashboard-driven storytelling
- Production-style project structure

---

## ▶️ How to Run Locally

### 1. Clone repository
```bash
git clone https://github.com/YOUR_USERNAME/job-market-intelligence.git
cd job-market-intelligence

 2. Install dependencies
```
pip instal -r requirements.txt

# 3. Configure environment variables
Create .env file:
DB_HOST=localhost
DB_PORT=5432
DB_NAME=jobmarket
DB_USER=postgres
DB_PASSWORD=yourpassword

# 4. Run pipeline

python run_pipeline.py

👨‍💻 Author

Subham Mangi
MS Business Analytics & AI — UT Dallas
Data Analytics | Data Engineering | Business Intelligence

LinkedIn: https://linkedin.com/in/subhammangi


