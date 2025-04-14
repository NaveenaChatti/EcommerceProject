# 💼 Brazilian E-Commerce Data Engineering Project

This project is an end-to-end data engineering pipeline built around a real-world Brazilian e-commerce dataset. The goal was to understand the full lifecycle of data—from ingestion to transformation, and finally to analytics-ready output—using industry-standard tools and cloud services.

---

## 📁 Data Sources

The raw data comes from a Kaggle dataset on Brazilian e-commerce and includes **9 CSV files**, each representing a different entity:

- Customers
- Orders
- Reviews
- Order Items
- Products
- Sellers
- Geolocation
- Payments - A SQL database version (hosted on `files.io`)
- Product_category- A MongoDB export (hosted separately for NoSQL exploration)

---

## ☁️ Tools & Services Used

| Layer | Tools / Services |
|------|------------------|
| Data Storage | GitHub, files.io, Azure Blob Storage |
| Data Processing | PySpark, Azure Synapse Analytics |
| Data Orchestration | Azure Data Factory (ADF) |
| Architecture | Medallion Architecture (Bronze → Silver → Gold) |
| Database | Azure Synapse Pools |
| Misc | Azure Portal, MongoDB, SQL |

---

## 🔧 Steps & Workflow

1. **Data Collection**  
   Downloaded datasets from Kaggle and stored 7 of the CSVs in a GitHub repo. The remaining SQL and NoSQL files were hosted on `files.io` and a MongoDB instance.

2. **Cloud Setup**  
   Created a free-tier Azure account to leverage services like Blob Storage, Synapse, and Data Factory.

3. **Data Ingestion**  
   Used Azure Data Factory to load all datasets into Azure Blob Storage (Bronze layer).

4. **Data Processing & Consolidation**  
   Used pyspark (via Azure Databricks) to clean, join, and enrich the data. The resulting consolidated dataset was written to the **Silver** layer.

5. **Data Modeling**  
   Final tables were transformed into analytics-ready format and stored in the **Gold** layer (Synapse).

6. **Pipeline Automation**  
   Built and scheduled pipelines in ADF to automate the entire process—using linked services, dataflows.

---

## 🏗️ Architecture Overview

This project follows the **Medallion Architecture** pattern:

```plaintext
                                   🔽
        +-----------------------------+
        |        Data Sources         |
        |-----------------------------|
        | Kaggle CSVs, SQL (files.io),|
        | MongoDB Export              |
        +-------------+---------------+
                      |
                      v
          +-------------------------+
          |   Azure Data Factory    |
          |   (Orchestration Layer) |
          +-----------+-------------+
                      |
                      v
        +----------------------------+
        |  Azure Blob Storage (Raw)  |
        |        Bronze Layer        |
        +----------------------------+
                      |
                      v
        +-----------------------------+
        |    Azure Databricks         |
        |  (Data Cleaning + Spark ETL)|
        +-------------+---------------+
                      |
                      v
        +-----------------------------+
        | Azure Data Lake / Synapse   |
        |       Silver Layer          |
        +-------------+---------------+
                      |
                      v
        +-----------------------------+
        |    Azure Synapse SQL Pools  |
        |       (Gold Layer DB)       |
        +-----------------------------+
