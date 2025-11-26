# SQL_Data_Warehouse

Welcome to the SQL Datawarehouse Project 👋
A comprehensive data warehouse project built using SQL Server and ETL (Extract , Transform, Load) techniques. This project focuses on designing a scalable data warehouse architecture, implementing efficient data pipelines, and performing analytical queries for business intelligence insights.
This Project was designed as a portfolio project highlighting the industy best practices in data enginneering 🛢 and analytics 📈. 

# 🏗️ Data Architecture

The following Data Warehouse project follows the Medallion Architecture, consisting of Bronze, Silver, and Gold layers.

![Medallion Architecture Diagram](https://github.com/namitha9eshan/SQL_Data_Warehouse/blob/main/Documents/Datawarehouse%20Architecture.png)


## Layer Overview

1. **Bronze Layer:**  
   Stores raw data as-is from the source systems. Data is ingested from CSV files into a SQL Server database.

2. **Silver Layer:**  
   This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

3. **Gold Layer:**  
   Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 📖 Project Overview

This project involves:

- **Data Architecture:** Designing a Modern Data Warehouse using Medallion Architecture (Bronze, Silver, and Gold layers).
- **ETL Pipelines:** Extracting, transforming, and loading data from source systems into the warehouse.
- **Data Modeling:** Developing fact and dimension tables optimized for analytical queries.
- **Analytics & Reporting:** Creating SQL-based reports and dashboards for actionable insights.
