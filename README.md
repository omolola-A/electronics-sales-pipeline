# Electronics Sales Data Pipeline

## 📖 Overview
This project automates the ingestion, cleaning, and storage of weekly electronics sales data using Apache Airflow and MySQL.

## ⚙️ Features
- Weekly scheduled pipeline
- Data cleaning (null removal, duplicates)
- MySQL data storage
- Execution logging

## 🛠️ Tools Used
- Apache Airflow
- MySQL
- Docker
- Python (Pandas, SQLAlchemy)

## ▶️ How to Run
1. Start Docker:
   docker-compose up -d

2. Access Airflow:
   http://localhost:8080

3. Trigger DAG:
   electronics_sales_pipeline

## 📊 Dataset
Sample dataset is included in the `/data` folder.

## 📌 Author
Omolola Adeojo # electronics-sales-pipeline
