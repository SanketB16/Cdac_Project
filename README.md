
# Scalable ETL Data Pipeline with Medallion Architecture

## Project Overview

This project implements a scalable ETL pipeline using **Medallion Architecture** (Bronze, Silver, Gold layers) to ingest raw data, clean and transform it, and prepare it for visualization and machine learning. The pipeline leverages **Apache Spark**, **Delta Lake**, and **Apache Airflow** for orchestration, with Docker to ensure environment consistency.

---

## Features

- **Bronze Layer:** Raw Excel data ingestion and conversion to Parquet format.
- **Silver Layer:** Data cleaning and structured data storage in Delta Lake tables.
- **Gold Layer:** Aggregated, ML-ready datasets (planned).
- **Orchestration:** Airflow DAGs automate ETL workflows.
- **Containerization:** Docker images guarantee reproducible environments.
- **Visualization:** Tableau dashboards consume processed data for insights.

---

## Project Structure

```
airflow-docker-ETL/
│
├── dags/                              # Airflow DAG definitions
├── etl_scripts/                       # ETL Python scripts (ingestion, cleaning)
│   ├── excel_to_parquet.py
│   ├── parquet_to_delta.py
│   └── spark_clean_and_load.py
├── data/                             # Data storage in Medallion layers
│   ├── bronze/                       # Raw data files (Excel, Parquet)
│   ├── silver/                       # Cleaned Delta Lake tables
│   └── gold/                        # Aggregated/ML datasets (future)
├── jars/                             # Spark JAR dependencies (e.g., spark-excel)
├── logs/                             # Airflow task execution logs
├── ml/                              # ML notebooks and models
├── plugins/                         # Airflow plugins (if any)
├── tableau_dashboard/               # Tableau dashboards and data sources
├── docker-compose.yaml              # Docker Compose config for Airflow + Spark
├── requirements.txt                 # Python dependencies (for local dev)
├── README.md                       # This documentation file
└── venv/                           # Virtual environment (optional, gitignored)
```

---

## Prerequisites

- Docker & Docker Compose installed  
- Python 3.10 installed for local development  
- Java 11 or higher (required by Spark)  
- Access to a terminal/command line  

---

## Setup and Usage

### Running with Docker (Recommended)

1. **Build and start containers**

```bash
docker-compose up --build
```

2. **Access Airflow UI**

- Open [http://localhost:8080](http://localhost:8080) in your browser.
- Trigger and monitor DAGs to run ETL pipelines.

3. **Place raw Excel files**

- Add your raw Excel files to the `data/bronze/` directory (mounted inside the container).

4. **Check output**

- Transformed data will appear in `data/silver/` as Delta tables.
- Logs are available in the `logs/` directory.

---

### Local Development **Optional**

1. **Create and activate a virtual environment**

```bash
python -m venv venv
source venv/bin/activate    # Linux/macOS
venv\Scripts\activate     # Windows
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Run ETL scripts manually**

Example:

```bash
python etl_scripts/excel_to_parquet.py
```

---

## Airflow DAGs

- `etl_pipeline_dag.py` orchestrates the ETL workflow:
  - Convert Excel → Parquet (Bronze)
  - Load and clean data → Delta Lake (Silver)
  - (Future) Aggregate data → Gold Layer

---

## Tableau Dashboards

- Tableau files are stored in `tableau_dashboard/`
- Dashboards consume processed data from Silver or Gold layers.
- Ensure data connections point to the appropriate Parquet or Delta Lake locations.

---

## Notes

- Ensure Java is installed locally if running Spark outside Docker.
- The project is designed to scale with larger datasets and additional ML workflows.

---



