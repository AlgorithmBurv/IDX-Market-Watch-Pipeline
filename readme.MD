# IDX Market Watch Pipeline

An end-to-end automated data pipeline for monitoring the Indonesian Stock Exchange (IDX).  
This project handles data extraction, transformation, storage, and visualization of stock market trends using a containerized analytics stack.

---

## Tech Stack

- Data Ingestion & Processing: Python (Pandas, yfinance)
- Database / Data Warehouse: PostgreSQL
- Visualization & BI: Metabase
- Infrastructure: Docker & Docker Compose
- Scheduling: Python Loop / Task Scheduler

---

## Key Features

1. Automated ETL Pipeline  
   Fetches OHLCV stock data from Yahoo Finance automatically.

2. Containerized Infrastructure  
   PostgreSQL and Metabase run in Docker containers.

3. Interactive Market Dashboard  
   Visualizes IDX stock trends (e.g. LQ45 stocks).

---

## Dashboard Preview

![Trend Stock Prices](foto_bi.png)

---

## Project Structure

```text
stock_data/
├── src/
│   └── etl_job.py
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## How to Run

- docker-compose up -d
- pip install -r requirements.txt
- python src/etl_job.py
- Open: http://localhost:3000
