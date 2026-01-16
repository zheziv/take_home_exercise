# FX Tick Data ETL – 1-Minute OHLC

## Overview
This project implements a Python ETL (Extract, Transform, Load) pipeline that converts **FX tick-level data** into **production-ready 1-minute OHLC (Open, High, Low, Close)** data using mid prices.

The pipeline:
- Reads gzipped CSV tick data
- Computes mid prices
- Aggregates tick data into 1-minute OHLC bars
- Outputs gzipped CSV files suitable for downstream analytics or modeling

---

## Input Data

### Location
~/Downloads/AT_take_home_exercise/tick_data/date=2024-03-01/sample_fx_data_A.csv.gz

### Format
Each input CSV file contains the following columns:

| Column        | Description |
|---------------|-------------|
| datetime      | Timestamp of the tick |
| currency_pair | Currency pair in `XXXYYY` format (e.g., `USDJPY`) |
| bid           | Bid price |
| ask           | Ask price |
| volume        | Trading volume |

---

## Transformation Logic

### Mid Price
mid = (bid + ask) / 2

### OHLC Aggregation
Tick data is aggregated into **1-minute bars** per currency pair:

| Field | Definition |
|------|------------|
| open | First mid price in the minute |
| high | Maximum mid price in the minute |
| low  | Minimum mid price in the minute |
| close| Last mid price in the minute |

### Currency Pair Normalization
USDJPY → USD/JPY

---

## Output Data

### Location
~/Downloads/AT_take_home_exercise/data_etl/ohlc_data/date=2024-03-01/data.csv.gz

### Format
The output is a gzipped CSV with the following schema:

| Column        | Description |
|---------------|-------------|
| timestamp     | 1-minute timestamp |
| currency_pair | Normalized format `XXX/YYY` |
| open          | Opening mid price |
| high          | Highest mid price |
| low           | Lowest mid price |
| close         | Closing mid price |

---

## Requirements

- Python **3.8+**
- pandas

Install dependencies:
```bash
pip install pandas

How to Run

Activate your virtual environment (if applicable), then run:

python data_etl.py

On success:
ETL completed successfully (1-minute OHLC).

## Notes

- There is no datetime data for 2024 in `sample_fx_data_A.csv.gz`.
- Therefore, the expected date is set to **2023-12-01**.
- You can change this value in `data_etl/data_etl.py` (line 16), for example to **2024-03-01**,  
- to verify that the prediction date validation works correctly.
