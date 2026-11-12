# 23L-0994 — Task 3: Building Airflow ETL Pipeline

**Student:** Shaheer Chishti | **Roll No:** 23L-0994

## Overview

An Apache Airflow ETL pipeline (deployed via Astro CLI) that extracts daily currency exchange rates from the [Frankfurter API](https://frankfurter.dev/), transforms them into a standardized schema, and loads them into MongoDB.

---

## Architecture

```
Frankfurter API ──► [Extract Task] ──(XCom)──► [Transform Task] ──(XCom)──► [Load Task] ──► MongoDB
```

### Services (Docker)

| Service             | Port  | Description                          |
|---------------------|-------|--------------------------------------|
| `airflow-webserver` | 8080  | Airflow UI                           |
| `airflow-scheduler` | —     | DAG scheduler                        |
| `postgres`          | 5432  | Airflow metadata DB                  |
| `mongodb`           | 27017 | Primary ETL storage (persistent vol) |
| `mongo-express`     | 8081  | MongoDB admin UI                     |

### Docker Networking & Storage
- All services share a custom bridge network: **`airflow_etl_shared_network`**
- MongoDB uses a named persistent volume: **`task3_mongodb_persistent_storage`**

---

## DAG: `currency_exchange_etl_pipeline`

**Schedule:** `@daily`  
**File:** [`dags/currency_exchange_etl_dag.py`](dags/currency_exchange_etl_dag.py)

### Tasks

1. **`extract_currency_rates`** — Calls Frankfurter API, pushes raw JSON via Named XCom key `raw_exchange_rate_data`
2. **`transform_currency_data`** — Pulls raw data, transforms into schema below, pushes via Named XCom key `transformed_exchange_rate_data`
3. **`load_data_into_mongodb`** — Pulls transformed records, upserts into MongoDB `currency_db.exchange_rates`

### Transformed Record Schema
```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1127,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T16:00:00"
}
```

---

## Setup & Run

### Prerequisites
- Docker & Docker Compose installed

### Start All Services
```bash
# 1. Build the image
docker-compose build

# 2. Initialize Airflow DB + create admin user
docker-compose run --rm airflow-init

# 3. Start all services
docker-compose up -d

# 4. Access Airflow UI at: http://localhost:8080
#    Login: admin / admin

# 5. Access Mongo Express at: http://localhost:8081
```

### Trigger the DAG manually
```bash
docker-compose exec airflow-scheduler airflow dags trigger currency_exchange_etl_pipeline
```

### View MongoDB data
```bash
docker exec -it task3_mongodb mongosh -u admin -p password123 --authenticationDatabase admin \
  --eval "db = db.getSiblingDB('currency_db'); printjson(db.exchange_rates.find({}).toArray())"
```

---

## Environment Variables

| Variable            | Default         | Description               |
|---------------------|-----------------|---------------------------|
| `MONGO_HOST`        | `mongodb`       | MongoDB hostname          |
| `MONGO_PORT`        | `27017`         | MongoDB port              |
| `MONGO_USER`        | `admin`         | MongoDB username          |
| `MONGO_PASSWORD`    | `password123`   | MongoDB password          |
| `MONGO_DB`          | `currency_db`   | Target database           |
| `MONGO_COLLECTION`  | `exchange_rates`| Target collection         |

---

## Project Structure

```
23L-0994_Task3_Airflow_ETL/
├── dags/
│   └── currency_exchange_etl_dag.py   # Main ETL DAG
├── plugins/                            # Airflow plugins (empty)
├── logs/                               # Airflow logs (auto-generated)
├── Dockerfile                          # Custom Airflow image
├── docker-compose.yml                  # Full stack (Airflow + MongoDB)
├── requirements.txt                    # Python dependencies
└── README.md
```
