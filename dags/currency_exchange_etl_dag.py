"""
DAG: Currency Exchange ETL Pipeline
Assignment: Task 3 - Building Airflow ETL Pipeline
Author: Shaheer Chishti (Roll No: 23L-0994)
Description:
    Extracts daily currency exchange rates from Frankfurter API,
    transforms the rates into the specified standardized document schema,
    and loads the transformed records into MongoDB.
    Uses Airflow Named XComs to pass data between tasks.
"""

from datetime import datetime, timedelta, timezone
import logging
import os
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default configuration and environment variables
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

FRANKFURTER_API_URL = os.getenv(
    "FRANKFURTER_API_URL",
    "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
)

# Named XCom keys
XCOM_RAW_DATA_KEY = "raw_exchange_rate_data"
XCOM_TRANSFORMED_DATA_KEY = "transformed_exchange_rate_data"

logger = logging.getLogger(__name__)


def extract_currency_rates(**context):
    """
    Task 1: Extract Currency Exchange Rates
    Fetches daily exchange rates from Frankfurter API and pushes raw response
    to Airflow using a Named XCom key.
    """
    logger.info("Starting extraction from Frankfurter API: %s", FRANKFURTER_API_URL)
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        response = requests.get(FRANKFURTER_API_URL, timeout=15)
        response.raise_for_status()
        raw_data = response.json()
        logger.info("Successfully fetched API response: %s", raw_data)
    except Exception as e:
        logger.error("Failed to extract data from Frankfurter API: %s", str(e))
        raise

    payload = {
        "raw_data": raw_data,
        "retrieved_at": retrieved_at,
        "source": "Frankfurter API"
    }

    # Named XCom push
    ti = context["ti"]
    ti.xcom_push(key=XCOM_RAW_DATA_KEY, value=payload)
    logger.info("Pushed raw data to Named XCom key '%s'", XCOM_RAW_DATA_KEY)
    return f"Extracted data at {retrieved_at} with {len(raw_data) if isinstance(raw_data, list) else 1} items"


def transform_currency_data(**context):
    """
    Task 2: Transform Exchange Rate Data
    Pulls raw exchange rate data from Named XCom, maps each rate record
    to the required schema:
    {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.15,
        "rate_date": "2026-11-04",
        "source": "Frankfurter API",
        "retrieved_at": "2026-11-05T07:15:00"
    }
    and pushes transformed records to Named XCom.
    """
    ti = context["ti"]
    logger.info("Pulling raw data from Named XCom key '%s'...", XCOM_RAW_DATA_KEY)
    payload = ti.xcom_pull(key=XCOM_RAW_DATA_KEY, task_ids="extract_currency_rates")

    if not payload or "raw_data" not in payload:
        raise ValueError("No raw currency data received from extract task")

    raw_data = payload["raw_data"]
    retrieved_at = payload.get("retrieved_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
    source_name = payload.get("source", "Frankfurter API")

    transformed_records = []

    # Handle v2 list format: [{"date": "...", "base": "EUR", "quote": "USD", "rate": 1.1127}, ...]
    if isinstance(raw_data, list):
        for item in raw_data:
            transformed_doc = {
                "base_currency": str(item.get("base", "EUR")).strip(),
                "target_currency": str(item.get("quote")).strip(),
                "exchange_rate": float(item.get("rate")),
                "rate_date": str(item.get("date")).strip(),
                "source": source_name,
                "retrieved_at": retrieved_at
            }
            transformed_records.append(transformed_doc)

    # Handle v1 dictionary format: {"base": "EUR", "date": "...", "rates": {"USD": 1.11, "GBP": 0.83}}
    elif isinstance(raw_data, dict) and "rates" in raw_data:
        base_curr = raw_data.get("base", "EUR")
        rate_date = raw_data.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        for target_curr, rate_val in raw_data["rates"].items():
            transformed_doc = {
                "base_currency": str(base_curr).strip(),
                "target_currency": str(target_curr).strip(),
                "exchange_rate": float(rate_val),
                "rate_date": str(rate_date).strip(),
                "source": source_name,
                "retrieved_at": retrieved_at
            }
            transformed_records.append(transformed_doc)
    else:
        raise ValueError(f"Unrecognized Frankfurter API response structure: {type(raw_data)}")

    logger.info("Transformed %d records successfully: %s", len(transformed_records), transformed_records)

    # Named XCom push
    ti.xcom_push(key=XCOM_TRANSFORMED_DATA_KEY, value=transformed_records)
    logger.info("Pushed transformed records to Named XCom key '%s'", XCOM_TRANSFORMED_DATA_KEY)
    return transformed_records


def load_data_into_mongodb(**context):
    """
    Task 3: Load Data into MongoDB
    Pulls transformed exchange rate records from Named XCom, connects to MongoDB,
    and upserts each document into the target collection.
    """
    ti = context["ti"]
    logger.info("Pulling transformed data from Named XCom key '%s'...", XCOM_TRANSFORMED_DATA_KEY)
    records = ti.xcom_pull(key=XCOM_TRANSFORMED_DATA_KEY, task_ids="transform_currency_data")

    if not records:
        raise ValueError("No transformed records found to load into MongoDB")

    # Connect to MongoDB
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    logger.info("Connecting to MongoDB at %s:%s...", MONGO_HOST, MONGO_PORT)
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)

    try:
        # Verify connection
        client.admin.command('ping')
        logger.info("Successfully authenticated and connected to MongoDB!")

        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # Ensure index on unique combination: (base_currency, target_currency, rate_date)
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True
        )

        upserted_count = 0
        for doc in records:
            query_filter = {
                "base_currency": doc["base_currency"],
                "target_currency": doc["target_currency"],
                "rate_date": doc["rate_date"]
            }
            result = collection.update_one(
                query_filter,
                {"$set": doc},
                upsert=True
            )
            upserted_count += 1
            logger.info("Saved doc for %s/%s on %s: matched=%s, modified=%s, upserted_id=%s",
                        doc['base_currency'], doc['target_currency'], doc['rate_date'],
                        result.matched_count, result.modified_count, result.upserted_id)

        total_docs = collection.count_documents({})
        logger.info("Load complete! Processed %d records. Total documents in collection: %d",
                    upserted_count, total_docs)

        # Log sample documents from MongoDB for verification
        sample_cursor = collection.find({}).sort("_id", -1).limit(3)
        samples = list(sample_cursor)
        for s in samples:
            s["_id"] = str(s["_id"])
            logger.info("Verified in MongoDB: %s", s)

        return f"Successfully loaded {upserted_count} records into MongoDB {MONGO_DB}.{MONGO_COLLECTION}. Total docs: {total_docs}"

    finally:
        client.close()


default_args = {
    'owner': 'shaheer_23L-0994',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='currency_exchange_etl_pipeline',
    default_args=default_args,
    description='Task 3: Airflow ETL Pipeline for Frankfurter Currency Rates to MongoDB',
    schedule_interval='@daily',
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['currency', 'etl', 'frankfurter', 'mongodb', 'astro', '23L-0994'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_currency_rates',
        python_callable=extract_currency_rates,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_currency_data',
        python_callable=transform_currency_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data_into_mongodb',
        python_callable=load_data_into_mongodb,
        provide_context=True,
    )

    # Task dependency pipeline
    extract_task >> transform_task >> load_task
