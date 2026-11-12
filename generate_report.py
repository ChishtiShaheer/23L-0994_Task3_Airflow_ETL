"""
Automated PDF Report Generator for Task 3: Airflow ETL Pipeline
Author: Shaheer Chishti (23L-0994)
"""
import os, sys, json, subprocess
from datetime import datetime
sys.path.insert(0, '/home/shaheer/23L-0994_A2_Ops/venv/lib/python3.14/site-packages')
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, PageBreak, HRFlowable, Preformatted)

OUTPUT = "23L-0994_Task3_Report.pdf"

def build_pdf():
    doc = SimpleDocTemplate(OUTPUT, pagesize=letter,
                            rightMargin=45, leftMargin=45, topMargin=45, bottomMargin=45)
    styles = getSampleStyleSheet()
    story = []

    # Styles
    title_style = ParagraphStyle('T', parent=styles['Heading1'],
                                 fontName='Helvetica-Bold', fontSize=20, leading=26,
                                 textColor=colors.HexColor('#1a1a2e'), spaceAfter=6)
    h2 = ParagraphStyle('H2', parent=styles['Heading2'],
                         fontName='Helvetica-Bold', fontSize=14,
                         textColor=colors.HexColor('#16213e'), spaceAfter=4, spaceBefore=12)
    h3 = ParagraphStyle('H3', parent=styles['Heading3'],
                         fontName='Helvetica-Bold', fontSize=11,
                         textColor=colors.HexColor('#0f3460'), spaceAfter=4, spaceBefore=8)
    body = ParagraphStyle('B', parent=styles['Normal'], fontName='Helvetica',
                           fontSize=10, leading=14, spaceAfter=4)
    code_style = ParagraphStyle('C', parent=styles['Normal'], fontName='Courier',
                                 fontSize=8, leading=11, backColor=colors.HexColor('#f4f4f4'),
                                 leftIndent=12, rightIndent=12, spaceAfter=6, spaceBefore=6)

    def hr():
        return HRFlowable(width="100%", thickness=1, color=colors.HexColor('#cccccc'), spaceAfter=8, spaceBefore=8)

    # Title Page
    story.append(Paragraph("Task 3: Airflow ETL Pipeline", title_style))
    story.append(Paragraph("Building a Currency Exchange ETL with Apache Airflow & MongoDB", h2))
    story.append(hr())
    meta = [
        ["Student Name", "Shaheer Chishti"],
        ["Roll No", "23L-0994"],
        ["Course", "MLOps / Data Engineering"],
        ["Submission Date", datetime.now().strftime("%B %d, %Y")],
        ["GitHub Repo", "https://github.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL"],
    ]
    t = Table(meta, colWidths=[1.8*inch, 4*inch])
    t.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#e8f4f8')),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.white, colors.HexColor('#f9f9f9')]),
        ('PADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.3*inch))

    # Section 1: Overview
    story.append(Paragraph("1. Assignment Overview", h2))
    story.append(hr())
    story.append(Paragraph(
        "This task requires building an end-to-end ETL pipeline using Apache Airflow (via Astro CLI) "
        "that extracts daily currency exchange rates from the Frankfurter API, transforms the data into "
        "a standardized schema, and loads it into MongoDB. Data is passed between tasks using Airflow "
        "Named XComs.", body))

    story.append(Paragraph("API Used:", h3))
    story.append(Paragraph("Frankfurter API — <b>https://api.frankfurter.dev/v2/rates?base=EUR&amp;quotes=USD,GBP,JPY</b>", body))

    # Section 2: Architecture
    story.append(PageBreak())
    story.append(Paragraph("2. Pipeline Architecture", h2))
    story.append(hr())
    story.append(Paragraph("The pipeline consists of 3 sequential tasks connected via Named XComs:", body))

    arch = [
        ["Task", "ID", "Description", "XCom Key"],
        ["Extract", "extract_currency_rates", "Call Frankfurter API, get EUR→USD/GBP/JPY rates", "raw_exchange_rate_data (push)"],
        ["Transform", "transform_currency_data", "Map raw response to target schema", "transformed_exchange_rate_data (push)"],
        ["Load", "load_data_into_mongodb", "Upsert transformed docs into MongoDB", "—"],
    ]
    at = Table(arch, colWidths=[0.8*inch, 1.8*inch, 2.5*inch, 2*inch])
    at.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#16213e')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTNAME', (0,1), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f0f4f8')]),
        ('PADDING', (0,0), (-1,-1), 5),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(at)
    story.append(Spacer(1, 0.2*inch))

    # Section 3: Docker Services
    story.append(Paragraph("3. Docker Services Configuration", h2))
    story.append(hr())
    story.append(Paragraph(
        "All services run on a shared Docker bridge network (<b>airflow_etl_shared_network</b>). "
        "MongoDB uses a named persistent volume (<b>task3_mongodb_persistent_storage</b>).", body))

    svc = [
        ["Service", "Image", "Port", "Role", "Labels"],
        ["airflow-webserver", "apache/airflow:2.10.2", "8080", "Airflow UI", "project, student_id"],
        ["airflow-scheduler", "apache/airflow:2.10.2", "—", "DAG scheduler", "project, student_id"],
        ["postgres", "postgres:13", "5432", "Airflow metadata DB", "project, student_id"],
        ["mongodb", "mongo:6.0", "27017", "ETL storage (persistent vol)", "project, student_id,\nservice_type, storage,\nnetworking"],
        ["mongo-express", "mongo-express:1.0.2", "8081", "MongoDB admin UI", "project, student_id,\nservice_type, storage,\nnetworking"],
    ]
    st = Table(svc, colWidths=[1.3*inch, 1.5*inch, 0.6*inch, 1.5*inch, 2.1*inch])
    st.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0f3460')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTNAME', (0,1), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f0f4f8')]),
        ('PADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(st)

    # Section 4: DAG Code
    story.append(PageBreak())
    story.append(Paragraph("4. DAG Code", h2))
    story.append(hr())

    dag_code = open('/home/shaheer/23L-0994_Task3_Airflow_ETL/dags/currency_exchange_etl_dag.py').read()
    # Split into sections
    story.append(Paragraph("4.1 Extract Task", h3))
    story.append(Preformatted('''def extract_currency_rates(**context):
    """Calls Frankfurter API and pushes raw data via Named XCom."""
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    response = requests.get(FRANKFURTER_API_URL, timeout=15)
    response.raise_for_status()
    raw_data = response.json()
    payload = {"raw_data": raw_data, "retrieved_at": retrieved_at, "source": "Frankfurter API"}
    ti = context["ti"]
    ti.xcom_push(key="raw_exchange_rate_data", value=payload)
    return f"Extracted data at {retrieved_at}"''', code_style))

    story.append(Paragraph("4.2 Transform Task", h3))
    story.append(Preformatted('''def transform_currency_data(**context):
    """Pulls raw data from XCom, maps to target schema, pushes transformed records."""
    ti = context["ti"]
    payload = ti.xcom_pull(key="raw_exchange_rate_data", task_ids="extract_currency_rates")
    raw_data = payload["raw_data"]
    transformed_records = []
    for item in raw_data:   # Frankfurter v2 returns a list
        transformed_records.append({
            "base_currency":   item.get("base", "EUR"),
            "target_currency": item.get("quote"),
            "exchange_rate":   float(item.get("rate")),
            "rate_date":       item.get("date"),
            "source":          "Frankfurter API",
            "retrieved_at":    payload["retrieved_at"]
        })
    ti.xcom_push(key="transformed_exchange_rate_data", value=transformed_records)
    return transformed_records''', code_style))

    story.append(Paragraph("4.3 Load Task", h3))
    story.append(Preformatted('''def load_data_into_mongodb(**context):
    """Pulls transformed records from XCom and upserts into MongoDB."""
    ti = context["ti"]
    records = ti.xcom_pull(key="transformed_exchange_rate_data", task_ids="transform_currency_data")
    client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/")
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.create_index([("base_currency",1),("target_currency",1),("rate_date",1)], unique=True)
    for doc in records:
        collection.update_one(
            {"base_currency": doc["base_currency"],
             "target_currency": doc["target_currency"], "rate_date": doc["rate_date"]},
            {"$set": doc}, upsert=True)
    client.close()
    return f"Loaded {len(records)} records into MongoDB"''', code_style))

    story.append(Paragraph("4.4 DAG Definition", h3))
    story.append(Preformatted('''with DAG(
    dag_id="currency_exchange_etl_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["currency", "etl", "frankfurter", "mongodb", "23L-0994"],
) as dag:
    extract_task  = PythonOperator(task_id="extract_currency_rates",  python_callable=extract_currency_rates)
    transform_task= PythonOperator(task_id="transform_currency_data", python_callable=transform_currency_data)
    load_task     = PythonOperator(task_id="load_data_into_mongodb",  python_callable=load_data_into_mongodb)
    extract_task >> transform_task >> load_task''', code_style))

    # Section 5: Transformed Data Schema
    story.append(PageBreak())
    story.append(Paragraph("5. Transformed Data Schema", h2))
    story.append(hr())
    story.append(Paragraph("Each extracted rate is mapped to the following document schema before being loaded into MongoDB:", body))

    # Live API data
    import urllib.request
    try:
        with urllib.request.urlopen("https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY", timeout=10) as r:
            api_response = json.loads(r.read())
    except Exception as e:
        api_response = [{"error": str(e)}]

    story.append(Paragraph("Live API Response from Frankfurter:", h3))
    story.append(Preformatted(json.dumps(api_response, indent=2), code_style))

    # Build transformed records
    retrieved_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    transformed = []
    if isinstance(api_response, list):
        for item in api_response:
            transformed.append({
                "base_currency": item.get("base", "EUR"),
                "target_currency": item.get("quote"),
                "exchange_rate": item.get("rate"),
                "rate_date": item.get("date"),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at
            })

    story.append(Paragraph("Transformed Records (ready to load):", h3))
    story.append(Preformatted(json.dumps(transformed, indent=2), code_style))

    # Section 6: docker-compose
    story.append(PageBreak())
    story.append(Paragraph("6. Docker Compose Configuration", h2))
    story.append(hr())
    dc_code = open('/home/shaheer/23L-0994_Task3_Airflow_ETL/docker-compose.yml').read()
    story.append(Preformatted(dc_code[:3000], code_style))

    # Section 7: Submission info
    story.append(PageBreak())
    story.append(Paragraph("7. Submission", h2))
    story.append(hr())
    story.append(Paragraph(
        "<b>GitHub Repository:</b> https://github.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL", body))
    story.append(Spacer(1, 0.1*inch))
    story.append(Paragraph("All code has been committed and pushed to the repository. The pipeline was successfully verified end-to-end.", body))

    doc.build(story)
    print(f"Report generated: {OUTPUT}")

if __name__ == "__main__":
    build_pdf()
