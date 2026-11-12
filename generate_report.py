"""
Automated Formal PDF Report Generator for Task 3: Building Airflow ETL Pipeline
Author: Shaheer Chishti (23L-0994)
Course: MLOps / Data Engineering
"""
import os
import sys
import json
from datetime import datetime
from PIL import Image as PILImage

sys.path.insert(0, '/home/shaheer/23L-0994_A2_Ops/venv/lib/python3.14/site-packages')
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Preformatted, Image as RLImage, KeepTogether
)

OUTPUT_PDF = "/home/shaheer/23L-0994_Task3_Airflow_ETL/23L-0994_Task3_Report.pdf"
SCREENSHOTS_DIR = "/home/shaheer/23L-0994_Task3_Airflow_ETL/screenshots"


def get_scaled_image(img_filename, target_width=6.6 * inch, max_height=4.2 * inch):
    """Load an image, calculate aspect ratio, and return a scaled ReportLab Image flowable."""
    img_path = os.path.join(SCREENSHOTS_DIR, img_filename)
    if not os.path.exists(img_path):
        raise FileNotFoundError(f"Missing screenshot: {img_path}")

    with PILImage.open(img_path) as im:
        orig_w, orig_h = im.size

    aspect = orig_w / float(orig_h)
    calc_height = target_width / aspect

    if calc_height > max_height:
        calc_height = max_height
        target_width = calc_height * aspect

    rl_img = RLImage(img_path, width=target_width, height=calc_height)
    rl_img.hAlign = 'CENTER'
    return rl_img


def build_pdf():
    print(f"Generating Formal Submission Report: {OUTPUT_PDF}")

    doc = SimpleDocTemplate(
        OUTPUT_PDF,
        pagesize=letter,
        rightMargin=40,
        leftMargin=40,
        topMargin=40,
        bottomMargin=40
    )

    styles = getSampleStyleSheet()

    # Custom styling palette
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=20,
        leading=24,
        textColor=colors.HexColor('#0f2b46'),
        spaceAfter=4
    )
    subtitle_style = ParagraphStyle(
        'DocSubTitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        textColor=colors.HexColor('#3a506b'),
        spaceAfter=10
    )
    h1_style = ParagraphStyle(
        'SectionH1',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=17,
        textColor=colors.HexColor('#0f2b46'),
        spaceBefore=12,
        spaceAfter=6,
        keepWithNext=True
    )
    h2_style = ParagraphStyle(
        'SectionH2',
        parent=styles['Heading3'],
        fontName='Helvetica-Bold',
        fontSize=10.5,
        leading=14,
        textColor=colors.HexColor('#1b4965'),
        spaceBefore=8,
        spaceAfter=4,
        keepWithNext=True
    )
    body_style = ParagraphStyle(
        'DocBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        leading=13,
        textColor=colors.HexColor('#222222'),
        spaceAfter=5
    )
    caption_style = ParagraphStyle(
        'ImgCaption',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor('#1b4965'),
        alignment=1, # Center
        spaceBefore=4,
        spaceAfter=10
    )
    code_snippet_style = ParagraphStyle(
        'CodeSnippet',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=7.5,
        leading=10,
        backColor=colors.HexColor('#f5f7fa'),
        borderColor=colors.HexColor('#d0d7de'),
        borderWidth=0.5,
        borderPadding=6,
        spaceBefore=4,
        spaceAfter=6
    )

    def hr():
        return HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b0c4de'), spaceBefore=6, spaceAfter=8)

    story = []

    # ==========================================
    # COVER / HEADER
    # ==========================================
    story.append(Paragraph("Task 3: Building Airflow ETL Pipeline", title_style))
    story.append(Paragraph("Automated Currency Exchange ETL Integration with MongoDB & Mongo Express", subtitle_style))
    story.append(hr())

    meta_table_data = [
        [Paragraph("<b>Student Name</b>", body_style), Paragraph("Shaheer Chishti", body_style),
         Paragraph("<b>Roll Number</b>", body_style), Paragraph("23L-0994", body_style)],
        [Paragraph("<b>Course</b>", body_style), Paragraph("MLOps / Data Engineering", body_style),
         Paragraph("<b>Date</b>", body_style), Paragraph(datetime.now().strftime("%B %d, %Y"), body_style)],
        [Paragraph("<b>GitHub Repo</b>", body_style), Paragraph("<a href='https://github.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL'><u>https://github.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL</u></a>", body_style),
         Paragraph("<b>DagsHub Repo</b>", body_style), Paragraph("<a href='https://dagshub.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL'><u>https://dagshub.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL</u></a>", body_style)]
    ]
    meta_table = Table(meta_table_data, colWidths=[1.1*inch, 2.6*inch, 1.1*inch, 2.4*inch])
    meta_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0f5fa')),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#c3d9e8')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e1ecf4')),
        ('PADDING', (0,0), (-1,-1), 5),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 10))

    # ==========================================
    # 1. EXECUTIVE SUMMARY & ARCHITECTURE
    # ==========================================
    story.append(Paragraph("1. Executive Summary & Pipeline Architecture", h1_style))
    story.append(Paragraph(
        "This project implements an end-to-end automated ETL pipeline engineered with Apache Airflow (via Astro CLI), "
        "Frankfurter Currency Exchange API, MongoDB primary persistent storage, and Mongo Express web administration interface. "
        "The architecture enforces modular design, persistent container storage, explicit Docker bridge networking, and "
        "strict data transfer between tasks using <b>Airflow Named XComs</b>.", body_style))

    arch_rows = [
        ["Phase", "Airflow Task ID", "Operator", "Data Transfer Mechanism", "Output Schema / Target"],
        ["Extract", "extract_currency_rates", "PythonOperator", "Named XCom ('raw_exchange_rate_data')", "Raw JSON from Frankfurter API v2"],
        ["Transform", "transform_currency_data", "PythonOperator", "Named XCom ('transformed_exchange_rate_data')", "Standardized 6-attribute documents"],
        ["Load", "load_data_into_mongodb", "PythonOperator", "Pulls Named XCom", "MongoDB: currency_db.exchange_rates"]
    ]
    arch_table = Table(arch_rows, colWidths=[0.8*inch, 1.8*inch, 1.1*inch, 2.1*inch, 1.4*inch])
    arch_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0f2b46')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 8),
        ('FONTSIZE', (0,1), (-1,-1), 7.5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#c3d9e8')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8fafc')]),
        ('PADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(arch_table)
    story.append(Spacer(1, 8))

    # Transformed data specification block
    story.append(Paragraph("<b>Required Standardized Document Schema:</b>", h2_style))
    story.append(Preformatted("""{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1122,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T17:14:43"
}""", code_snippet_style))

    # ==========================================
    # 2. SCREENSHOT 1 & 2: AIRFLOW DAG EXECUTION
    # ==========================================
    story.append(PageBreak())
    story.append(Paragraph("2. Airflow DAG & Task Execution Verification", h1_style))
    story.append(Paragraph(
        "The DAG <code>currency_exchange_etl_pipeline</code> was scheduled and triggered. As captured below, "
        "all three tasks successfully executed in sequence. The web UI displays the DAG state as active, healthy, and green.", body_style))

    # Screenshot 1
    story.append(get_scaled_image("1_airflow_dags_list.png", target_width=6.8*inch, max_height=2.8*inch))
    story.append(Paragraph("Figure 1: Airflow Web UI — Active DAGs list showing 'currency_exchange_etl_pipeline' unpaused with successful runs.", caption_style))

    story.append(Spacer(1, 6))

    # Screenshot 2
    story.append(get_scaled_image("2_airflow_graph_success.png", target_width=6.8*inch, max_height=3.2*inch))
    story.append(Paragraph("Figure 2: Airflow Graph View — All three pipeline tasks ('extract_currency_rates' ➔ 'transform_currency_data' ➔ 'load_data_into_mongodb') in SUCCESS state.", caption_style))

    # ==========================================
    # 3. SCREENSHOT 3: AIRFLOW NAMED XCOM DATA PASSING
    # ==========================================
    story.append(PageBreak())
    story.append(Paragraph("3. Named XCom Data Transfer Verification", h1_style))
    story.append(Paragraph(
        "Per assignment specifications, all task communication strictly utilizes <b>Airflow Named XComs</b>. "
        "The extract task pushes raw API responses under key <code>raw_exchange_rate_data</code>. "
        "The transform task pulls this key and pushes mapped records under <code>transformed_exchange_rate_data</code>. "
        "The load task retrieves these transformed records directly from the named XCom before writing to MongoDB.", body_style))

    # Screenshot 3
    story.append(get_scaled_image("3_airflow_named_xcoms.png", target_width=6.8*inch, max_height=2.7*inch))
    story.append(Paragraph("Figure 3: Airflow XCom Admin View — Showing verified keys 'raw_exchange_rate_data' and 'transformed_exchange_rate_data' in metadata store.", caption_style))

    story.append(Paragraph("<b>Code Snippet — Named XCom Push and Pull Implementation:</b>", h2_style))
    story.append(Preformatted("""# Task 1: Named XCom Push
ti.xcom_push(key="raw_exchange_rate_data", value=payload)

# Task 2: Named XCom Pull & Push
payload = ti.xcom_pull(key="raw_exchange_rate_data", task_ids="extract_currency_rates")
# ... data transformation ...
ti.xcom_push(key="transformed_exchange_rate_data", value=transformed_records)

# Task 3: Named XCom Pull
records = ti.xcom_pull(key="transformed_exchange_rate_data", task_ids="transform_currency_data")""", code_snippet_style))

    # ==========================================
    # 4. SCREENSHOT 4 & 5: MONGODB & MONGO EXPRESS
    # ==========================================
    story.append(PageBreak())
    story.append(Paragraph("4. MongoDB Storage & Mongo Express Administration Interface", h1_style))
    story.append(Paragraph(
        "MongoDB is configured as the primary persistent database, with Mongo Express deployed as a web administration interface. "
        "The load task performs an idempotent upsert on compound key <code>(base_currency, target_currency, rate_date)</code>. "
        "Both the Mongo Express UI and direct terminal queries verify the populated records matching the exact required schema.", body_style))

    # Screenshot 4
    story.append(get_scaled_image("4_mongo_express_ui.png", target_width=6.8*inch, max_height=3.1*inch))
    story.append(Paragraph("Figure 4: Mongo Express Administration UI — Inspecting collection 'exchange_rates' under database 'currency_db' with loaded documents.", caption_style))

    story.append(Spacer(1, 6))

    # Screenshot 5
    story.append(get_scaled_image("5_terminal_mongodb_records.png", target_width=6.8*inch, max_height=2.8*inch))
    story.append(Paragraph("Figure 5: Terminal Output — Querying MongoDB directly via mongosh confirming documents containing base_currency, target_currency, exchange_rate, rate_date, source, and retrieved_at.", caption_style))

    # ==========================================
    # 5. SCREENSHOT 6: DOCKER SERVICES, NETWORKING & PERSISTENCE
    # ==========================================
    story.append(PageBreak())
    story.append(Paragraph("5. Docker Container Infrastructure, Networking & Storage", h1_style))
    story.append(Paragraph(
        "All required metadata is embedded within the Docker compose architecture: "
        "<br/>• <b>Docker Networking:</b> All services share <code>airflow_etl_shared_network</code> (custom bridge driver). "
        "<br/>• <b>Persistent Storage:</b> MongoDB data is mapped to named volume <code>task3_mongodb_persistent_storage</code>, ensuring zero data loss across container restarts. "
        "<br/>• <b>Metadata Labels:</b> Service type, student ID (<code>23L-0994</code>), networking, and storage labels are assigned to each container.", body_style))

    # Screenshot 6
    story.append(get_scaled_image("6_terminal_docker_meta.png", target_width=6.8*inch, max_height=1.8*inch))
    story.append(Paragraph("Figure 6: Terminal Output — Verifying running Docker containers, persistent named volume 'task3_mongodb_persistent_storage', and shared bridge network.", caption_style))

    docker_table_data = [
        ["Service Name", "Docker Image", "Port Mapping", "Role in Pipeline", "Persistence"],
        ["airflow-webserver", "airflow-etl-23l-0994", "8080:8080", "Airflow Web UI & Orchestration Monitor", "Stateless"],
        ["airflow-scheduler", "airflow-etl-23l-0994", "Internal", "DAG Scheduler & Task Executor", "Stateless"],
        ["postgres", "postgres:13", "5432:5432", "Airflow Metadata Database & XCom Store", "Persistent (volume)"],
        ["mongodb", "mongo:6.0", "27017:27017", "Primary ETL Database (currency_db)", "Persistent Volume"],
        ["mongo-express", "mongo-express:1.0.2", "8081:8081", "Web Administration Interface for MongoDB", "Stateless"]
    ]
    dt = Table(docker_table_data, colWidths=[1.3*inch, 1.4*inch, 0.9*inch, 2.3*inch, 1.3*inch])
    dt.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1b4965')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 8),
        ('FONTSIZE', (0,1), (-1,-1), 7.5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#c3d9e8')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8fafc')]),
        ('PADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(dt)

    # ==========================================
    # 6. SUBMISSION & REPOSITORIES
    # ==========================================
    story.append(Spacer(1, 10))
    story.append(Paragraph("6. Submission & Version Control Links", h1_style))
    story.append(Paragraph(
        "All pipeline source code, DAG scripts, Docker configurations, dependency manifests, and report generation utilities "
        "are version controlled and published to the following public repositories:", body_style))

    repo_data = [
        ["Platform", "Repository URL", "Status"],
        ["GitHub", "https://github.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL", "Pushed & Verified (Active)"],
        ["DagsHub", "https://dagshub.com/ChishtiShaheer/23L-0994_Task3_Airflow_ETL", "Configured & Mirrored"]
    ]
    rt = Table(repo_data, colWidths=[1.2*inch, 4.4*inch, 1.6*inch])
    rt.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0f2b46')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 8.5),
        ('FONTSIZE', (0,1), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#c3d9e8')),
        ('PADDING', (0,0), (-1,-1), 5),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(rt)

    # Build the document
    doc.build(story)
    print(f"Successfully generated {OUTPUT_PDF}")


if __name__ == "__main__":
    build_pdf()
