from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os
import pandas as pd
import sys
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import json

def fetch_credentials(credential_id, credential_type_id, key_map=('username', 'password')):
    try:
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        payload = json.dumps({
            "credential_id": credential_id,
            "credential_type_id": credential_type_id
        })

        response = requests.post(credential_endpoint_url, headers=headers, data=payload)
        response_json = response.json()

        if response.status_code != 200 or 'expired' in str(response_json.get("data", "")):
            raise Exception(response_json.get("error_msg") or response_json.get("data"))

        data = response_json.get('data', {})
        return data[key_map[0]], data[key_map[1]]

    except Exception as e:
        logging.error(f"Failed to fetch credentials: {e}")
        sys.exit(1)

# # movementt id
# if len(sys.argv) < 2:
#     print("Usage: spark-submit report.py <data_movement_id>")
#     sys.exit(1)


spark = SparkSession.builder \
    .appName("PipelineExecutionReport") \
    .getOrCreate()

REPORT_DIR = "pipeline_reports_new"

os.makedirs(REPORT_DIR, exist_ok=True)

data_movement_id = int(spark.conf.get("spark.nabu.data_movement_id"))

kosh_credential_id = int(spark.conf.get("spark.nabu.kosh_credential_id"))
kosh_credential_type_id = int(spark.conf.get("spark.nabu.kosh_credential_type_id"))
token = spark.conf.get("spark.nabu.token")
credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")

kosh_username, kosh_password = fetch_credentials(kosh_credential_id, kosh_credential_type_id, key_map=('username', 'password'))

# -------- Configuration --------
JDBC_DRIVER = "org.postgresql.Driver"
# JDBC_JAR_PATH = "/opt/cloudera/parcels/CDH-7.2.18-1.cdh7.2.18.p400.57851273/jars/postgresql-42.7.2.jar"
USERNAME = kosh_username
PASSWORD = kosh_password
JDBC_URL = "jdbc:postgresql://w3.devpsql.modak.com:5432/nabu_v3"


# JDBC Properties
db_properties = {
    "user": USERNAME,
    "password": PASSWORD,
    "driver": JDBC_DRIVER
}

transformation_query = f"""
    SELECT 
        object_info->'config'->>'nodeName' AS node_name,
        node_type_name,
        object_info->'config'->>'description' AS description,
        status,
        start_time,
        end_time,
        wpod.object_id,
        cns.node_id
    FROM nabu.checkpoint_node_status cns
    JOIN nabu.workspace_pipeline_object_details wpod
        ON cns.data_movement_id = wpod.workspace_pipeline_id
       AND cns.node_id = wpod.object_id
    JOIN nabu.curation_node_type_lookup cntl
        ON cntl.node_type_id = (object_info->>'nodeTypeId')::int
    WHERE cns.data_movement_id = {data_movement_id}
      AND wpod.valid_to_ts = '9999-12-31 00:00:00.000'
    ORDER BY start_time
"""

query = f"({transformation_query}) as subquery"

try:
    df = spark.read.jdbc(url=JDBC_URL, table=query, properties=db_properties)
except Exception as e:
    print("Error while reading from JDBC:", e)
    spark.stop()
    sys.exit(1)

pdf_df = df.toPandas()
if pdf_df.empty:
    print("No records found. Please check the data_movement_id.")
    spark.stop()
    sys.exit(0)

status_counts = pdf_df["status"].value_counts().to_dict()
summary_text = " | ".join([f"{k}: {v}" for k, v in status_counts.items()])

report_file = os.path.join(REPORT_DIR, f"pipeline_execution_report_dm_{data_movement_id}.pdf")
doc = SimpleDocTemplate(report_file, pagesize=letter)
styles = getSampleStyleSheet()
title_style = styles["Title"]
header_style = styles["Heading2"]
normal_style = styles["BodyText"]

story = [
    Paragraph(f"Pipeline Execution Report - Data Movement ID: {data_movement_id}", title_style),
    Spacer(1, 12),
    Paragraph(f"Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", normal_style),
    Spacer(1, 12),
    Paragraph(f"Total Records: {len(pdf_df)}", normal_style),
    Spacer(1, 6),
    Paragraph(f"Status Summary: {summary_text}", normal_style),
    Spacer(1, 18),
    Paragraph("1. Node Definitions", header_style),
    Spacer(1, 12)
]

# Table: Node Definitions
node_table_data = [["Object ID", "Node Name", "Status", "Node Type", "Description"]]
for _, row in pdf_df.iterrows():
    status_color = colors.green if row["status"] == "Success" else (
        colors.red if row["status"] == "Failed" else colors.black)
    node_table_data.append([
        row["object_id"],
        row["node_name"],
        Paragraph(f'<font color="{status_color.hexval()}">{row["status"]}</font>', normal_style),
        row["node_type_name"],
        row["description"]
    ])

node_table = Table(node_table_data, hAlign='LEFT')
node_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT')
]))
story.append(node_table)
story.append(Spacer(1, 24))

# Table: Execution Status
story.append(Paragraph("2. Execution Status", header_style))
story.append(Spacer(1, 12))

exec_table_data = [["Node ID", "Status", "Start Time", "End Time"]]
for _, row in pdf_df.iterrows():
    status_color = colors.green if row["status"] == "Success" else (
        colors.red if row["status"] == "Failed" else colors.black)
    exec_table_data.append([
        row["node_id"],
        Paragraph(f'<font color="{status_color.hexval()}">{row["status"]}</font>', normal_style),
        str(row["start_time"]),
        str(row["end_time"])
    ])

exec_table = Table(exec_table_data, hAlign='LEFT')
exec_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT')
]))
story.append(exec_table)

# Generate PDF
doc.build(story)
print(f"PDF saved: {report_file}")
spark.stop()
print("SCRIPT COMPLETE")
