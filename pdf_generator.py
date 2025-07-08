import re  # Import the regular expression module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER
import json
import logging
import sys
import requests

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


# -------------------- CONFIGURATION --------------------
# JDBC: Pipeline DB (now the single source)

# -------------------- HELPER FUNCTIONS --------------------


def get_status_for_validation_type(v_type, status_details):
    print("status_details:" , status_details)
    status_keywords = {
        "schema": ["schema validator", "fetching source schema", "fetching target schema",
                   "saving schema validation result"],
        "data": ["data validation", "saving data validation result"],
        "row_count": ["row count validator", "saving row count validation"],
        "uniqueness": ["uniqueness validator", "saving uniqueness validation"],
        "not_null": ["not null validator", "saving not null validation"],
        "ingestion": ["rest api source", "target", "get body", "ingestion"]
    }

    relevant_statuses = []
    for row in status_details:
        node_name = row.node_name.lower() if row.node_name else ""

        if v_type in status_keywords:
            for keyword in status_keywords[v_type]:
                if keyword in node_name:
                    relevant_statuses.append(row.status.upper())
                    break

    print("releant statuses:", relevant_statuses)

    if "FAILED" in relevant_statuses:
        return "Fail"
    elif "SUCCEEDED" in relevant_statuses:
        return "Success"
    else:
        return "NA"


# -------------------- PDF GENERATION FUNCTION --------------------

def generate_pipeline_report(spark_session, current_pipeline_id, current_batch_id, current_pipeline_name):
    """
    Generates a PDF report for a single pipeline based on provided IDs and name.
    """
    # SQL Queries - now using function arguments
    VALIDATION_SQL = f"""
    SELECT validation_type, comment, batch_id FROM validation_result
    WHERE batch_id = CAST('{current_batch_id}' AS BIGINT)
    ORDER BY validation_type
    """

    STATUS_DETAILS_SQL = f"""
WITH pipeline_status AS (
  SELECT status_code_id
  FROM (
    SELECT status_code_id,
           ROW_NUMBER() OVER (
             PARTITION BY data_movement_id, batch_id
             ORDER BY flow_ts DESC
           ) AS rownum
    FROM nabu.flow_status
    WHERE job_type_id IN (3, 25)
      AND data_movement_id = {current_pipeline_id}
      AND batch_id = {current_batch_id}
  ) x
  WHERE rownum = 1
)

SELECT 
  a.object_info #>> '{{config, nodeName}}' AS node_name,
  CASE
    WHEN b.status = 'SUCCESS' THEN 'Succeeded'
    WHEN b.status = 'COMPLETED' THEN 'Completed'
    WHEN (b.status IN ('PROGRESS', 'ERROR') OR b.status IS NULL)
         AND (SELECT status_code_id FROM pipeline_status) IN (13, 29, 32) THEN 'Failed'
    WHEN b.status = 'PROGRESS' THEN 'Running'
    WHEN b.status = 'ERROR' THEN 'Failed'
    WHEN b.status IS NULL AND (SELECT status_code_id FROM pipeline_status) = 13 THEN 'Failed'
    WHEN b.status IS NULL THEN 'Waiting'
  END AS status
FROM nabu.workspace_pipeline_object_details a
LEFT JOIN nabu.checkpoint_node_status b
  ON a.object_id = b.node_id
  AND a.workspace_pipeline_id = b.data_movement_id
  AND b.batch_id = {current_batch_id}
WHERE a.workspace_pipeline_id = {current_pipeline_id}
    """

    print("STATUS_DETAILS_SQL", STATUS_DETAILS_SQL)

    # -------------------- LOAD DATA FROM DATABASE --------------------
    print(f"Executing VALIDATION_SQL for {current_pipeline_name}: {VALIDATION_SQL}")
    print(f"Executing STATUS_DETAILS_SQL for {current_pipeline_name}: {STATUS_DETAILS_SQL}")

    validation_results = []
    status_details = []

    if spark_session:  # Only attempt to load data if SparkSession was initialized successfully
        try:
            validation_df = spark_session.read.format("jdbc").options(
                url=PIPELINE_DB_URL,
                query=VALIDATION_SQL,
                user=PIPELINE_DB_USERNAME,
                password=PIPELINE_DB_PASSWORD,
                driver=JDBC_DRIVER
            ).load()
            validation_results = validation_df.collect()  # Collect all rows for easier processing

            if validation_results:
                print(f"Successfully loaded validation data for {current_pipeline_name}.")
                print("--- Raw Validation Results Data ---")
                for row in validation_results:
                    print(f"  Type: {row.validation_type}, Comment: {row.comment}")
                print("-----------------------------------")
            else:
                print(
                    f"No validation data found for BATCH_ID: {current_batch_id}. Please check the database and batch ID.")
                validation_results = []  # Ensure it's an empty list if no data
        except Exception as e:
            print(f"Error loading validation data for {current_pipeline_name}: {e}")
            validation_results = []  # Ensure it's an empty list if loading fails

        # Load Status Details Data
        try:
            status_details_df = spark_session.read.format("jdbc").options(
                url=PIPELINE_DB_URL,
                query=STATUS_DETAILS_SQL,
                user=PIPELINE_DB_USERNAME,
                password=PIPELINE_DB_PASSWORD,
                driver=JDBC_DRIVER
            ).load()
            status_details = status_details_df.collect()

            if status_details:
                print(f"Successfully loaded status details data for {current_pipeline_name}.")
                print("--- Raw Status Details Data ---")
                for row in status_details:
                    print(f"  Node Name: {row.node_name}, Status: {row.status}")
                print("-------------------------------")
            else:
                print(
                    f"No status details found for PIPELINE_ID: {current_pipeline_id}. Please check the database and pipeline ID.")
                status_details = []
        except Exception as e:
            print(f"Error loading status data for {current_pipeline_name}: {e}")
            status_details = []
    else:
        print(
            f"Skipping database data loading for {current_pipeline_name} because SparkSession could not be initialized.")

    # -------------------- PDF GENERATION --------------------

    # Define styles for the PDF document
    styles = getSampleStyleSheet()

    # Custom ParagraphStyle for main table headers to ensure bolding and consistent size
    main_table_header_bold_style = ParagraphStyle(name='MainTableHeaderBoldStyle', parent=styles['Normal'],
                                                  fontSize=10, alignment=TA_CENTER, fontName='Helvetica-Bold')

    # Style for the actual content cells in the table body - REDUCED FONT SIZE
    table_cell_style = ParagraphStyle(name='TableCellStyle', parent=styles['BodyText'], fontSize=7, alignment=TA_LEFT,
                                      spaceAfter=1)

    # New style for schema mismatch heading and comments to control spacing
    schema_mismatch_heading_style = ParagraphStyle(name='SchemaMismatchHeadingStyle', parent=styles['Normal'],
                                                   fontSize=7, alignment=TA_LEFT, fontName='Helvetica-Bold',
                                                   spaceBefore=0, spaceAfter=0)
    schema_mismatch_comment_style = ParagraphStyle(name='SchemaMismatchCommentStyle', parent=styles['BodyText'],
                                                   fontSize=7, alignment=TA_LEFT, spaceBefore=0, spaceAfter=0)

    # New style for ordered list items to control indentation
    list_item_style = ParagraphStyle(name='ListItemStyle', parent=styles['BodyText'],
                                     fontSize=7, alignment=TA_LEFT,
                                     leftIndent=12,
                                     firstLineIndent=-12,
                                     spaceBefore=0, spaceAfter=1)

    # New style for detailed log section
    detailed_log_heading_style = ParagraphStyle(name='DetailedLogHeadingStyle', parent=styles['Heading2'], fontSize=12,
                                                alignment=TA_LEFT, spaceAfter=10)
    detailed_log_content_style = ParagraphStyle(name='DetailedLogContentStyle', parent=styles['BodyText'], fontSize=7,
                                                alignment=TA_LEFT, spaceAfter=2)

    title_style = ParagraphStyle(name='TitleStyle', parent=styles['Heading1'], fontSize=14, alignment=TA_CENTER,
                                 spaceAfter=12)
    header_info_style = ParagraphStyle(name='HeaderInfoStyle', parent=styles['Normal'], fontSize=10, alignment=TA_LEFT,
                                       spaceAfter=4)
    table_header_style = ParagraphStyle(name='TableHeaderStyle', parent=styles['Normal'], fontSize=9,
                                        alignment=TA_CENTER, fontName='Helvetica-Bold')
    table_cell_bold_style = ParagraphStyle(name='TableCellBoldStyle', parent=styles['BodyText'], fontSize=8,
                                           alignment=TA_LEFT, fontName='Helvetica-Bold', spaceAfter=2)
    conclusion_style = ParagraphStyle(name='ConclusionStyle', parent=styles['Normal'], fontSize=9, alignment=TA_CENTER,
                                      spaceBefore=12)

    # Dynamic PDF path using current_pipeline_name and current_batch_id
    pdf_path = os.path.join(NEW_REPORT_DIR,
                            f"Test_Execution_Report_{current_pipeline_name}_Batch_{current_batch_id}.pdf")
    doc = SimpleDocTemplate(pdf_path, pagesize=A4, topMargin=40, bottomMargin=40, leftMargin=30, rightMargin=30)
    story = []

    # --------- Document Header ---------
    story.append(Paragraph("<b>Project ID:</b> BA0007156 Dataverse", header_info_style))
    story.append(Paragraph(f"<b>VTP Name:</b> {current_pipeline_name}", header_info_style))  # Use pipeline name here
    story.append(Paragraph("<b>Document Version Number:</b> &lt;Add version number here in format example: 1.0&gt;",
                           header_info_style))
    story.append(Spacer(1, 15))

    # --------- Test Set-Up Section ---------
    story.append(Paragraph("<b>Test Case Set-Up and Execution Steps</b>", title_style))
    story.append(Spacer(1, 5))

    # Table for Test Set-Up
    test_setup_data = [
        [Paragraph("<b>Test Set-Up</b>", table_header_style)],
        [Paragraph("<b>VTP Name</b>", table_header_style), Paragraph(current_pipeline_name, table_cell_style)],
        # Use pipeline name here
        [Paragraph("<b>Objective</b>", table_header_style),
         Paragraph("&lt;Add VTP Objective here&gt;", table_cell_style)],
        [Paragraph("<b>Set Ups</b>", table_header_style),
         Paragraph("&lt;Add Set up information here&gt;", table_cell_style)],
        [Paragraph("<b>Instruction To Tester</b>", table_header_style), Paragraph("", table_cell_style)],
    ]

    test_setup_table = Table(test_setup_data, colWidths=[100, 400])
    test_setup_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('SPAN', (0, 0), (1, 0)),
        ('BACKGROUND', (0, 1), (0, 1), colors.HexColor("#E0F2F7")),
        ('BACKGROUND', (0, 2), (0, 2), colors.HexColor("#FFF3E0")),
        ('BACKGROUND', (0, 3), (0, 3), colors.HexColor("#E8F5E9")),
        ('BACKGROUND', (0, 4), (0, 4), colors.HexColor("#F3E5F5")),
    ]))
    story.append(test_setup_table)
    story.append(Spacer(1, 20))

    # --------- Main Test Execution Table Section ---------
    story.append(Paragraph("<b>Test Execution</b>", title_style))
    story.append(Spacer(1, 5))

    # Define the data for the main test execution table header
    main_table_header_data = [
        [
            Paragraph("<b>Executed By</b>", main_table_header_bold_style),
            Paragraph("<b>Execution Start Date/Time</b>", main_table_header_bold_style),
            '',
            Paragraph("<b>Execution End Date/Time</b>", main_table_header_bold_style),
            '',
            Paragraph("<b>Pass/Fail/Not Applicable (NA)</b>", main_table_header_bold_style),
            ''
        ],
        [
            Paragraph("&lt;Add Tester Name Here&gt;", table_cell_style),
            Paragraph("&lt;Add Start Date/Time Here&gt;", table_cell_style),
            '',
            Paragraph("&lt;Add End Date/Time Here&gt;", table_cell_style),
            '',
            '',
            ''
        ],
        [
            Paragraph("<b>ID</b>", main_table_header_bold_style),
            Paragraph("<b>Description/Action</b>", main_table_header_bold_style),
            Paragraph("<b>Expected Result</b>", main_table_header_bold_style),
            Paragraph("<b>Actual Result</b>", main_table_header_bold_style),
            Paragraph("<b>Pass/Fail/NA</b>", main_table_header_bold_style),
            '',
            ''
        ]
    ]

    # Define steps with their content and validation_type for dynamic data fetching
    steps_data = [
        {
            "id": 1,
            "desc": "<b>Ingestion:</b> One time Data Extract and Load. Ingestion pipeline will connect with the provided Source and Target and execute.",
            "expected": ["Pipeline should be created and executed.",
                         "Last Run status should be displayed as 'Succeeded'."],
            "actual_type": "ingestion",
            "actual_content": ""
        },
        {
            "id": 2,
            "desc": "<b>Schema Validation:</b> Fetch the Source and Target schema and validate the Schema. Save the schema validation result.",
            "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                         "Source and Target Schema should be validated successfully.",
                         "Source and Target Data Type(s) should be validated successfully."],
            "actual_type": "schema",
            "actual_content_prefix": ""
        },
        {
            "id": 3,
            "desc": "<b>Data Validation:</b> Fetch the data from Source and Destination/Target nodes and validate the data. Save the Data Validation result.",
            "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                         "Source and Target Data should be validated successfully."],
            "actual_type": "data",
            "actual_content_prefix": ""
        },
        {
            "id": 4,
            "desc": "<b>Record Count Validation:</b> Fetch the record count from Source and Destination/Target and validate.",
            "expected": ["Mandatory fields should be filled and validation should be successfully completed."],
            "actual_type": "row_count",
            "actual_content_prefix": ""
        },
        {
            "id": 5,
            "desc": "<b>Uniqueness Validation:</b> Fetch the data from Target and check whether the mentioned column have unique values. Save the Uniqueness Validation result.",
            "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                         "Validate the Target unique column count"],
            "actual_type": "uniqueness",
            "actual_content_prefix": ""
        },
        {
            "id": 6,
            "desc": "<b>Not Null value Validation:</b> Fetch the data from Target and verify if any of the mentioned column have null values. Save the Not Null value Validation result.",
            "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                         "Validate the Target Not Null column count"],
            "actual_type": "not_null",
            "actual_content_prefix": ""
        }
    ]

    detailed_data_validation_comments_for_section = []

    # Populate main table data (excluding the header rows for now)
    main_table_body_data = []
    for item in steps_data:
        actual_result_content_parts = []  # Use a list to build content parts
        row_pass_fail_na = "NA"  # Default for 5th column

        overall_execution_status = get_status_for_validation_type(item["actual_type"], status_details)
        # Add Execution Status once here
        actual_result_content_parts.append(
            Paragraph(f"<b>Execution Status: {overall_execution_status}</b>", table_cell_style))
        actual_result_content_parts.append(Spacer(1, 6))  # Add a bit more space after Execution Status

        if item["id"] == 2:  # Schema Validation
            schemas_comment_content = None
            schema_comments_singular = []

            for row in validation_results:
                if row.validation_type == "schemas":
                    schemas_comment_content = row.comment
                elif row.validation_type == "schema":
                    schema_comments_singular.append(row.comment)  # Collect all 'schema' comments

            # Determine Pass/Fail for schema based on 'schema' comments (mismatch)
            if schema_comments_singular:
                row_pass_fail_na = "Fail"
            else:
                row_pass_fail_na = "Pass"  # If no 'schema' comments, it's a pass for schema validation

            if schemas_comment_content:
                # If 'schemas' comment exists, parse and use it
                source_schema_match = re.search(r"Source Schema:\s*(\[.*?\])", schemas_comment_content)
                target_schema_match = re.search(r"Target Schema:\s*(\[.*?\])", schemas_comment_content)

                if source_schema_match:
                    actual_result_content_parts.append(
                        Paragraph(f"<b>Source Schema:</b> {source_schema_match.group(1)}", table_cell_style))
                else:

                    actual_result_content_parts.append(Paragraph("<b>Source Schema:</b> N/A", table_cell_style))

                actual_result_content_parts.append(Spacer(1, 2))  # Small space between schema lines

                if target_schema_match:
                    actual_result_content_parts.append(
                        Paragraph(f"<b>Target Schema:</b> {target_schema_match.group(1)}", table_cell_style))
                else:
                    actual_result_content_parts.append(Paragraph("<b>Target Schema:</b> N/A", table_cell_style))

                # Add a line break if schema_comments_singular will follow
                if schema_comments_singular:  # Check if schema_comments_singular exists to add separator
                    actual_result_content_parts.append(
                        Paragraph("<b>Mismatch in schema:</b>", schema_mismatch_heading_style))
                    for comment in schema_comments_singular:
                        actual_result_content_parts.append(Paragraph(comment.strip(), schema_mismatch_comment_style))

            if not schemas_comment_content and not schema_comments_singular:
                if len(actual_result_content_parts) == 1:  # Only Execution Status is present
                    actual_result_content_parts.append(Paragraph("<b>Source Schema:</b> N/A", table_cell_style))
                    actual_result_content_parts.append(Spacer(1, 2))
                    actual_result_content_parts.append(Paragraph("<b>Target Schema:</b> N/A", table_cell_style))
                    row_pass_fail_na = "Pass"

        elif item["id"] == 1:  # Ingestion
            ingestion_comments = [row.comment for row in validation_results if row.validation_type == "ingestion"]
            if ingestion_comments:
                for comment in ingestion_comments:
                    actual_result_content_parts.append(Paragraph(comment.strip(), table_cell_style))
                row_pass_fail_na = "Pass"
            else:
                actual_result_content_parts.append(Paragraph("Pipeline Name : N/A", table_cell_style))
                actual_result_content_parts.append(Paragraph("Source: N/A", table_cell_style))
                actual_result_content_parts.append(Paragraph("Target: N/A", table_cell_style))
                row_pass_fail_na = "NA"

        elif item["id"] == 4:  # Record Count Validation
            comments_raw = [row.comment for row in validation_results if row.validation_type == item["actual_type"]]
            if comments_raw:
                comment = comments_raw[0]
                source_match = re.search(r"(?:Source Row Count:|Number of rows in Source:)\s*(\d+)", comment)
                target_match = re.search(r"Target:\s*(\d+)", comment)
                difference_match = re.search(r"Difference:\s*(\d+)", comment)

                source_val = source_match.group(1) if source_match else "N/A"
                target_val = target_match.group(1) if target_match else "N/A"
                difference_val = difference_match.group(1) if difference_match else "N/A"

                actual_result_content_parts.append(Paragraph(
                    f"<b>Number of rows in Source:</b> {source_val}<br/><b>Target:</b> {target_val}<br/><b>Difference:</b> {difference_val}",
                    table_cell_style))
                if difference_val == "0":
                    row_pass_fail_na = "Pass"
                else:
                    row_pass_fail_na = "Fail"
            else:
                actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                row_pass_fail_na = "NA"

        elif item["id"] == 5:  # Uniqueness Validation
            comments_raw = [row.comment for row in validation_results if row.validation_type == item["actual_type"]]
            if comments_raw:
                is_unique_overall = True
                for comment in comments_raw:
                    # Split by comma and then process each part
                    parts = comment.strip().split(', ')
                    formatted_parts = []
                    for part in parts:
                        # Bold the key before the colon, ensuring only the key is bolded
                        formatted_part = re.sub(r"([^:]+):(.*)", r"<b>\1</b>:\2", part)
                        formatted_parts.append(formatted_part)
                    actual_result_content_parts.append(Paragraph("<br/>".join(formatted_parts), table_cell_style))

                    if "not_unique" in comment.lower() or "fail" in comment.lower():
                        is_unique_overall = False

                if is_unique_overall:
                    row_pass_fail_na = "Pass"
                else:
                    row_pass_fail_na = "Fail"
            else:
                actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                row_pass_fail_na = "NA"

        elif item["id"] == 6:  # Not Null Validation
            comments_raw = [row.comment for row in validation_results if row.validation_type == item["actual_type"]]
            if comments_raw:
                is_no_nulls_overall = True
                for comment in comments_raw:
                    # Split by comma and then process each part
                    parts = comment.strip().split(', ')
                    formatted_parts = []
                    for part in parts:
                        formatted_part = re.sub(r"([^:]+):(.*)", r"<b>\1</b>:\2", part)
                        formatted_parts.append(formatted_part)
                    actual_result_content_parts.append(Paragraph("<br/>".join(formatted_parts), table_cell_style))

                    if ("nulls" in comment.lower() and "no nulls" not in comment.lower()) or "fail" in comment.lower():
                        is_no_nulls_overall = False

                if is_no_nulls_overall:
                    row_pass_fail_na = "Pass"
                else:
                    row_pass_fail_na = "Fail"
            else:
                actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                row_pass_fail_na = "NA"

        elif item["id"] == 3:  # Data Validation
            comments_raw = [row.comment for row in validation_results if row.validation_type == item["actual_type"]]

            source_only_count = 0
            destination_only_count = 0

            source_only_details = []
            destination_only_details = []

            for comment in comments_raw:
                parts = comment.split('||', 1)
                comment_type = parts[0]

                formatted_data_content = ""
                if len(parts) > 1:
                    json_or_string_part = parts[1]
                    try:
                        data_dict = json.loads(json_or_string_part)
                        if 'row_hash' in data_dict:
                            del data_dict['row_hash']
                        formatted_pairs = []
                        for k, v in data_dict.items():
                            formatted_pairs.append(f"<b>{k}</b>: {v}")
                        formatted_data_content = ", ".join(formatted_pairs)
                    except json.JSONDecodeError:
                        formatted_data_content = re.sub(r'\|\|[0-9a-fA-F]{64}$', '', json_or_string_part).strip()

                if "present_only_in_source" in comment_type:
                    source_only_count += 1
                    source_only_details.append(formatted_data_content)
                elif "present_only_in_destination" in comment_type:
                    destination_only_count += 1
                    destination_only_details.append(formatted_data_content)

            if source_only_count > 0 or destination_only_count > 0:
                row_pass_fail_na = "Fail"
            else:
                row_pass_fail_na = "Pass"

            if comments_raw:
                if len(comments_raw) <= 10:  # Print details inline (NO SUMMARY)
                    if source_only_details:
                        actual_result_content_parts.append(
                            Paragraph("<b>present_only_in_source:</b>", table_cell_style))
                        for detail in source_only_details:
                            actual_result_content_parts.append(Paragraph(detail, table_cell_style))
                        actual_result_content_parts.append(Spacer(1, 4))
                        actual_result_content_parts.append(
                            Paragraph("<b>present_only_in_destination:</b>", table_cell_style))
                        for detail in destination_only_details:
                            actual_result_content_parts.append(Paragraph(detail, table_cell_style))
                        actual_result_content_parts.append(Spacer(1, 4))

                else:
                    actual_result_content_parts.append(
                        Paragraph(f"Total discrepancies: {len(comments_raw)}", table_cell_style))
                    if source_only_count > 0:
                        actual_result_content_parts.append(
                            Paragraph(f"- Present only in Source: {source_only_count}", table_cell_style))
                    if destination_only_count > 0:
                        actual_result_content_parts.append(
                            Paragraph(f"- Present only in Destination: {destination_only_count}", table_cell_style))
                    actual_result_content_parts.append(Spacer(1, 6))
                    actual_result_content_parts.append(
                        Paragraph("<i>See 'Detailed Data Validation Logs' below for full list.</i>", table_cell_style))

                    if source_only_details:
                        detailed_data_validation_comments_for_section.append("<b>Present only in Source:</b>")
                        detailed_data_validation_comments_for_section.extend(source_only_details)
                    if destination_only_details:
                        detailed_data_validation_comments_for_section.append("<b>Present only in Destination:</b>")
                        detailed_data_validation_comments_for_section.extend(destination_only_details)
            else:

                actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                row_pass_fail_na = "NA"

        if overall_execution_status == "Fail" or overall_execution_status == "NA":
            row_pass_fail_na = "NA"

        main_table_body_data.append([
            Paragraph(str(item["id"]), table_cell_style),
            Paragraph(item["desc"], table_cell_style),
            # Process expected results to use list_item_style
            [Paragraph(f"{idx + 1}. {expected_item}", list_item_style) for idx, expected_item in
             enumerate(item["expected"])],
            actual_result_content_parts,  # Pass the list of flowables directly
            Paragraph(row_pass_fail_na, table_cell_style)  # Populated 5th column with calculated status
        ])

    # Combine header and body data
    full_main_table_data = main_table_header_data + main_table_body_data

    # Create the main table
    main_table = Table(full_main_table_data, colWidths=[80, 120, 120, 120, 90, 0, 0])  # Adjusted column widths

    main_table.setStyle(TableStyle([
        # Header rows styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),  # First header row
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor("#F0F0F0")),  # Second header row
        ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor("#E0E0E0")),
        # Third header row (ID, Desc, Expected, Actual, Pass/Fail)

        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('FONTNAME', (0, 0), (-1, 2), 'Helvetica-Bold'),  # Ensure header text is bold
        ('FONTSIZE', (0, 0), (-1, -1), 10),  # Increased font size for header text
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        # Spanning cells for the first two header rows to match the image
        ('SPAN', (0, 0), (0, 1)),  # "Executed By" spans down (Col 0, Row 0 to Col 0, Row 1)
        ('SPAN', (1, 0), (2, 0)),  # "Execution Start Date/Time" spans right (Col 1, Row 0 to Col 2, Row 0)
        ('SPAN', (3, 0), (4, 0)),  # "Execution End Date/Time" spans right (Col 3, Row 0 to Col 4, Row 0)
        ('SPAN', (5, 0), (6, 0)),  # "Pass/Fail/Not Applicable (NA)" spans right (Col 5, Row 0 to Col 6, Row 0)
        ('SPAN', (1, 1), (2, 1)),  # "<Add Start Date/Time Here>" spans right (Col 1, Row 1 to Col 2, Row 1)
        ('SPAN', (3, 1), (4, 1)),  # "<Add End Date/Time Here>" spans right (Col 3, Row 1 to Col 4, Row 1)
        ('SPAN', (5, 1), (6, 1)),  # Empty cell below Pass/Fail/NA spans right (Col 5, Row 1 to Col 6, Row 1)
        # Aligning the text within the spanned cells
        ('ALIGN', (0, 0), (0, 1), 'CENTER'),  # Center "Executed By" and its placeholder
        ('ALIGN', (1, 0), (2, 0), 'CENTER'),  # Center "Execution Start Date/Time"
        ('ALIGN', (3, 0), (4, 0), 'CENTER'),  # Center "Execution End Date/Time"
        ('ALIGN', (5, 0), (6, 0), 'CENTER'),  # Center "Pass/Fail/Not Applicable (NA)"
        # Specific alignment for the placeholders in the second header row
        ('ALIGN', (1, 1), (2, 1), 'CENTER'),  # Center "<Add Start Date/Time Here>"
        ('ALIGN', (3, 1), (4, 1), 'CENTER'),  # Center "<Add End Date/Time Here>"
        # Align ID, Description/Action, Expected Result, Actual Result, Pass/Fail/NA headers to center
        ('ALIGN', (0, 2), (-1, 2), 'CENTER'),
    ]))
    story.append(main_table)
    story.append(Spacer(1, 20))

    # --------- Detailed Data Validation Logs Section ---------
    # This section is only added if there are more than 10 discrepancies for Data Validation
    if detailed_data_validation_comments_for_section:
        story.append(PageBreak())  # Start this section on a new page if needed
        story.append(Paragraph("<b>Detailed Data Validation Logs</b>", detailed_log_heading_style))
        story.append(Spacer(1, 10))
        # Iterate through the collected detailed comments for the separate section
        for comment in detailed_data_validation_comments_for_section:
            story.append(Paragraph(comment, detailed_log_content_style))
            story.append(Spacer(1, 2))  # Small space between detailed comments

    # --------- Conclusion ---------
    story.append(Paragraph("This concludes the execution of this test.", conclusion_style))

    # --------- Footer with Page Number ---------
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 8)
        canvas.drawString(30, 30, f"Page {doc.page}")
        canvas.line(30, 40, doc.width + 30, 40)
        canvas.restoreState()

    # -------------------- Build & Save PDF --------------------
    try:
        doc.build(story, onFirstPage=footer, onLaterPages=footer)
        print(f"PDF report saved at: {pdf_path}")
    except Exception as e:
        print(f"Error building PDF for {current_pipeline_name}: {e}")

spark = None
try:
    spark = SparkSession.builder \
        .appName("ValidationReportGenerator") \
        .getOrCreate()
        # .config("spark.jars", JDBC_JAR_PATH) \
    print("SparkSession initialized successfully.")
except Exception as e:
    print(f"ERROR: Could not initialize SparkSession. This script requires a Spark environment to load data.")
    print(f"Error details: {e}")

    exit(1)

kosh_credential_id = int(spark.conf.get("spark.nabu.kosh_credential_id"))
kosh_credential_type_id = int(spark.conf.get("spark.nabu.kosh_credential_type_id"))
token = spark.conf.get("spark.nabu.token")
credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")

kosh_username, kosh_password = fetch_credentials(kosh_credential_id, kosh_credential_type_id, key_map=('username', 'password'))

PIPELINE_DB_URL = "jdbc:postgresql://w3.devpsql.modak.com:5432/nabu_v3"
PIPELINE_DB_USERNAME = kosh_username
PIPELINE_DB_PASSWORD = kosh_password
JDBC_DRIVER = "org.postgresql.Driver"

# JDBC_JAR_PATH = "/home/ab2012/Desktop/PythonProject/PythonProject/postgresql-42.7.6.jar"

# Report Directory
NEW_REPORT_DIR = "/home/spark-srv-account/ab2012/pipeline_reports_new"  # New folder for saving PDFs
os.makedirs(NEW_REPORT_DIR, exist_ok=True)


# Initialize SparkSession once
custom_code_batch_id = 167376462025880870

# Step 1: Get dataflow_id and dataflow_batch_id
workflow_query = f"""
    SELECT workflow_orch_id, batch_id
    FROM nabu.checkpoint_workflow_node_status
    WHERE node_batch_id = {custom_code_batch_id}
"""

workflow_df = spark.read.format("jdbc").options(
    url=PIPELINE_DB_URL,
    query=workflow_query,
    user=PIPELINE_DB_USERNAME,
    password=PIPELINE_DB_PASSWORD,
    driver=JDBC_DRIVER
).load()

workflow_row = workflow_df.first()
if workflow_row:
    dataflow_id = workflow_row['workflow_orch_id']
    dataflow_batch_id = str(workflow_row['batch_id'])  # ✅ cast to str to avoid scientific notation
    print(f"✅ Dataflow ID: {dataflow_id}")
    print(f"✅ Dataflow Batch ID: {dataflow_batch_id}")
else:
    print("⚠️ No records found for the given node_batch_id")
    spark.stop()
    exit(0)

# Step 2: Get execution metadata
exec_meta_query = f"""
    SELECT cru_by, valid_from_ts
    FROM nabu.init_flow_status AS ifs
    JOIN nabu.job_schedule_details AS jsd
    ON ifs.job_schedule_id = jsd.job_schedule_id
    WHERE batch_id = {dataflow_batch_id} AND status_code_id = 12
"""

exec_df = spark.read.format("jdbc").options(
    url=PIPELINE_DB_URL,
    query=exec_meta_query,
    user=PIPELINE_DB_USERNAME,
    password=PIPELINE_DB_PASSWORD,
    driver=JDBC_DRIVER
).load()

exec_row = exec_df.first()
if exec_row:
    print(f"👤 Executed By    : {exec_row['cru_by']}")
    print(f"🕒 Execution Time : {exec_row['valid_from_ts']}")
else:
    print("⚠️ No execution metadata found.")

# Step 3: Fetch node status from orchestration
nodes_query = f"""
    SELECT 
        node_data.node_name,
        node_data.pipeline_name,
        node_data.pipeline_id::text,
        status.node_batch_id::text AS node_batch_id,
        status.status
    FROM (
        SELECT 
            node -> 'config' ->> 'name' AS node_name,
            COALESCE(
                node -> 'config' ->> 'data_movement_name',
                node -> 'config' ->> 'workflow_orch_name'
            ) AS pipeline_name,
            COALESCE(
                node -> 'config' ->> 'data_movement_id',
                node -> 'config' ->> 'workflow_orch_id'
            ) AS pipeline_id,
            node ->> 'nodeId' AS node_id
        FROM nabu.workflow_orchestration,
             jsonb_array_elements(workflow_orch_json::jsonb -> 'workflowJson' -> 'nodeDataArray') AS node
        WHERE workflow_orch_id = {dataflow_id}
    ) AS node_data
    LEFT JOIN nabu.checkpoint_workflow_node_status AS status
        ON node_data.node_id::bigint = status.node_id
       AND status.batch_id = {dataflow_batch_id}
"""

nodes_df = spark.read.format("jdbc").options(
    url=PIPELINE_DB_URL,
    query=nodes_query,
    user=PIPELINE_DB_USERNAME,
    password=PIPELINE_DB_PASSWORD,
    driver=JDBC_DRIVER
).load()

node_details = {
    row['node_name']: {
        "pipeline_name": row['pipeline_name'],
        "pipeline_id": row['pipeline_id'],
        "node_batch_id": row['node_batch_id'],
        "status": row['status']
    }
    for row in nodes_df.collect() if row['node_name']
}

print("\n✅ Node Details:")
print(node_details)

# Optional: Process validation flow
if node_details.get("Ingestion pipeline"):
    ingestion_status = node_details["Ingestion pipeline"]["status"]
    print(f"\n📦 Ingestion pipeline status: {ingestion_status}")

    if ingestion_status is None or ingestion_status == "SUCCESS":
        validation_dataflow_id = node_details["Validation dataflow"]["pipeline_id"]
        validation_dataflow_batch_id = node_details["Validation dataflow"]["node_batch_id"]

        print(f"🔍 Validation Dataflow ID     : {validation_dataflow_id}")
        print(f"🔍 Validation Dataflow BatchID: {validation_dataflow_batch_id}")

        validation_query = f"""
            SELECT 
                node_data.node_name,
                node_data.pipeline_name,
                node_data.pipeline_id,
                status.node_batch_id,
                status.status
            FROM (
                SELECT 
                    node -> 'config' ->> 'name' AS node_name,
                    COALESCE(
                        node -> 'config' ->> 'data_movement_name',
                        node -> 'config' ->> 'workflow_orch_name'
                    ) AS pipeline_name,
                    COALESCE(
                        node -> 'config' ->> 'data_movement_id',
                        node -> 'config' ->> 'workflow_orch_id'
                    ) AS pipeline_id,
                    node ->> 'nodeId' AS node_id
                FROM nabu.workflow_orchestration,
                     jsonb_array_elements(workflow_orch_json::jsonb -> 'workflowJson' -> 'nodeDataArray') AS node
                WHERE workflow_orch_id = {validation_dataflow_id}
            ) AS node_data
            LEFT JOIN nabu.checkpoint_workflow_node_status AS status
                ON node_data.node_id::bigint = status.node_id
               AND status.batch_id = {validation_dataflow_batch_id}
            WHERE node_data.node_name NOT IN ('Modify validation pipelines', 'Target crawler')
        """

        pipelines_to_process_df = spark.read.format("jdbc").options(
            url=PIPELINE_DB_URL,
            query=validation_query,
            user=PIPELINE_DB_USERNAME,
            password=PIPELINE_DB_PASSWORD,
            driver=JDBC_DRIVER
        ).load()

        pipelines_to_process = [{
            "pipeline_name": row["pipeline_name"],
            "pipeline_id": row["pipeline_id"],
            "pipeline_batch_id": str(row["node_batch_id"]),
            "status": row["status"]
        } for row in pipelines_to_process_df.collect()]

print("pipelines_to_proccess:" , pipelines_to_process)

for pipeline in pipelines_to_process:
    p_name = pipeline['pipeline_name']
    p_id = pipeline['pipeline_id']
    # Ensure batch_id is converted to string for consistency with SQL CAST
    b_id = str(pipeline['pipeline_batch_id'])
    p_status = pipeline['status']  # This can be used for logging or conditional logic if needed

    print(f"\n--- Generating report for Pipeline: {p_name} (ID: {p_id}, Batch ID: {b_id}) ---")
    # Call the function for each pipeline (currently just one in the list)
    generate_pipeline_report(spark, p_id, b_id, p_name)
    print(f"--- Finished report for Pipeline: {p_name} ---")

# Stop SparkSession after all reports are generated
if spark:
    spark.stop()
print("All reports generated. Script finished.")
