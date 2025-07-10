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
import csv  # Import the csv module
from email.message import EmailMessage
import smtplib

# Set up logging to see debug messages
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

# --- Configuration (Provided by User) ---
# These values are now hardcoded as per the user's explicit instruction



# The fetch_credentials function is kept as it was in the user's provided updated.txt,
# although its direct use for DB credentials is now bypassed by the hardcoded values.
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



# -------------------- HELPER FUNCTIONS --------------------


def send_all_pdfs_via_outlook_smtp(
    sender_email, sender_password, receiver_email, subject, body, folder_path
):
    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.set_content(body)

    # Attach all PDFs in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(folder_path, filename)
            with open(pdf_path, "rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype="application",
                    subtype="pdf",
                    filename=filename
                )

    # Send email via Outlook SMTP
    with smtplib.SMTP("smtp.office365.com", 587) as smtp:
        smtp.starttls()
        smtp.login(sender_email, sender_password)
        smtp.send_message(msg)


def generate_ingestion_failed_report(executed_by_name, execution_start_time, execution_end_time):
    styles = getSampleStyleSheet()

    all_reports_story_elements=[]

    table_cell_center_style = ParagraphStyle(name='TableCellCenterStyle', parent=styles['BodyText'], fontSize=7,
                                             alignment=TA_CENTER)

    main_table_header_bold_style = ParagraphStyle(name='MainTableHeaderBoldStyle', parent=styles['Normal'],
                                                  fontSize=10, alignment=TA_CENTER, fontName='Helvetica-Bold')
    main_table_header_data = [
        # Row 0: "Test Execution" spanning all 5 columns
        [Paragraph("<b>Test Execution</b>", main_table_header_bold_style), '', '', '', ''],
        # New Row 1: "Executed By" label and value, centered across the entire row
        [Paragraph(f"<b>Executed By:</b> {executed_by_name}", main_table_header_bold_style), '', '', '', ''],
        # Span will handle centering
        # Original Row 1 (now Row 2): Headers for Start Date/Time, End Date/Time (only 2 columns)
        [Paragraph("<b>Execution Start Date/Time</b>", main_table_header_bold_style),
         '',  # Placeholder for span
         '',  # Placeholder for span
         Paragraph("<b>Execution End Date/Time</b>", main_table_header_bold_style),
         ''],  # Placeholder for span
        # Original Row 2 (now Row 3): Actual values for Start Date/Time, End Date/Time (only 2 columns)
        [Paragraph(execution_start_time, table_cell_center_style),
         '',  # Placeholder for span
         '',  # Placeholder for span
         Paragraph(execution_end_time, table_cell_center_style),
         ''],  # Placeholder for span
        # Original Row 3 (now Row 4): ID, Description/Action, Expected Result, Actual Result, Pass/Fail/NA (5 columns)
        [Paragraph("<b>ID</b>", main_table_header_bold_style),
         Paragraph("<b>Description/Action</b>", main_table_header_bold_style),
         Paragraph("<b>Expected Result</b>", main_table_header_bold_style),
         Paragraph("<b>Actual Result</b>", main_table_header_bold_style),
         Paragraph("<b>Pass/Fail/NA</b>", main_table_header_bold_style)]
    ]

    styles = getSampleStyleSheet()

    list_item_style = ParagraphStyle(name='ListItemStyle', parent=styles['BodyText'],
                                     fontSize=7, alignment=TA_LEFT,
                                     leftIndent=12,
                                     firstLineIndent=0,
                                     spaceBefore=0, spaceAfter=1)

    table_cell_style = ParagraphStyle(name='TableCellStyle', parent=styles['BodyText'], fontSize=7, alignment=TA_LEFT,
                                      spaceAfter=0)
    steps_data = [
        {
            "id": 1,
            "desc": "<b>Ingestion:</b> One time Data Extract and Load. Ingestion pipeline will connect with the provided Source and Target and execute.",
            "expected": ["Pipeline should be created and executed.",
                         "Last Run status should be displayed as 'Succeeded'."],
            "actual_type": "ingestion",
            "actual_content": ""
        }
    ]

    main_table_body_data = []

    item = steps_data[0]  # Ingestion is always the first item (ID 1)
    actual_result_content_parts = []
    row_pass_fail_na = "Fail"  # Explicitly set to Fail for failed ingestion

    actual_result_content_parts.append(
        Paragraph(f"<b>Execution Status: {ingestion_status}</b>", table_cell_style))
    actual_result_content_parts.append(Spacer(1, 2))
    actual_result_content_parts.append(Paragraph(f"<b>Pipeline Name:</b> {ingestion_pipeline_name}", table_cell_style))
    actual_result_content_parts.append(Spacer(1, 2))

    main_table_body_data.append([
        Paragraph(str(item["id"]), table_cell_style),
        Paragraph(item["desc"], table_cell_style),
        [Paragraph(f"{i}. {text}", list_item_style) for i, text in enumerate(item['expected'], 1)],
        actual_result_content_parts,
        Paragraph(row_pass_fail_na, table_cell_style)
    ])

    full_table_data = main_table_header_data + main_table_body_data

    main_table = Table(full_table_data, colWidths=[40, 120, 150, 190, 60])

    # MODIFIED: Re-structured TableStyle for better header alignment and new row
    main_table.setStyle(TableStyle([
        # General Grid/Padding
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),

        # Header Rows Background
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),  # "Test Execution" row (Row 0)
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor("#E0F2F7")),  # New "Executed By" row (Row 1)
        ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor("#D9E1F2")),  # Headers for Start/End Date/Time (Row 2)
        ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor("#E0F2F7")),  # Actual values for dates/times (Row 3)
        ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor("#D9E1F2")),  # "ID", "Description", etc. headers (Row 4)

        # Header Structure (Spans)
        ('SPAN', (0, 0), (4, 0)),  # Span "Test Execution" across all 5 columns (Row 0)
        # MODIFIED: Span for "Executed By" row to cover all 5 columns, centered
        ('SPAN', (0, 1), (4, 1)),  # Span the entire "Executed By" row across all 5 columns

        # MODIFIED: For Row 2 (Execution Start/End Date/Time headers)
        ('SPAN', (0, 2), (2, 2)),  # Span the first 3 columns for "Execution Start Date/Time" header
        ('SPAN', (3, 2), (4, 2)),  # Span the last 2 columns for "Execution End Date/Time" header

        # MODIFIED: For Row 3 (Execution Start/End Date/Time values)
        ('SPAN', (0, 3), (2, 3)),  # Span the first 3 columns for start time value
        ('SPAN', (3, 3), (4, 3)),  # Span the last 2 columns for end time value

        # Row 4 ("ID", "Description", etc. headers) - remains 5 columns
        ('ALIGN', (0, 4), (4, 4), 'CENTER'),  # All cells in this row should be centered
        ('VALIGN', (0, 4), (4, 4), 'MIDDLE'),

        # Header Alignment - Explicitly set per cell/span for precision
        ('ALIGN', (0, 0), (4, 0), 'CENTER'),  # "Test Execution" title (Row 0)
        ('VALIGN', (0, 0), (4, 0), 'MIDDLE'),

        # MODIFIED: Alignment for new "Executed By" row (Row 1)
        ('ALIGN', (0, 1), (4, 1), 'CENTER'),  # "Executed By:" and name should be centered within their span
        ('VALIGN', (0, 1), (4, 1), 'MIDDLE'),

        # Row 2 (Headers for Start/End Date/Time)
        ('ALIGN', (0, 2), (2, 2), 'CENTER'),  # "Execution Start Date/Time" header
        ('ALIGN', (3, 2), (4, 2), 'CENTER'),  # "Execution End Date/Time" header
        ('VALIGN', (0, 2), (4, 2), 'MIDDLE'),  # Vertical align for the whole row

        # MODIFIED: Explicitly setting alignment for Row 3 (Actual values for Start/End Date/Time)
        ('ALIGN', (0, 3), (2, 3), 'CENTER'),  # Actual 'Execution Start Date/Time' value
        ('ALIGN', (3, 3), (4, 3), 'CENTER'),  # Actual 'Execution End Date/Time' value
        ('VALIGN', (0, 3), (4, 3), 'MIDDLE'),  # Vertical align for the whole row

        # Body Alignment (defaults to TOP/LEFT via Paragraph styles, but set VALIGN for rows)
        # The row index for body data starts from 5 (0-4 are headers)
        # This style applies to the rows that are actually added to final_table_body_data
        ('VALIGN', (0, 5), (-1, -1), 'TOP'),  # Vertical align top for body rows
    ]))

    report_dir = os.path.join(NEW_REPORT_DIR, dataflow_batch_id)
    os.makedirs(report_dir, exist_ok=True)  # ✅ Creates directory if not exists

    # Final PDF path
    combined_pdf_path = os.path.join(report_dir, "validation_report.pdf")
    doc = SimpleDocTemplate(combined_pdf_path, pagesize=A4, topMargin=40, bottomMargin=40, leftMargin=30,
                            rightMargin=30)

    all_reports_story_elements.append(main_table)
    all_reports_story_elements.append(Spacer(1, 12))

    try:
        doc.build(all_reports_story_elements, onFirstPage=footer, onLaterPages=footer)
        logging.info(f"Combined PDF report saved at: {combined_pdf_path}")
    except Exception as e:
        logging.error(f"Error building combined PDF: {e}")


def get_status_for_validation_type(v_type, status_details):
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

    if "FAILED" in relevant_statuses:
        return "Fail"
    elif "SUCCEEDED" in relevant_statuses:
        return "Success"
    else:
        return "NA"


# --- PDF Footer Function ---
def footer(canvas, doc):
    """
    Adds a page number and a line to the footer of each PDF page.
    """
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    # Adjusted Y position for page number to be slightly above the line
    canvas.drawString(30, 38, f"Page {doc.page}")
    canvas.line(30, 30, doc.width + 30, 30)  # Keep line at 30, so text is above it.
    canvas.restoreState()


# --- Removed generate_schema_report_pdf function as per user request ---
# The schema details will now be printed inline in the main report.


# --- New Function to Generate Separate Data Validation PDF ---
def generate_data_validation_pdf(pipeline_name, pipeline_id, batch_id, discrepancies, report_dir):
    """
    Generates a separate PDF report specifically for Data Validation discrepancies.
    This function is called by generate_pipeline_report when data validation discrepancies
    exceed a certain threshold.
    """
    # Get absolute path for the directory
    absolute_report_dir = os.path.abspath(report_dir)
    # Ensure the directory exists
    os.makedirs(absolute_report_dir, exist_ok=True)

    # Sanitize pipeline_name for filename, removing characters that might cause issues
    sanitized_pipeline_name = re.sub(r'[\\/:*?"<>|]', '', pipeline_name.replace(' ', '_'))
    pdf_filename = f"Data_Validation_Report_{sanitized_pipeline_name}_Batch_{batch_id}.pdf"
    report_dir = os.path.join(NEW_REPORT_DIR, dataflow_batch_id)
    data_validation_pdf_full_path = os.path.join(report_dir, pdf_filename)

    # Use a file stream to ensure explicit closing
    try:
        with open(data_validation_pdf_full_path, 'wb') as f:
            # Attempt a dummy write and flush to ensure file handle is writable
            try:
                f.write(b' ')  # Write a single byte
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
                f.seek(0)  # Reset file pointer to beginning
                logging.debug(f"Successfully performed dummy write to {data_validation_pdf_full_path}")
            except Exception as e:
                logging.error(f"Error during dummy write to {data_validation_pdf_full_path}: {e}", exc_info=True)
                return None

            doc = SimpleDocTemplate(f, pagesize=A4, topMargin=40, bottomMargin=40, leftMargin=30, rightMargin=30)
            story = []

            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(name='TitleStyle', parent=styles['Heading1'], fontSize=14, alignment=TA_CENTER,
                                         spaceAfter=12)
            heading_style = ParagraphStyle(name='Heading2Style', parent=styles['Heading2'], fontSize=11,
                                           alignment=TA_LEFT, spaceAfter=8)
            content_style = ParagraphStyle(name='ContentStyle', parent=styles['Normal'], fontSize=9, alignment=TA_LEFT,
                                           spaceAfter=4)
            discrepancy_item_style = ParagraphStyle(name='DiscrepancyItemStyle', parent=styles['Normal'], fontSize=8,
                                                    alignment=TA_LEFT, leftIndent=20, spaceAfter=2)
            sub_heading_style = ParagraphStyle(name='SubHeadingStyle', parent=styles['Heading3'], fontSize=10,
                                               alignment=TA_LEFT, spaceAfter=6,
                                               spaceBefore=10)  # New style for sub-headers

            story.append(Paragraph(f"<b>Data Validation Report for:</b> {pipeline_name}", title_style))
            story.append(Paragraph(f"<b>Pipeline ID:</b> {pipeline_id}", content_style))
            story.append(Paragraph(f"<b>Batch ID:</b> {batch_id}", content_style))
            story.append(Spacer(1, 12))

            logging.debug(f"Discrepancies received for PDF generation: {discrepancies}")

            if discrepancies:
                source_only_count = sum(1 for d in discrepancies if d['type'] == 'present_only_in_source')
                destination_only_count = sum(1 for d in discrepancies if d['type'] == 'present_only_in_destination')

                story.append(Paragraph(f"<b>Total Discrepancies:</b> {len(discrepancies)}", heading_style))
                if source_only_count > 0:
                    story.append(Paragraph(f"<b>- Present only in Source:</b> {source_only_count}", content_style))
                if destination_only_count > 0:
                    story.append(
                        Paragraph(f"<b>- Present only in Destination:</b> {destination_only_count}", content_style))
                story.append(Spacer(1, 10))

                story.append(Paragraph("<b>Detailed Discrepancies:</b>", heading_style))

                # Group and display "Present Only In Source" discrepancies
                source_discrepancies = [d for d in discrepancies if d['type'] == 'present_only_in_source']
                if source_discrepancies:
                    story.append(Paragraph("<b>Present Only In Source:</b>", sub_heading_style))
                    for disc in source_discrepancies:
                        formatted_data_content = disc['data']
                        try:
                            data_dict = json.loads(disc['data'])
                            if 'row_hash' in data_dict:
                                del data_dict['row_hash']
                            formatted_pairs = [f"<b>{k}</b>: {v}" for k, v in data_dict.items()]
                            formatted_data_content = ", ".join(formatted_pairs)
                        except json.JSONDecodeError:
                            formatted_data_content = re.sub(r'\|\|[0-9a-fA-F]{64}$', '', disc['data']).strip()
                        story.append(Paragraph(f"<b>Data:</b> {formatted_data_content}", discrepancy_item_style))
                        story.append(Spacer(1, 5))
                    story.append(Spacer(1, 10))  # Add space after this section

                # Group and display "Present Only In Destination" discrepancies
                destination_discrepancies = [d for d in discrepancies if d['type'] == 'present_only_in_destination']
                if destination_discrepancies:
                    story.append(Paragraph("<b>Present Only In Destination:</b>", sub_heading_style))
                    for disc in destination_discrepancies:
                        formatted_data_content = disc['data']
                        try:
                            data_dict = json.loads(disc['data'])
                            if 'row_hash' in data_dict:
                                del data_dict['row_hash']
                            formatted_pairs = [f"<b>{k}</b>: {v}" for k, v in data_dict.items()]
                            formatted_data_content = ", ".join(formatted_pairs)
                        except json.JSONDecodeError:
                            formatted_data_content = re.sub(r'\|\|[0-9a-fA-F]{64}$', '', disc['data']).strip()
                        story.append(Paragraph(f"<b>Data:</b> {formatted_data_content}", discrepancy_item_style))
                        story.append(Spacer(1, 5))
                    story.append(Spacer(1, 10))  # Add space after this section

            else:
                story.append(Paragraph("<b>No Data Discrepancies Found.</b>", heading_style))
                story.append(Spacer(1, 8))

            logging.debug(f"Story content before building PDF (length: {len(story)}): {story}")  # Log story length
            if not story:  # Added check for empty story
                logging.warning(
                    f"Story is empty for Data Validation PDF: {data_validation_pdf_full_path}. Skipping PDF build.")
                return None

            doc.build(story, onFirstPage=footer, onLaterPages=footer)
            logging.info(f"Data Validation PDF report saved at: {data_validation_pdf_full_path}")

            # Verify file existence after building
            if os.path.exists(data_validation_pdf_full_path):
                logging.info(f"Confirmed: Data Validation PDF file exists at: {data_validation_pdf_full_path}")
                # Check the size of the file to ensure it's not 0KB
                file_size = os.path.getsize(data_validation_pdf_full_path)
                if file_size == 0:
                    logging.error(
                        f"Error: Data Validation PDF file is 0KB at: {data_validation_pdf_full_path}. Content might not have been written.")
                    return None
                return data_validation_pdf_full_path
            else:
                logging.error(
                    f"Error: Data Validation PDF file was NOT found at: {data_validation_pdf_full_path} after build attempt.")
                return None
    except Exception as e:
        logging.error(f"Error building Data Validation PDF for {pipeline_name}: {e}",
                      exc_info=True)  # Added exc_info=True for full traceback
        return None


# -------------------- PDF GENERATION FUNCTION (MODIFIED TO RETURN ELEMENTS) --------------------

def generate_pipeline_report(spark_session, current_pipeline_id, current_batch_id, current_pipeline_name,
                             executed_by_name, execution_start_time, execution_end_time,
                             source_obj_name, target_obj_name, pipeline_status_from_list, pipeline_ui_url, ingestion_failed):
    """
    Generates a list of ReportLab Flowables for a single pipeline report.
    This function no longer builds the PDF directly.
    It now accepts source/target names and a flag to control validation details.
    """
    story_elements = []  # This list will hold all flowables for this pipeline's report
    data_validation_pdf_path = None
    try:
        # Determine if ingestion failed for conditional rendering. Convert to uppercase for case-insensitivity.

        process_validation_details = (pipeline_status_from_list.upper() == "SUCCESS")

        # SQL Queries - using function arguments
        # Ensure current_pipeline_id and current_batch_id are safely cast to BIGINT in SQL
        # The queries will only be executed if current_batch_id is not 'None' (string)
        VALIDATION_SQL = f"""
        SELECT validation_type, comment, batch_id FROM validation_result
        WHERE batch_id = CAST('{current_batch_id}' AS BIGINT)
        ORDER BY validation_type
        """

        # Updated STATUS_DETAILS_SQL to match the more comprehensive version from updated.txt
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
      AND data_movement_id = CAST('{current_pipeline_id}' AS BIGINT)
      AND batch_id = CAST('{current_batch_id}' AS BIGINT)
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
  AND a.workspace_pipeline_id = CAST('{current_pipeline_id}' AS BIGINT)
  AND b.batch_id = CAST('{current_batch_id}' AS BIGINT)
WHERE a.workspace_pipeline_id = CAST('{current_pipeline_id}' AS BIGINT)
        """

        # -------------------- LOAD DATA FROM DATABASE --------------------
        logging.info(f"Executing SQL queries for {current_pipeline_name} (Batch ID: {current_batch_id})...")

        validation_results = []
        status_details = []

        # Check if batch_id is valid before attempting to load validation/status data
        is_batch_id_valid = current_batch_id and current_batch_id.lower() != 'none'

        # Only load validation and status details if process_validation_details is True AND batch_id is valid
        # OR if it's the ingestion pipeline and we need to fetch its status details
        if spark_session and (
                process_validation_details ):  # Added condition for ingestion
            try:
                validation_df = spark_session.read.format("jdbc").options(
                    url=PIPELINE_DB_URL,
                    query=VALIDATION_SQL,
                    user=PIPELINE_DB_USERNAME,  # Corrected variable name
                    password=PIPELINE_DB_PASSWORD,
                    driver=JDBC_DRIVER
                ).load()
                validation_results = validation_df.collect()

                if validation_results:
                    logging.info(f"Successfully loaded validation data for {current_pipeline_name}.")
                else:
                    logging.info(f"No validation data found for BATCH_ID: {current_batch_id}.")
                    validation_results = []
            except Exception as e:
                logging.error(f"Error loading validation data for {current_pipeline_name}: {e}")
                validation_results = []  # Ensure it's an empty list on error

            try:
                status_details_df = spark_session.read.format("jdbc").options(
                    url=PIPELINE_DB_URL,
                    query=STATUS_DETAILS_SQL,
                    user=PIPELINE_DB_USERNAME,  # Corrected variable name
                    password=PIPELINE_DB_PASSWORD,
                    driver=JDBC_DRIVER
                ).load()
                status_details = status_details_df.collect()

                if status_details:
                    logging.info(f"Successfully loaded status details data for {current_pipeline_name}.")
                else:
                    logging.info(f"No status details found for PIPELINE_ID: {current_pipeline_id}.")
                    status_details = []
            except Exception as e:
                logging.error(f"Error loading status data for {current_pipeline_name}: {e}")
                status_details = []  # Ensure it's an empty list on error
        else:
            logging.info(
                f"Skipping validation and status details loading for {current_pipeline_name} due to invalid batch ID or process_validation_details flag.")

        # -------------------- PDF GENERATION ELEMENTS --------------------

        # Define styles for the PDF document
        styles = getSampleStyleSheet()

        main_table_header_bold_style = ParagraphStyle(name='MainTableHeaderBoldStyle', parent=styles['Normal'],
                                                      fontSize=10, alignment=TA_CENTER, fontName='Helvetica-Bold')

        # Ensured no significant spaceAfter for table_cell_style, as nested table will control it
        table_cell_style = ParagraphStyle(name='TableCellStyle', parent=styles['BodyText'], fontSize=7,
                                          alignment=TA_LEFT,
                                          spaceAfter=0)
        # Reverting table_cell_center_style to just center alignment without bold
        table_cell_center_style = ParagraphStyle(name='TableCellCenterStyle', parent=styles['BodyText'], fontSize=7,
                                                 alignment=TA_CENTER)

        # MODIFIED: Adjusted spaceBefore/spaceAfter for better spacing in schema validation
        schema_mismatch_heading_style = ParagraphStyle(name='SchemaMismatchHeadingStyle', parent=styles['Normal'],
                                                       fontSize=7, alignment=TA_LEFT, fontName='Helvetica-Bold',
                                                       spaceBefore=4, spaceAfter=2)  # Slight adjustment from previous
        schema_mismatch_comment_style = ParagraphStyle(name='SchemaMismatchCommentStyle', parent=styles['BodyText'],
                                                       fontSize=7, alignment=TA_LEFT, spaceBefore=0,
                                                       spaceAfter=2)  # Added spaceAfter

        list_item_style = ParagraphStyle(name='ListItemStyle', parent=styles['BodyText'],
                                         fontSize=7, alignment=TA_LEFT,
                                         leftIndent=12,
                                         firstLineIndent=0,
                                         spaceBefore=0, spaceAfter=1)

        detailed_log_heading_style = ParagraphStyle(name='DetailedLogHeadingStyle', parent=styles['Heading2'],
                                                    fontSize=12,
                                                    alignment=TA_LEFT, spaceAfter=10)
        detailed_log_content_style = ParagraphStyle(name='DetailedLogContentStyle', parent=styles['BodyText'],
                                                    fontSize=7,
                                                    alignment=TA_LEFT, spaceAfter=2)

        title_style = ParagraphStyle(name='TitleStyle', parent=styles['Heading1'], fontSize=12, alignment=TA_CENTER,
                                     spaceAfter=12)
        header_info_style = ParagraphStyle(name='HeaderInfoStyle', parent=styles['Normal'], fontSize=10,
                                           alignment=TA_LEFT,
                                           spaceAfter=4)
        table_header_style = ParagraphStyle(name='TableHeaderStyle', parent=styles['Normal'], fontSize=9,
                                            alignment=TA_CENTER, fontName='Helvetica-Bold')
        table_cell_bold_style = ParagraphStyle(name='TableCellBoldStyle', parent=styles['BodyText'], fontSize=8,
                                               alignment=TA_LEFT, fontName='Helvetica-Bold', spaceAfter=2)
        conclusion_style = ParagraphStyle(name='ConclusionStyle', parent=styles['Normal'], fontSize=9,
                                          alignment=TA_CENTER,
                                          spaceBefore=12)
        signature_style = ParagraphStyle(name='SignatureStyle', parent=styles['Normal'], fontSize=10, alignment=TA_LEFT,
                                         spaceBefore=6)

        # --------- Document Header ---------
        story_elements.append(Paragraph("<b>Project ID:</b> BA0007156 Dataverse", header_info_style))
        story_elements.append(Paragraph(f"<b>VTP Name:</b> {current_pipeline_name}", header_info_style))
        story_elements.append(
            Paragraph("<b>Document Version Number:</b> &lt;Add version number here in format example: 1.0&gt;",
                      header_info_style))
        story_elements.append(Spacer(1, 15))

        # --------- Test Set-Up Section (Conditional) ---------
        if not ingestion_failed:  # Only add this section if ingestion was NOT failed
            story_elements.append(Paragraph("<b>Test Case Set-Up and Execution Steps</b>", title_style))
            story_elements.append(Spacer(1, 5))

            # MODIFIED: Added <font color="blue"> tag around the URL in objective_content
            objective_content = Paragraph(
                f"<b>Pipeline URL:</b> <link href=\"{pipeline_ui_url}\"><font color=\"blue\">{pipeline_ui_url}</font></link>",
                table_cell_style)

            test_setup_data = [
                [Paragraph("<b>Test Set-Up</b>", table_header_style)],
                [Paragraph("<b>VTP Name</b>", table_header_style), Paragraph(current_pipeline_name, table_cell_style)],
                [Paragraph("<b>Objective</b>", table_header_style), objective_content],
                [Paragraph("<b>Set Ups</b>", table_header_style),
                 Paragraph("&lt;Add Set up information here&gt;", table_cell_style)],
                [Paragraph("<b>Source Object Name</b>", table_header_style),
                 Paragraph(source_obj_name, table_cell_style),
                 Paragraph("<b>Target Object Name</b>", table_cell_style),
                 Paragraph(target_obj_name, table_cell_style)],
            ]
            # Conditionally add "Instruction To Tester" row
            test_setup_data.append(
                [Paragraph("<b>Instruction To Tester</b>", table_header_style), Paragraph("", table_cell_style), '',
                 ''])

            test_setup_table = Table(test_setup_data, colWidths=[100, 150, 100, 150])

            # Dynamic styling for test_setup_table
            style_commands_test_setup = [
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('SPAN', (0, 0), (3, 0)),
                ('SPAN', (1, 1), (3, 1)),
                ('SPAN', (1, 2), (3, 2)),
                ('SPAN', (1, 3), (3, 3)),
                # Backgrounds for fixed rows
                ('BACKGROUND', (0, 1), (0, 1), colors.HexColor("#E0F2F7")),
                ('BACKGROUND', (0, 2), (0, 2), colors.HexColor("#FFF3E0")),
                ('BACKGROUND', (0, 3), (0, 3), colors.HexColor("#E8F5E9")),
                ('BACKGROUND', (0, 4), (0, 4), colors.HexColor("#F3E5F5")),
                ('BACKGROUND', (2, 4), (2, 4), colors.HexColor("#F3E5F5")),
            ]

            # Add span and background for the "Instruction To Tester" row if it exists
            # If it's present, it's at index 5 (0-indexed)
            style_commands_test_setup.append(('SPAN', (1, 5), (3, 5)))
            style_commands_test_setup.append(('BACKGROUND', (0, 5), (0, 5), colors.HexColor("#F3E5F5")))

            test_setup_table.setStyle(TableStyle(style_commands_test_setup))
            story_elements.append(test_setup_table)
            story_elements.append(Spacer(1, 20))

        # --------- Main Test Execution Table Section (Combined) ---------
        story_elements.append(Paragraph("<b>Test Execution Summary</b>", title_style))
        story_elements.append(Spacer(1, 5))

        # MODIFIED: Re-structured main_table_header_data for 2-column "Executed By" row
        main_table_header_data = [
            # Row 0: "Test Execution" spanning all 5 columns
            [Paragraph("<b>Test Execution</b>", main_table_header_bold_style), '', '', '', ''],
            # New Row 1: "Executed By" label and value, centered across the entire row
            [Paragraph(f"<b>Executed By:</b> {executed_by_name}", main_table_header_bold_style), '', '', '', ''],
            # Span will handle centering
            # Original Row 1 (now Row 2): Headers for Start Date/Time, End Date/Time (only 2 columns)
            [Paragraph("<b>Execution Start Date/Time</b>", main_table_header_bold_style),
             '',  # Placeholder for span
             '',  # Placeholder for span
             Paragraph("<b>Execution End Date/Time</b>", main_table_header_bold_style),
             ''],  # Placeholder for span
            # Original Row 2 (now Row 3): Actual values for Start Date/Time, End Date/Time (only 2 columns)
            [Paragraph(execution_start_time, table_cell_center_style),
             '',  # Placeholder for span
             '',  # Placeholder for span
             Paragraph(execution_end_time, table_cell_center_style),
             ''],  # Placeholder for span
            # Original Row 3 (now Row 4): ID, Description/Action, Expected Result, Actual Result, Pass/Fail/NA (5 columns)
            [Paragraph("<b>ID</b>", main_table_header_bold_style),
             Paragraph("<b>Description/Action</b>", main_table_header_bold_style),
             Paragraph("<b>Expected Result</b>", main_table_header_bold_style),
             Paragraph("<b>Actual Result</b>", main_table_header_bold_style),
             Paragraph("<b>Pass/Fail/NA</b>", main_table_header_bold_style)]
        ]

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
                "desc": "<b>Record Count Validation:</b> Fetch the record count from Source and Destination/Target nodes and validate.",
                "expected": ["Mandatory fields should be filled and validation should be successfully completed."],
                "actual_type": "row_count",
                "actual_content_prefix": ""
            },
            {
                "id": 5,
                "desc": "<b>Uniqueness Validation:</b> Fetch the data from Target and check whether the mentioned column have unique values. Save the Uniqueness Validation result.",
                "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                             "Validate that all specified Target columns contain unique values."],
                "actual_type": "uniqueness",
                "actual_content_prefix": ""
            },
            {
                "id": 6,
                "desc": "<b>Not Null value Validation:</b> Fetch the data from Target and verify if any of the mentioned column have null values. Save the Not Null value Validation result.",
                "expected": ["Mandatory fields should be filled and Validation should be successfully completed.",
                             "Validate that all specified Target columns contain non-null values."],
                "actual_type": "not_null",
                "actual_content_prefix": ""
            }
        ]

        # data_validation_csv_path is now initialized globally to be accessible for the entire report generation
        data_validation_csv_path = "N/A"

        main_table_body_data = []

        # Process steps based on ingestion status

        for item in steps_data:
            actual_result_content_parts = []
            row_pass_fail_na = "NA"

            if item["id"] == 1:  # Ingestion
                overall_execution_status = pipeline_status_from_list.upper()

                actual_result_content_parts.append(
                    Paragraph(f"<b>Execution Status: {ingestion_status}</b>", table_cell_style))
                actual_result_content_parts.append(Spacer(1, 2))
                actual_result_content_parts.append(
                    Paragraph(f"<b>Pipeline Name:</b> {ingestion_pipeline_name}", table_cell_style))
                actual_result_content_parts.append(Spacer(1, 2))

                # Ingestion status logic
                if ingestion_status == "SUCCESS":
                    row_pass_fail_na = "Pass"
                elif ingestion_status == "FAILED":
                    row_pass_fail_na = "Fail"
                else:  # NA or other status
                    row_pass_fail_na = "NA"

            else:  # For all other validations (Schema, Data, Record Count, Uniqueness, Not Null)
                if process_validation_details:
                    overall_execution_status = get_status_for_validation_type(item["actual_type"], status_details)
                else:
                    overall_execution_status = "Fail"

                actual_result_content_parts.append(
                    Paragraph(f"<b>Execution Status: {overall_execution_status}</b>", table_cell_style))
                actual_result_content_parts.append(Spacer(1, 6))

                # Default to NA if execution status is Fail for validations 2-6
                if overall_execution_status == "Fail":
                    row_pass_fail_na = "NA"
                    actual_result_content_parts.append(Paragraph("N/A (Execution Failed)", table_cell_style))

            if item["id"] == 2:  # Schema Validation
                if overall_execution_status == "Success":
                    schemas_comment_content = None
                    schema_comments_singular = []

                    for row in validation_results:
                        if row.validation_type == "schemas":
                            schemas_comment_content = row.comment
                        elif row.validation_type == "schema":
                            schema_comments_singular.append(row.comment)

                    # Determine Pass/Fail based on singular schema comments and "N/A" in schema content
                    source_schema_is_na = False
                    target_schema_is_na = False

                    if schemas_comment_content:
                        source_schema_match = re.search(r"Source Schema:\s*(\[.*?\])", schemas_comment_content)
                        target_schema_match = re.search(r"Target Schema:\s*(\[.*?\])", schemas_comment_content)

                        source_schema_para = Paragraph(
                            f"<b>Source Schema:</b> {source_schema_match.group(1)}" if source_schema_match else "<b>Source Schema:</b> N/A",
                            table_cell_style)
                        target_schema_para = Paragraph(
                            f"<b>Target Schema:</b> {target_schema_match.group(1)}" if target_schema_match else "<b>Target Schema:</b> N/A",
                            table_cell_style)

                        if not source_schema_match or "N/A" in source_schema_match.group(1):
                            source_schema_is_na = True
                        if not target_schema_match or "N/A" in target_schema_match.group(1):
                            target_schema_is_na = True
                    else:
                        source_schema_is_na = True
                        target_schema_is_na = True
                        source_schema_para = Paragraph("<b>Source Schema:</b> N/A", table_cell_style)
                        target_schema_para = Paragraph("<b>Target Schema:</b> N/A", table_cell_style)

                    if source_schema_is_na or target_schema_is_na:
                        row_pass_fail_na = "Fail"  # If any schema is NA, it's a Fail
                    elif schema_comments_singular:
                        row_pass_fail_na = "Fail"  # If singular mismatches, it's a Fail
                    else:
                        row_pass_fail_na = "Pass"  # Otherwise, it's a Pass

                    # --- Start of Nested Table for Schema Details ---
                    schema_details_table_data = []
                    schema_details_table_data.append([source_schema_para])
                    schema_details_table_data.append([target_schema_para])

                    # Add schema mismatch details if any (from singular schema validation comments)
                    if schema_comments_singular:
                        schema_details_table_data.append(
                            [Paragraph("<b>Mismatch in schema:</b>", schema_mismatch_heading_style)])
                        for comment in schema_comments_singular:
                            schema_details_table_data.append(
                                [Paragraph(comment.strip(), schema_mismatch_comment_style)])

                    # Create the nested table
                    nested_schema_table = Table(schema_details_table_data,
                                                colWidths=[None])  # None means auto-width
                    nested_schema_table.setStyle(TableStyle([
                        # Removed global padding settings to allow individual row padding to work
                        # ('TOPPADDING', (0,0), (-1,-1), 0),
                        # ('BOTTOMPADDING', (0,0), (-1,-1), 0),
                        ('LEFTPADDING', (0, 0), (-1, -1), 0),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 0),

                        # Apply specific padding for vertical separation between rows (increased values)
                        ('BOTTOMPADDING', (0, 0), (0, 0), 8),  # Space after Source Schema (increased)
                        ('BOTTOMPADDING', (0, 1), (0, 1), 12),  # Space after Target Schema (more space, increased)
                        ('BOTTOMPADDING', (0, 2), (0, 2), 6),  # Space after Mismatch heading (increased)

                        # No grid for internal table
                        ('GRID', (0, 0), (-1, -1), 0, colors.white)  # Ensure no internal grid lines
                    ]))
                    actual_result_content_parts.append(nested_schema_table)
                    # --- End of Nested Table for Schema Details ---

                # If overall_execution_status is "Fail", row_pass_fail_na is already "NA" from the default block above.
                # If overall_execution_status is "NA", row_pass_fail_na is already "NA" from the default block above.



            elif item["id"] == 3:  # Data Validation

                if overall_execution_status == "Success":

                    comments_raw = [row.comment for row in validation_results if
                                    row.validation_type == item["actual_type"]]

                    source_only_count = 0

                    destination_only_count = 0

                    all_discrepancies_for_pdf = []  # To store all discrepancies for the new PDF

                    for comment in comments_raw:

                        parts = comment.split('||', 1)

                        comment_type = parts[0]

                        raw_data_content = ""

                        if len(parts) > 1:
                            raw_data_content = parts[1]  # Store raw content for PDF

                        if "present_only_in_source" in comment_type:

                            source_only_count += 1

                            all_discrepancies_for_pdf.append(
                                {"type": "present_only_in_source", "data": raw_data_content})

                        elif "present_only_in_destination" in comment_type:

                            destination_only_count += 1

                            all_discrepancies_for_pdf.append(
                                {"type": "present_only_in_destination", "data": raw_data_content})

                    # Data Validation status logic

                    if source_only_count > 0 or destination_only_count > 0:

                        row_pass_fail_na = "Fail"  # If any discrepancies, it's a Fail

                    else:

                        row_pass_fail_na = "Pass"  # Otherwise, it's a Pass

                    if comments_raw:

                        # if len(comments_raw) <= 10:  # If 10 or fewer discrepancies, print inline
                        #
                        #     if source_only_count > 0:
                        #
                        #         actual_result_content_parts.append(
                        #
                        #             Paragraph("<b>present_only_in_source:</b>", table_cell_style))
                        #
                        #         for disc in all_discrepancies_for_pdf:
                        #
                        #             if disc['type'] == 'present_only_in_source':
                        #
                        #                 formatted_data = disc['data']
                        #
                        #                 try:
                        #
                        #                     data_dict = json.loads(disc['data'])
                        #
                        #                     if 'row_hash' in data_dict:
                        #                         del data_dict['row_hash']
                        #
                        #                     formatted_pairs = [f"<b>{k}</b>: {v}" for k, v in data_dict.items()]
                        #
                        #                     formatted_data = ", ".join(formatted_pairs)
                        #
                        #                 except json.JSONDecodeError:
                        #
                        #                     formatted_data = re.sub(r'\|\|[0-9a-fA-F]{64}$', '', disc['data']).strip()
                        #
                        #                 actual_result_content_parts.append(Paragraph(formatted_data, table_cell_style))
                        #
                        #                 actual_result_content_parts.append(Spacer(1, 2))
                        #
                        #         actual_result_content_parts.append(Spacer(1, 4))
                        #
                        #     if destination_only_count > 0:
                        #
                        #         actual_result_content_parts.append(
                        #
                        #             Paragraph("<b>present_only_in_destination:</b>", table_cell_style))
                        #
                        #         for disc in all_discrepancies_for_pdf:
                        #
                        #             if disc['type'] == 'present_only_in_destination':
                        #
                        #                 formatted_data = disc['data']
                        #
                        #                 try:
                        #
                        #                     data_dict = json.loads(disc['data'])
                        #
                        #                     if 'row_hash' in data_dict:
                        #                         del data_dict['row_hash']
                        #
                        #                     formatted_pairs = [f"<b>{k}</b>: {v}" for k, v in data_dict.items()]
                        #
                        #                     formatted_data = ", ".join(formatted_pairs)
                        #
                        #                 except json.JSONDecodeError:
                        #
                        #                     formatted_data = re.sub(r'\|\|[0-9a-fA-F]{64}$', '', disc['data']).strip()
                        #
                        #                 actual_result_content_parts.append(Paragraph(formatted_data, table_cell_style))
                        #
                        #                 actual_result_content_parts.append(Spacer(1, 2))
                        #
                        #         actual_result_content_parts.append(Spacer(1, 4))


                        # else:  # If more than 10 discrepancies, generate a separate PDF

                            # Generate PDF file for discrepancies.

                        data_validation_pdf_path = generate_data_validation_pdf(

                            current_pipeline_name, current_pipeline_id, current_batch_id,

                            all_discrepancies_for_pdf, NEW_REPORT_DIR

                        )

                        actual_result_content_parts.append(

                            Paragraph(f"Total discrepancies: {len(comments_raw)}", table_cell_style))

                        if source_only_count > 0:
                            actual_result_content_parts.append(

                                Paragraph(f"- Present only in Source: {source_only_count}", table_cell_style))

                        if destination_only_count > 0:
                            actual_result_content_parts.append(

                                Paragraph(f"- Present only in Destination: {destination_only_count}",
                                          table_cell_style))

                        actual_result_content_parts.append(Spacer(1, 6))

                        # Removed the clickable link as per user's request.

                        if data_validation_pdf_path:

                            actual_result_content_parts.append(
                                Paragraph("Data Validation Report generated successfully.", table_cell_style))

                        else:

                            actual_result_content_parts.append(
                                Paragraph("Data Validation Report generation failed.", table_cell_style))


                    else:

                        actual_result_content_parts.append(Paragraph("N/A", table_cell_style))

            elif item["id"] == 4:  # Record Count Validation
                if overall_execution_status == "Success":
                    comments_raw = [row.comment for row in validation_results if
                                    row.validation_type == item["actual_type"]]
                    if comments_raw:
                        comment = comments_raw[0]
                        source_match = re.search(r"(?:Source Row Count:|Number of rows in Source:)\s*(\d+)",
                                                 comment)
                        target_match = re.search(r"Target:\s*(\d+)", comment)
                        difference_match = re.search(r"Difference:\s*(\d+)", comment)

                        source_val = source_match.group(1) if source_match else "N/A"
                        target_val = target_match.group(1) if target_match else "N/A"
                        difference_val = difference_match.group(1) if difference_match else "N/A"

                        actual_result_content_parts.append(Paragraph(
                            f"<b>Number of rows in Source:</b> {source_val}<br/><b>Target:</b> {target_val}<br/><b>Difference:</b> {difference_val}",
                            table_cell_style))
                        # Record Count Validation status logic
                        if difference_val == "0":
                            row_pass_fail_na = "Pass"
                        else:
                            row_pass_fail_na = "Fail"
                    else:
                        actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                        row_pass_fail_na = "NA"  # If no comments, and status is success, it's still NA for content
                # If overall_execution_status is "Fail", row_pass_fail_na is already "NA" from the default block above.
                # If overall_execution_status is "NA", row_pass_fail_na is already "NA" from the default block above.


            elif item["id"] == 5:  # Uniqueness Validation
                if overall_execution_status == "Success":
                    comments_raw = [row.comment for row in validation_results if
                                    row.validation_type == item["actual_type"]]
                    if comments_raw:
                        has_not_unique = False
                        for comment in comments_raw:
                            parts = comment.strip().split(', ')
                            formatted_parts = []
                            for part in parts:
                                formatted_part = re.sub(r"([^:]+):(.*)", r"<b>\1</b>:\2", part)
                                formatted_parts.append(formatted_part)
                            actual_result_content_parts.append(
                                Paragraph("<br/>".join(formatted_parts), table_cell_style))
                            actual_result_content_parts.append(Spacer(1, 2))  # Added spacer for uniqueness comments

                            if "not_unique" in comment.lower():
                                has_not_unique = True

                        # Uniqueness Validation status logic
                        if has_not_unique:
                            row_pass_fail_na = "Fail"  # If any comment contains "not_unique", it's a Fail
                        else:
                            row_pass_fail_na = "Pass"  # Otherwise, it's a Pass
                    else:
                        actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                        row_pass_fail_na = "NA"  # If no comments, and status is success, it's still NA for content
                # If overall_execution_status is "Fail", row_pass_fail_na is already "NA" from the default block above.
                # If overall_execution_status is "NA", row_pass_fail_na is already "NA" from the default block above.


            elif item["id"] == 6:  # Not Null Validation
                if overall_execution_status == "Success":
                    comments_raw = [row.comment for row in validation_results if
                                    row.validation_type == item["actual_type"]]
                    logging.debug(f"Not Null Validation comments_raw: {comments_raw}")
                    if comments_raw:
                        has_problematic_nulls = False
                        all_formatted_comments = []  # List to hold all formatted comment strings
                        for comment in comments_raw:
                            # Split the comment by comma and format each part
                            parts = comment.strip().split(',')
                            formatted_parts = []
                            for part in parts:
                                formatted_part = re.sub(r"([^:]+):(.*)", r"<b>\1</b>:\2", part.strip())
                                formatted_parts.append(formatted_part)

                            # Join parts of the current comment with <br/> and add to the list
                            all_formatted_comments.append("<br/>".join(formatted_parts))

                            # Check for problematic nulls across all comments
                            if "NULLs Exist" in comment:
                                has_problematic_nulls = True

                        # Append all formatted comments as a single Paragraph, separated by spacers if multiple comments
                        for i, formatted_comment_block in enumerate(all_formatted_comments):
                            actual_result_content_parts.append(Paragraph(formatted_comment_block, table_cell_style))
                            if i < len(
                                    all_formatted_comments) - 1:  # Add spacer between comment blocks, not after the last one
                                actual_result_content_parts.append(Spacer(1, 2))

                        # Not Null Validation status logic
                        if has_problematic_nulls:
                            row_pass_fail_na = "Fail"  # If any comment contains "nulls" (but not "no nulls"), it's a Fail
                        else:
                            row_pass_fail_na = "Pass"  # Otherwise, it's a Pass
                    else:
                        actual_result_content_parts.append(Paragraph("N/A", table_cell_style))
                        row_pass_fail_na = "NA"  # If no comments, and status is success, it's still NA for content
                # If overall_execution_status is "Fail", row_pass_fail_na is already "NA" from the default block above.
                # If overall_execution_status is "NA", row_pass_fail_na is already "NA" from the default block above.

            expected_result_flowables = []
            for i, text in enumerate(item['expected'], 1):
                p = Paragraph(f"{i}. {text}", list_item_style)
                expected_result_flowables.append(p)

            # Append all validation rows if ingestion was successful
            main_table_body_data.append([
                Paragraph(str(item["id"]), table_cell_style),
                Paragraph(item["desc"], table_cell_style),
                expected_result_flowables,
                actual_result_content_parts,
                Paragraph(row_pass_fail_na, table_cell_style)
            ])

        # final_table_body_data is now directly main_table_body_data as the filtering happens inside the loop
        final_table_body_data = main_table_body_data

        full_table_data = main_table_header_data + final_table_body_data
        # MODIFIED: Adjusted colWidths to make the "ID" column smaller and "Description/Action" slightly smaller.
        # Original: [40, 120, 180, 160, 60]
        # New: [40, 120, 150, 190, 60] (ID, Description same, Expected smaller, Actual larger)
        main_table = Table(full_table_data, colWidths=[40, 120, 150, 190, 60])

        # MODIFIED: Re-structured TableStyle for better header alignment and new row
        main_table.setStyle(TableStyle([
            # General Grid/Padding
            ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),

            # Header Rows Background
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),  # "Test Execution" row (Row 0)
            ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor("#E0F2F7")),  # New "Executed By" row (Row 1)
            ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor("#D9E1F2")),  # Headers for Start/End Date/Time (Row 2)
            ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor("#E0F2F7")),  # Actual values for dates/times (Row 3)
            ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor("#D9E1F2")),  # "ID", "Description", etc. headers (Row 4)

            # Header Structure (Spans)
            ('SPAN', (0, 0), (4, 0)),  # Span "Test Execution" across all 5 columns (Row 0)
            # MODIFIED: Span for "Executed By" row to cover all 5 columns, centered
            ('SPAN', (0, 1), (4, 1)),  # Span the entire "Executed By" row across all 5 columns

            # MODIFIED: For Row 2 (Execution Start/End Date/Time headers)
            ('SPAN', (0, 2), (2, 2)),  # Span the first 3 columns for "Execution Start Date/Time" header
            ('SPAN', (3, 2), (4, 2)),  # Span the last 2 columns for "Execution End Date/Time" header

            # MODIFIED: For Row 3 (Execution Start/End Date/Time values)
            ('SPAN', (0, 3), (2, 3)),  # Span the first 3 columns for start time value
            ('SPAN', (3, 3), (4, 3)),  # Span the last 2 columns for end time value

            # Row 4 ("ID", "Description", etc. headers) - remains 5 columns
            ('ALIGN', (0, 4), (4, 4), 'CENTER'),  # All cells in this row should be centered
            ('VALIGN', (0, 4), (4, 4), 'MIDDLE'),

            # Header Alignment - Explicitly set per cell/span for precision
            ('ALIGN', (0, 0), (4, 0), 'CENTER'),  # "Test Execution" title (Row 0)
            ('VALIGN', (0, 0), (4, 0), 'MIDDLE'),

            # MODIFIED: Alignment for new "Executed By" row (Row 1)
            ('ALIGN', (0, 1), (4, 1), 'CENTER'),  # "Executed By:" and name should be centered within their span
            ('VALIGN', (0, 1), (4, 1), 'MIDDLE'),

            # Row 2 (Headers for Start/End Date/Time)
            ('ALIGN', (0, 2), (2, 2), 'CENTER'),  # "Execution Start Date/Time" header
            ('ALIGN', (3, 2), (4, 2), 'CENTER'),  # "Execution End Date/Time" header
            ('VALIGN', (0, 2), (4, 2), 'MIDDLE'),  # Vertical align for the whole row

            # MODIFIED: Explicitly setting alignment for Row 3 (Actual values for Start/End Date/Time)
            ('ALIGN', (0, 3), (2, 3), 'CENTER'),  # Actual 'Execution Start Date/Time' value
            ('ALIGN', (3, 3), (4, 3), 'CENTER'),  # Actual 'Execution End Date/Time' value
            ('VALIGN', (0, 3), (4, 3), 'MIDDLE'),  # Vertical align for the whole row

            # Body Alignment (defaults to TOP/LEFT via Paragraph styles, but set VALIGN for rows)
            # The row index for body data starts from 5 (0-4 are headers)
            # This style applies to the rows that are actually added to final_table_body_data
            ('VALIGN', (0, 5), (-1, -1), 'TOP'),  # Vertical align top for body rows
        ]))

        story_elements.append(main_table)
        story_elements.append(Spacer(1, 20))

        # Only append conclusion if ingestion was successful
        if not ingestion_failed:
            story_elements.append(Paragraph("<b>Conclusion:</b> &lt;Add Conclusion Here&gt;", conclusion_style))
            story_elements.append(Spacer(1, 20))

        return story_elements, None, data_validation_pdf_path
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during report generation for pipeline {current_pipeline_name}: {e}")
        return [], None, None


# --------- Footer Function (moved to global scope for doc.build) ---------
def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    # Adjusted Y position for page number to be slightly above the line
    canvas.drawString(30, 38, f"Page {doc.page}")  # Moved up from 30 to 38
    # Keep line at 30, so text is above it.
    canvas.line(30, 30, doc.width + 30, 30)  # Line remains at Y=30
    canvas.restoreState()


# Initialize SparkSession once
spark = None



sender_email = "nabu-dev.notifications@modak.com"
sender_password = "Yuy62178"

# Report Directory
NEW_REPORT_DIR = "/home/spark-srv-account/ab2012/pipeline_reports_new/pipeline_reports_new"
os.makedirs(NEW_REPORT_DIR, exist_ok=True)

try:
    spark = SparkSession.builder \
        .appName("ValidationReportGenerator") \
        .getOrCreate()
    logging.info("SparkSession initialized successfully.")
except Exception as e:
    logging.error(f"ERROR: Could not initialize SparkSession. This script requires a Spark environment to load data.")
    logging.error(f"Error details: {e}")
    sys.exit(1)

kosh_credential_id = int(spark.conf.get("spark.nabu.kosh_credential_id"))
kosh_credential_type_id = 1
token = spark.conf.get("spark.nabu.token")
credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")

kosh_username, kosh_password = fetch_credentials(kosh_credential_id, kosh_credential_type_id, key_map=('username', 'password'))


PIPELINE_DB_URL = spark.conf.get("spark.nabu.kosh_url")
PIPELINE_DB_USERNAME = kosh_username
PIPELINE_DB_PASSWORD = kosh_password
JDBC_DRIVER = "org.postgresql.Driver"



# JDBC_JAR_PATH = "file:///C:/Users/MT23003/Downloads/postgresql-42.7.6.jar"

# Report Directory
# NEW_REPORT_DIR = "/home/spark-srv-account/ab2012/pipeline_reports_new"  # New folder for saving PDFs
os.makedirs(NEW_REPORT_DIR, exist_ok=True)

logging.info(f"Using provided DB URL: {PIPELINE_DB_URL}")
logging.info(f"Using provided DB Username: {PIPELINE_DB_USERNAME}")  # Corrected variable name

os.makedirs(NEW_REPORT_DIR, exist_ok=True)

report_generator_batch_id = 167376462025882330

ui_url = spark.conf.get("spark.nabu.ui_url")
email_creds_id = int(spark.conf.get("spark.nabu.email_credential_id"))

sender_email, sender_password = fetch_credentials(email_creds_id, 1, key_map=('username', 'password'))


# Step 1: Get dataflow_id and dataflow_batch_id
workflow_query = f"""
    SELECT workflow_orch_id, batch_id
    FROM nabu.checkpoint_workflow_node_status
    WHERE node_batch_id = {report_generator_batch_id}
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
ingestion_status = None

# Optional: Process validation flow
if node_details.get("Ingestion pipeline"):
    ingestion_status = node_details["Ingestion pipeline"]["status"]
    ingestion_pipeline_name = node_details["Ingestion pipeline"]["pipeline_name"]
    print(f"\n📦 Ingestion pipeline status: {ingestion_status}")

    if ingestion_status == "FAILED":
        executed_by = exec_row['cru_by']
        execution_start_dt = exec_row["valid_from_ts"].strftime("%Y-%m-%d %H:%M:%S.%f")

        execution_end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print("Ingestion failed")
        generate_ingestion_failed_report(executed_by_name=executed_by, execution_start_time=execution_start_dt, execution_end_time=execution_end_dt)
        send_all_pdfs_via_outlook_smtp(
            sender_email=sender_email,
            sender_password=sender_password,
            receiver_email=executed_by,
            subject="Validation reports",
            body="Please find validation report.",
            folder_path=os.path.join(NEW_REPORT_DIR, dataflow_batch_id)
        )
        exit(0)
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

executed_by = exec_row['cru_by']
execution_start_dt = exec_row["valid_from_ts"].strftime("%Y-%m-%d %H:%M:%S.%f")

execution_end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

# --- User-specified pipelines to process (hardcoded) ---

logging.info("pipelines_to_proccess: %s", pipelines_to_process)

if not pipelines_to_process:
    logging.info("No pipelines to process for report generation. Exiting.")
    if spark:
        spark.stop()
    sys.exit(0)

all_reports_story_elements = []

generated_data_validation_reports = []

for idx, pipeline in enumerate(pipelines_to_process):
    p_name = pipeline['pipeline_name']
    p_id = pipeline['pipeline_id']
    b_id = str(pipeline['pipeline_batch_id'])
    p_status = pipeline['status']

    effective_ingestion_status_for_report = p_status

    logging.info(f"\n--- Generating report elements for Pipeline: {p_name} (ID: {p_id}, Batch ID: {b_id}) ---")

    workspace_id = "N/A"
    pipeline_ui_url = "N/A"
    source_obj_name = "N/A"
    target_obj_name = "N/A"

    if p_id != "N/A":
        workspace_query = f"SELECT workspace_id FROM nabu.workspace_pipeline WHERE workspace_pipeline_id = {p_id}"
        try:
            workspace_df = spark.read.format("jdbc").options(
                url=PIPELINE_DB_URL,
                query=workspace_query,
                user=PIPELINE_DB_USERNAME,  # Corrected variable name
                password=PIPELINE_DB_PASSWORD,
                driver=JDBC_DRIVER
            ).load()
            workspace_row = workspace_df.first()
            if workspace_row:
                workspace_id = workspace_row['workspace_id']
                logging.info(f"✅ Fetched Workspace ID: {workspace_id} for pipeline {p_id}")
            else:
                logging.warning(f"⚠️ No workspace_id found for pipeline_id: {p_id}")
        except Exception as e:
            logging.error(f"Error fetching workspace_id for pipeline {p_id}: {e}")

    if workspace_id != "N/A" and p_id != "N/A":
        pipeline_ui_url = f"{ui_url}/workspace?workspace_id={workspace_id}&pipeline_id={p_id}&active_tab=curationUI"
        logging.info(f"Generated Pipeline URL: {pipeline_ui_url} for pipeline {p_id}")

    source_target_sql = f"""
    SELECT
      MAX(CASE WHEN var ->> 'variable' = 'source_obj_name' THEN var ->> 'value' END) AS source_obj_name,
      MAX(CASE WHEN var ->> 'variable' = 'target_obj_name' THEN var ->> 'value' END) AS target_obj_name
    FROM nabu.workspace_pipeline,
      jsonb_array_elements(
        (workspace_pipeline_json -> 'pipelineJson' -> 'pipeline_config' -> 'flow_details' -> 'user_variables' -> 'user_variables_details')::jsonb
      ) AS var
    WHERE workspace_pipeline_id = {p_id}
    """
    try:
        source_target_df = spark.read.format("jdbc").options(
            url=PIPELINE_DB_URL,
            query=source_target_sql,
            user=PIPELINE_DB_USERNAME,  # Corrected variable name
            password=PIPELINE_DB_PASSWORD,
            driver=JDBC_DRIVER
        ).load()
        source_target_row = source_target_df.first()
        if source_target_row:
            source_obj_name = source_target_row['source_obj_name'] if source_target_row['source_obj_name'] else "N/A"
            target_obj_name = source_target_row['target_obj_name'] if source_target_row['target_obj_name'] else "N/A"
            logging.info(f"Successfully loaded Source: {source_obj_name}, Target: {target_obj_name} for pipeline {p_id}.")
        else:
            logging.warning(f"No source/target object names found for pipeline ID: {p_id}.")
    except Exception as e:
        logging.error(f"Error loading source/target object names for pipeline {p_id}: {e}")

    # Generate report elements for the current pipeline
    current_pipeline_story, _, data_validation_pdf_path = generate_pipeline_report( # Unpack only data_validation_pdf_path
        spark, p_id, b_id, p_name,
        executed_by, execution_start_dt, execution_end_dt,
        source_obj_name, target_obj_name,
        pipeline_status_from_list=effective_ingestion_status_for_report,
        pipeline_ui_url=pipeline_ui_url, ingestion_failed=False
    )
    all_reports_story_elements.extend(current_pipeline_story)

    if data_validation_pdf_path:
        generated_data_validation_reports.append(data_validation_pdf_path) # Add to list of data validation reports
        logging.info(f"Separate Data Validation Report for {p_name} generated at: {data_validation_pdf_path}")

    # Add a page break between pipeline reports in the combined PDF, unless it's the last one
    if idx < len(pipelines_to_process) - 1:
        all_reports_story_elements.append(PageBreak())

    logging.info(f"--- Finished report elements for Pipeline: {p_name} ---")

report_dir = os.path.join(NEW_REPORT_DIR, dataflow_batch_id)
os.makedirs(report_dir, exist_ok=True)  # ✅ Creates directory if not exists

# Final PDF path
combined_pdf_path = os.path.join(report_dir, "validation_report.pdf")
doc = SimpleDocTemplate(combined_pdf_path, pagesize=A4, topMargin=40, bottomMargin=40, leftMargin=30, rightMargin=30)

try:
    doc.build(all_reports_story_elements, onFirstPage=footer, onLaterPages=footer)
    logging.info(f"Combined PDF report saved at: {combined_pdf_path}")
except Exception as e:
    logging.error(f"Error building combined PDF: {e}")
if generated_data_validation_reports:
    logging.info("\n--- All Generated Data Validation Reports ---")
    for path in generated_data_validation_reports:
        logging.info(f"- {path}")
else:
    logging.info("\nNo separate data validation reports were generated.")

if spark:
    spark.stop()
logging.info("All reports generated. Script finished.")

send_all_pdfs_via_outlook_smtp(
            sender_email=sender_email,
            sender_password=sender_password,
            receiver_email=executed_by,
            subject="Validation reports",
            body="Please find validation report.",
            folder_path=os.path.join(NEW_REPORT_DIR, dataflow_batch_id)
        )
