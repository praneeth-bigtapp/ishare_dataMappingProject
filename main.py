from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from werkzeug.utils import secure_filename
import os
import json
import logging
import traceback
from datetime import datetime
from db_connection import connect_to_mysql, load_db_config
from datamapping import get_vidal_mappings, upload_vidal_data, upload_mapping
import mysql.connector
from mysql.connector import Error
import logging.handlers
from schedularlogic import process_target_table ,schedule_processing   # Import the function
from subprocess import Popen
import yaml

# Configure logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app.log')

# Create a custom formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Configure file handler with a more robust configuration
file_handler = logging.handlers.RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    delay=True  # Don't create file until first log
)
file_handler.setFormatter(formatter)

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Add our handlers
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Create logger for this module
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configure upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'xlsx', 'xls'}

@app.route("/uploadMapping", methods=["POST"])
@cross_origin(origins="*")
def handle_mapping_upload():
    """
    API endpoint to upload mapping configuration Excel file
    Request: multipart/form-data with file
    Response: JSON with status and message
    """
    try:
        if 'file' not in request.files:
            return jsonify({
                "error": "No file part in the request",
                "status": "error"
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                "error": "No file selected",
                "status": "error"
            }), 400

        if not allowed_file(file.filename):
            return jsonify({
                "error": f"File type not allowed. Allowed types: {', '.join(['xlsx', 'xls'])}",
                "status": "error"
            }), 400

        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        try:
            # Process the mapping file
            result = upload_mapping(file_path)
            
            if result.get("status") == "error":
                return jsonify(result), 400
                
            return jsonify(result), 200
            
        finally:
            # Clean up the uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)

    except Exception as e:
        logger.error(f"Error in mapping upload: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500

@app.route("/uploadVidalClaims", methods=["POST"])
@cross_origin(origins="*")
def handle_vidal_upload():
    """
    API endpoint to upload Vidal claims Excel file
    Request: multipart/form-data with file
    Response: JSON with status and message
    """
    try:
        if 'file' not in request.files:
            return jsonify({
                "error": "No file part in the request",
                "status": "error"
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                "error": "No file selected",
                "status": "error"
            }), 400

        if not allowed_file(file.filename):
            return jsonify({
                "error": f"File type not allowed. Allowed types: {', '.join(['xlsx', 'xls'])}",
                "status": "error"
            }), 400

        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        try:
            # Process the Vidal claims file
            result = upload_vidal_data(file_path)
            
            if result.get("status") == "error":
                return jsonify(result), 400
                
            return jsonify(result), 200
            
        finally:
            # Clean up the uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)

    except Exception as e:
        logger.error(f"Error in Vidal claims upload: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500

@app.route("/processMClaims", methods=["POST"])
def process_m_claims():
    """
    Endpoint to process m_claims data.
    """
    try:
        # Call the processing logic for `m_claims`
        result = process_target_table("m_claims")
        if "error" in result:
            return jsonify(result), 500
        return jsonify(result), 200
    except Exception as e:
        logging.error(f"Error in /processMClaims: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/processMClaimHealth", methods=["POST"])
def process_m_claim_health():
    """
    Endpoint to process m_claim_health data.
    """
    try:
        # Call the processing logic for `m_claim_health`
        result = process_target_table("m_claim_health")
        if "error" in result:
            return jsonify(result), 500
        return jsonify(result), 200
    except Exception as e:
        logging.error(f"Error in /processMClaimHealth: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/processMClaimSettleDetail", methods=["POST"])
def process_m_claim_settle_detail():
    """
    Endpoint to process m_claim_settle_detail data.
    """
    try:
        # Call the processing logic for `m_claim_settle_detail`
        result = process_target_table("m_claim_settle_detail")
        if "error" in result:
            return jsonify(result), 500
        return jsonify(result), 200
    except Exception as e:
        logging.error(f"Error in /processMClaimSettleDetail: {str(e)}")
        return jsonify({"error": str(e)}), 500

def schedule_processing_endpoint():
    """
    API to schedule processing for specific target tables with optional start_date and end_date filters.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request body"}), 400

        scheduler_name = data.get("scheduler_name")
        cron_expression = data.get("cron_expression")
        target_table = data.get("target_table")
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        if not scheduler_name or not cron_expression or not target_table:
            return jsonify({
                "error": "Missing required parameters: scheduler_name, cron_expression, or target_table"
            }), 400

        # Call the schedule processing function
        result = schedule_processing(scheduler_name, cron_expression, start_date, end_date)
        if "error" in result:
            return jsonify(result), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error in /scheduleProcessing: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/scheduleProcessing", methods=["POST"])
def schedule_processing_api():
    """
    API to schedule processing and update scheduler_details.
    """
    try:
        # Parse request payload
        payload = request.json
        if not payload:
            return jsonify({"error": "Missing JSON payload"}), 400

        scheduler_name = payload.get("scheduler_name")
        cron_expression = payload.get("cron_expression")
        target_table = payload.get("target_table")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")

        # Validate required parameters
        missing_params = []
        if not scheduler_name:
            missing_params.append("scheduler_name")
        if not cron_expression:
            missing_params.append("cron_expression")
        if not target_table:
            missing_params.append("target_table")

        if missing_params:
            return jsonify({"error": f"Missing required parameters: {', '.join(missing_params)}"}), 400

        # Call the function to schedule processing
        result = schedule_processing(
            scheduler_name=scheduler_name,
            cron_expression=cron_expression,
            target_table=target_table,
            start_date=start_date,
            end_date=end_date
        )

        if "error" in result:
            return jsonify(result), 500

        return jsonify(result), 200

    except Exception as e:
        logging.error(f"Error in /scheduleProcessing: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

@app.route("/schedulerLog", methods=["GET"])
def get_schedulor_log():
    """
    API to schedule processing and update scheduler_details.
    """
    try:
        # Database connection and setup
        connection = connect_to_mysql()
        if not connection:
            raise Exception("Failed to connect to database")

        cursor = connection.cursor(dictionary=True)
        query = """SELECT * FROM schedulerlog"""
        cursor.execute(query)
        result = cursor.fetchall()
        

        return jsonify({
            "success":True,
            "data":result
            }), 200

    except Exception as e:
        logging.error(f"Error in /scheduleProcessing: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8081)
