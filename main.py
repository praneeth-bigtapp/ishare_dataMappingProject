from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from werkzeug.utils import secure_filename
import os
import logging
import traceback
from datetime import datetime
from db_connection import connect_to_mysql, load_db_config
from datamapping import get_vidal_mappings, upload_vidal_data, upload_mapping
import mysql.connector
from mysql.connector import Error
import logging.handlers
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

@app.route("/uploadVidalData", methods=["POST"])
@cross_origin(origins="*")
def upload_vidal_data_endpoint():
    """
    API to upload Vidal claim data Excel file and process it:
    1. Creates TEMP_CLAIM table based on Excel structure
    2. Loads data into TEMP_CLAIM
    3. Distributes data to target tables based on mapping configurations
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"})
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"})
            
        if not allowed_file(file.filename):
            return jsonify({"error": f"File type not allowed. Allowed types: {', '.join(['xlsx', 'xls'])}"}), 400
            
        # Save file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{timestamp}_{filename}")
        file.save(save_path)
        
        logger.info(f"File saved to {save_path}")
        
        # Create temp table and load data
        temp_result = create_temp_table_and_load_data(save_path)
        if "error" in temp_result:
            return jsonify(temp_result)
            
        # Distribute data to target tables
        connection = connect_to_mysql()
        if not connection:
            return jsonify({"error": "Database connection failed"})
            
        cursor = connection.cursor()
        dist_result = distribute_data_to_target_tables(connection, cursor)
        
        # Close database connection
        cursor.close()
        connection.close()
        
        # Return combined results
        return jsonify({
            "temp_table": temp_result,
            "distribution": dist_result
        })
        
    except Exception as e:
        logger.error(f"Error in upload_vidal_data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)})

@app.route("/createTempClaimMapping", methods=["POST"])
@cross_origin(origins="*")
def create_temp_claim_mappings():
    """
    API to create the TEMP_CLAIM to m_claims mapping table with predefined mappings
    """
    try:
        result = create_temp_claim_mapping()
        if "error" in result:
            return jsonify(result), 500
            
        return jsonify(result), 200
                
    except Exception as e:
        logger.error(f"Error in create_temp_claim_mappings: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8081)
