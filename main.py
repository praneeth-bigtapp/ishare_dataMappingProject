from flask import Flask, request, jsonify
from datamapping import create_mapping_table_from_excel
from data_upload import upload_data_with_mapping
from ftp_connection import connect_and_list_files
import os
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import jwt
import re
from werkzeug.utils import secure_filename
import logging
import traceback
from logging.handlers import RotatingFileHandler
import mysql.connector
from mysql.connector import Error
from decimal import Decimal
from datetime import date
from flask_cors import CORS, cross_origin

# Configure logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app.log')
logging.basicConfig(
    handlers=[RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)],
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configure SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'your-secret-key'  # Change this to a secure secret key in production

# Configure upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')

# Initialize SQLAlchemy
db = SQLAlchemy(app)

# User Model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

# Create all database tables
with app.app_context():
    db.create_all()

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='your_database',
            user='your_username',
            password='your_password'
        )
        return connection
    except Error as e:
        logger.error(f"Failed to connect to MySQL database: {str(e)}")
        return None

@app.route("/uploadMapping", methods=["POST"])
@cross_origin(origins="*")
def upload_mapping():
    """
    API endpoint to upload an Excel file and create a mapping table in the database.
    Accepts multipart/form-data with a file field.
    The table name will be derived from the Excel filename.
    """
    try:
        logger.info("Starting file upload process")
        if 'file' not in request.files:
            logger.error("No file part in the request")
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']  # This handles multipart/form-data
        if file.filename == '':
            logger.error("No file selected for uploading")
            return jsonify({"error": "No file selected for uploading"}), 400

        logger.info(f"Processing file: {file.filename}")

        # Check file extension
        if not file.filename.lower().endswith(('.xls', '.xlsx')):
            logger.error(f"Invalid file format: {file.filename}")
            return jsonify({"error": "Invalid file format. Please upload an Excel file (.xls or .xlsx)"}), 400

        # Create upload folder if it doesn't exist
        logger.debug(f"Creating upload folder: {UPLOAD_FOLDER}")
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

        # Generate unique filename for storage
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        original_filename = secure_filename(file.filename)
        unique_filename = f"{timestamp}_{original_filename}"
        file_path = os.path.join(UPLOAD_FOLDER, unique_filename)
        
        logger.info(f"Saving file to: {file_path}")
        
        try:
            # Save the uploaded file
            file.save(file_path)
            logger.info("File saved successfully")
        except Exception as e:
            logger.error(f"Failed to save file: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Failed to save file: {str(e)}"}), 500

        # Get table name from the original filename (without extension)
        table_name = os.path.splitext(original_filename)[0].lower()
        logger.info(f"Using table name from filename: {table_name}")

        try:
            # Create the mapping table from the uploaded Excel file
            logger.info("Starting table creation process")
            result = create_mapping_table_from_excel(file_path, table_name)
            logger.info("Table creation completed")
            
            # Clean up the uploaded file after processing
            if os.path.exists(file_path):
                logger.debug(f"Removing temporary file: {file_path}")
                os.remove(file_path)

            if "error" in result:
                logger.error(f"Error in table creation: {result['error']}")
                return jsonify(result), 400
            
            logger.info("Upload process completed successfully")
            return jsonify(result), 200

        except Exception as e:
            logger.error(f"Error during table creation: {str(e)}")
            logger.error(traceback.format_exc())
            # Clean up the file in case of processing error
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up file: {str(cleanup_error)}")
            raise e

    except Exception as e:
        logger.error(f"Unexpected error during upload process: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"An error occurred while processing the file: {str(e)}"}), 500


@app.route("/uploaData", methods=["POST"])
@cross_origin(origins="*")
def upload_data_mapped():
    """
    API to upload Excel data to a table using mapping configuration.
    Accepts multipart/form-data with a file field and mapping_table field.
    The table name will be derived from the Excel filename.
    Required form parameters:
    - file: Excel file with source data (multipart/form-data)
    - mapping_table: Name of the mapping table containing source-to-target column mappings
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400
            
        file = request.files['file']  # This handles multipart/form-data
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
            
        mapping_table = request.form.get('mapping_table')
        if not mapping_table:
            return jsonify({"error": "Mapping table name is required"}), 400
            
        # Save uploaded file
        file_path = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(file_path)
        
        try:
            # Process file and create table
            result = upload_data_with_mapping(file_path, mapping_table)
            
            if "error" in result:
                return jsonify(result), 500
                
            return jsonify(result), 200
            
        finally:
            # Clean up uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/ftpConnection', methods=['POST'])
@cross_origin(origins="*")
def connect_and_list():
    """
    API to connect to an FTP server and list the files in a directory.
    Requires: host, username, password, and an optional path.
    """
    try:
        # Get connection details from the request
        host = request.json.get('host')
        username = request.json.get('username')
        password = request.json.get('password')
        port = request.json.get('port', 21)  # Default FTP port
        path = request.json.get('path', '/')  # Default to root directory

        # Validate required fields
        if not host or not username or not password:
            return jsonify({"error": "host, username, and password are required"}), 400

        # Call the FTP connection and listing logic
        result = connect_and_list_files(host, username, password, port, path)

        # Return the result as a JSON response
        if "error" in result:
            return jsonify(result), 500

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



    """
    API endpoint to get all data from a specified mapping table.
    Args:
        table_name: Name of the table to retrieve data from
    Returns:
        JSON response with table data or error message
    """
    connection = None
    cursor = None
    try:
        logger.info(f"Fetching data from table: {table_name}")
        
        # Connect to database
        connection = connect_to_mysql()
        if not connection:
            logger.error("Database connection failed")
            return jsonify({"error": "Failed to connect to database"}), 500

        cursor = connection.cursor(dictionary=True)  # Return results as dictionaries
        
        # First check if table exists
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = %s
        """, (table_name,))
        
        if cursor.fetchone()['count'] == 0:
            logger.error(f"Table {table_name} does not exist")
            return jsonify({"error": f"Table {table_name} does not exist"}), 404

        # Get column names
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [column['Field'] for column in cursor.fetchall()]
        logger.debug(f"Columns in table: {', '.join(columns)}")

        # Fetch all data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Convert any non-serializable objects to strings
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (datetime, date)):
                    row[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    row[key] = float(value)

        result = {
            "columns": columns,
            "data": rows,
            "total_rows": len(rows)
        }
        
        logger.info(f"Successfully retrieved {len(rows)} rows from {table_name}")
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error retrieving data from {table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Database connection closed")


@app.route("/getMappingTables", methods=["GET"])
@cross_origin(origins="*")
def get_mapping_tables():
    """
    API endpoint to get all available mapping table names in the database.
    Returns:
        JSON response with list of table names or error message
    """
    connection = None
    cursor = None
    try:
        logger.info("Fetching list of mapping tables")
        
        # Connect to database
        connection = connect_to_mysql()
        if not connection:
            logger.error("Database connection failed")
            return jsonify({"error": "Failed to connect to database"}), 500

        cursor = connection.cursor(dictionary=True)
        
        # Get all tables that might be mapping tables
        # You can modify this query based on your naming convention
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE()
            AND table_name LIKE '%mapping%'
            OR table_name IN (
                SELECT DISTINCT table_name 
                FROM information_schema.columns 
                WHERE column_name = 'mapping_id'
                AND table_schema = DATABASE()
            )
        """)
        
        tables = [table['table_name'] for table in cursor.fetchall()]
        
        result = {
            "tables": tables,
            "total_tables": len(tables)
        }
        
        logger.info(f"Found {len(tables)} mapping tables")
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error retrieving table list: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Database connection closed")


# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8081)
