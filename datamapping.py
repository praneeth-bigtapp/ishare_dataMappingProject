from db_connection import connect_to_mysql
import pandas as pd
import logging
import re
import traceback

logger = logging.getLogger(__name__)

def sanitize_column_name(name):
    """Sanitize column names for SQL table creation"""
    try:
        # Remove special characters and spaces, replace with underscore
        sanitized = re.sub(r'[^a-zA-Z0-9]', '_', str(name))
        # Ensure it starts with a letter
        if sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        return sanitized.lower()
    except Exception as e:
        logger.error(f"Error sanitizing column name '{name}': {str(e)}")
        raise

def create_mapping_table_from_excel(file_path, table_name):
    """
    Creates a mapping table based on the Excel file structure.
    The table name will be derived from the Excel filename.
    Only adds mapping_id if it's not present in the Excel.
    """
    connection = None
    cursor = None
    try:
        logger.info(f"Reading Excel file: {file_path}")
        # Read the Excel file
        data = pd.read_excel(file_path)
        
        logger.info(f"Excel columns found: {', '.join(data.columns)}")
        
        # Verify if tpa_id exists in the Excel columns
        if 'tpa_id' not in data.columns:
            logger.error("Required column 'tpa_id' not found in Excel file")
            return {"error": "Excel file must contain 'tpa_id' column"}

        # Connect to the database
        logger.info("Attempting database connection")
        connection = connect_to_mysql()
        if not connection:
            logger.error("Database connection failed")
            return {"error": "Failed to connect to database. Please check database configuration and connectivity."}

        cursor = connection.cursor()

        # Create column definitions for the table
        logger.info(f"Creating table structure for {table_name}")
        
        # Initialize columns list
        columns = []
        
        # Check if mapping_id exists in Excel
        has_mapping_id = 'mapping_id' in [col.lower() for col in data.columns]
        if not has_mapping_id:
            # Add mapping_id only if it's not in Excel
            columns.append("mapping_id INT AUTO_INCREMENT PRIMARY KEY")
        
        # Process Excel columns
        excel_columns = []  # Keep track of columns for INSERT
        processed_columns = set()  # Avoid duplicates
        
        for col in data.columns:
            sanitized_col = sanitize_column_name(col)
            if sanitized_col not in processed_columns:
                # Special handling for existing mapping_id
                if sanitized_col.lower() == 'mapping_id':
                    columns.append(f"{sanitized_col} INT AUTO_INCREMENT PRIMARY KEY")
                # Special handling for tpa_id
                elif sanitized_col.lower() == 'tpa_id':
                    columns.append(f"{sanitized_col} INT NOT NULL")
                else:
                    columns.append(f"{sanitized_col} VARCHAR(255)")
                
                processed_columns.add(sanitized_col)
                excel_columns.append(col)  # Keep original column name for data insertion
                logger.debug(f"Added column: {sanitized_col}")

        # Add timestamp columns
        columns.extend([
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ])

        # Create the table
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)},
                CONSTRAINT fk_ftpa_id FOREIGN KEY (tpa_id) REFERENCES ftpa_master(ftp_id)
            );
        """
        logger.debug(f"Table creation SQL: {create_table_sql}")
        
        try:
            cursor.execute(create_table_sql)
            logger.info(f"Table {table_name} created successfully")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            logger.error(f"SQL that failed: {create_table_sql}")
            raise

        # Prepare insert statement (excluding mapping_id if auto-generated)
        insert_columns = [sanitize_column_name(col) for col in excel_columns]
        if has_mapping_id and 'mapping_id' in insert_columns:
            # If mapping_id is in Excel, include it in the insert
            placeholders = ', '.join(['%s'] * len(insert_columns))
        else:
            # If mapping_id is auto-generated, exclude it from insert
            insert_columns = [col for col in insert_columns if col.lower() != 'mapping_id']
            placeholders = ', '.join(['%s'] * len(insert_columns))

        # Insert data
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(insert_columns)})
            VALUES ({placeholders});
        """
        logger.debug(f"Insert SQL template: {insert_sql}")

        # Prepare and execute insert statements
        logger.info(f"Starting data insertion for {len(data)} rows")
        rows_processed = 0
        for _, row in data.iterrows():
            try:
                # Prepare values based on whether mapping_id is included
                if has_mapping_id:
                    values = [row[col] for col in excel_columns]
                else:
                    values = [row[col] for col in excel_columns if sanitize_column_name(col).lower() != 'mapping_id']
                
                cursor.execute(insert_sql, values)
                rows_processed += 1
                if rows_processed % 100 == 0:  # Log progress every 100 rows
                    logger.info(f"Processed {rows_processed} rows")
            except Exception as e:
                logger.error(f"Error inserting row {rows_processed + 1}: {str(e)}")
                logger.error(f"Values that failed: {values}")
                raise

        # Commit the transaction
        connection.commit()
        logger.info(f"Successfully inserted {rows_processed} rows into {table_name}")
        return {"message": f"Mapping table '{table_name}' created and populated successfully"}

    except Exception as e:
        logger.error(f"Error in create_mapping_table_from_excel: {str(e)}")
        logger.error(traceback.format_exc())
        if connection:
            connection.rollback()
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Database connection closed")

def upload_mapping(file_path, table_name):
    """
    Handle the upload of a mapping Excel file and create corresponding database table
    """
    try:
        result = create_mapping_table_from_excel(file_path, table_name)
        return result
    except Exception as e:
        return {"error": f"Failed to process mapping file: {str(e)}"}
