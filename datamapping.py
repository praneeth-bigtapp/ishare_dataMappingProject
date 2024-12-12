from db_connection import connect_to_mysql
import pandas as pd
import logging
import traceback

logger = logging.getLogger(__name__)

def upload_mapping(file_path):
    """
    Upload mappings from Excel file to the mapping_table.
    Required columns in Excel:
    - tpa_id
    - source_database
    - source_table
    - source_column
    - target_database
    - target_table
    - target_column
    - transformation_logic
    """
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} rows from Excel file")
        
        # Validate required columns
        required_columns = [
            'tpa_id', 'source_database', 'source_table',
            'source_table', 'target_database', 'target_table',
            'target_column', 'transformation_logic'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                "error": f"Missing required columns: {', '.join(missing_columns)}",
                "status": "error"
            }
            
        connection = connect_to_mysql()
        if not connection:
            return {"error": "Database connection failed"}
            
        cursor = connection.cursor()
        
        # Create mapping_table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS mapping_table (
            mapping_id INT AUTO_INCREMENT PRIMARY KEY,
            tpa_id INT NOT NULL,
            source_database VARCHAR(255) NOT NULL,
            source_table VARCHAR(255) NOT NULL,
            source_column VARCHAR(255) NOT NULL,
            target_database VARCHAR(255) NOT NULL,
            target_table VARCHAR(255) NOT NULL,
            target_column VARCHAR(255) NOT NULL,
            transformation_logic TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        
        # Insert mappings
        inserted_count = 0
        errors = []
        
        for idx, row in df.iterrows():
            try:
                # Prepare insert data
                insert_data = {
                    'tpa_id': row['tpa_id'],
                    'source_database': row['source_database'],
                    'source_table': row['source_table'],
                    'source_column': row['source_column'],
                    'target_database': row['target_database'],
                    'target_table': row['target_table'],
                    'target_column': row['target_column'],
                    'transformation_logic': row['transformation_logic']  
                }
                
                # Generate SQL
                columns = ', '.join(f"`{col}`" for col in insert_data.keys())
                placeholders = ', '.join(['%s'] * len(insert_data))
                insert_sql = f"""
                INSERT INTO mapping_table ({columns})
                VALUES ({placeholders})
                """
                
                cursor.execute(insert_sql, list(insert_data.values()))
                connection.commit()
                inserted_count += 1
                
            except Exception as e:
                error_msg = f"Error in row {idx + 1}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue
        
        result = {
            "message": f"Successfully uploaded {inserted_count} mappings",
            "total_rows": len(df),
            "inserted_rows": inserted_count,
            "status": "success"
        }
        
        if errors:
            result["errors"] = errors
            result["status"] = "partial_success"
            
        return result
        
    except Exception as e:
        error_msg = f"Error uploading mappings: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return {"error": error_msg, "status": "error"}
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def get_vidal_mappings(cursor):
    """
    Get all mappings from mapping_table where target_table is 'temp_vidal_claims'
    Returns a dictionary mapping source columns to target columns
    """
    query = """
    SELECT source_column, target_column, transformation_logic
    FROM mapping_table 
    WHERE target_table = 'temp_vidal_claims'
    """
    cursor.execute(query)
    mappings = cursor.fetchall()
    
    # Create a mapping dictionary for easy lookup
    mapping_dict = {}
    for source_col, target_col, transform_logic in mappings:
        mapping_dict[source_col] = {
            'target_columns': [col.strip() for col in target_col.split(',')] if target_col else [],
            'transformation_logic': transform_logic
        }
    
    logger.info(f"Found {len(mapping_dict)} mappings for temp_vidal_claims table")
    return mapping_dict

def upload_vidal_data(file_path):
    """
    Upload Vidal claims data from Excel file using mappings from mapping_table.
    Process:
    1. Read Excel file
    2. Get mappings from mapping_table
    3. For each row:
        - Map source columns to target columns
        - Apply any transformation logic
        - Insert into temp_vidal_claims
    Args:
        file_path (str): Path to the Excel file containing Vidal claims data
    Returns:
        dict: Result of the upload operation with status and details
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} rows from Excel file")
        
        connection = connect_to_mysql()
        if not connection:
            return {"error": "Database connection failed", "status": "error"}
            
        cursor = connection.cursor()
        
        # Get mappings
        mappings = get_vidal_mappings(cursor)
        if not mappings:
            return {
                "error": "No mappings found in mapping_table for temp_vidal_claims",
                "status": "error"
            }
        
        # Check which columns are missing but continue processing
        available_columns = set(df.columns)
        all_mapped_columns = set(mappings.keys())
        missing_columns = all_mapped_columns - available_columns
        usable_columns = all_mapped_columns & available_columns
        
        if not usable_columns:
            return {
                "error": "No mapped columns found in Excel file",
                "status": "error"
            }
        
        # Insert data into temp_vidal_claims
        inserted_count = 0
        errors = []
        
        for idx, row in df.iterrows():
            try:
                # Collect values for this row
                insert_data = {}
                
                for source_col in usable_columns:
                    mapping_info = mappings[source_col]
                    value = str(row[source_col])
                    
                    # Apply transformation logic if exists
                    if mapping_info['transformation_logic']:
                        try:
                            # Here you can add specific transformation logic
                            # For example: date formatting, string operations, etc.
                            pass
                        except Exception as e:
                            logger.warning(f"Transformation failed for {source_col}: {str(e)}")
                    
                    # Add value for each target column
                    for target_col in mapping_info['target_columns']:
                        insert_data[target_col.strip()] = value
                
                if insert_data:
                    # Generate SQL for insert
                    columns = ', '.join([f"`{col}`" for col in insert_data.keys()])
                    placeholders = ', '.join(['%s'] * len(insert_data))
                    insert_sql = f"""
                    INSERT INTO temp_vidal_claims ({columns})
                    VALUES ({placeholders})
                    """
                    cursor.execute(insert_sql, list(insert_data.values()))
                    connection.commit()
                    inserted_count += 1
                
            except Exception as e:
                error_msg = f"Error in row {idx + 1}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue
        
        result = {
            "message": f"Successfully uploaded {inserted_count} rows to temp_vidal_claims",
            "total_rows": len(df),
            "inserted_rows": inserted_count,
            "processed_columns": list(usable_columns),
            "missing_columns": list(missing_columns),
            "status": "success" if not missing_columns else "partial_success"
        }
        
        if errors:
            result["errors"] = errors
            result["status"] = "partial_success"
            
        return result
        
    except Exception as e:
        error_msg = f"Error uploading Vidal data: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return {"error": error_msg, "status": "error"}
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
