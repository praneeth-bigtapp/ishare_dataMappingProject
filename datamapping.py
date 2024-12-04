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

def parse_column_mappings(column_name):
    """Parse column name into source and target mappings."""
    parts = [p.strip() for p in column_name.split(',')]
    source_col = parts[0]
    target_cols = parts[1:] if len(parts) > 1 else []
    return source_col, target_cols

def validate_column_mapping(source_col, target_cols, target_table_schema):
    """Validate if the column mappings are valid according to target schema."""
    invalid_cols = []
    for col in target_cols:
        if col not in target_table_schema:
            invalid_cols.append(col)
    return invalid_cols

def create_mapping_table_from_excel(file_path, table_name):
    """
    Creates a mapping table based on the Excel file structure.
    Handles multiple column mappings and data validation.
    """
    connection = None
    cursor = None
    try:
        logger.info(f"Reading Excel file: {file_path}")
        data = pd.read_excel(file_path)
        
        logger.info(f"Excel columns found: {', '.join(data.columns)}")
        
        # Parse all column mappings first
        column_mappings = {}
        unmapped_columns = []
        for col in data.columns:
            if ',' in col:
                source_col, target_cols = parse_column_mappings(col)
                sanitized_source = sanitize_column_name(source_col)
                sanitized_targets = [sanitize_column_name(tc) for tc in target_cols]
                column_mappings[sanitized_source] = sanitized_targets
            else:
                unmapped_columns.append(sanitize_column_name(col))
        
        logger.info(f"Found mappings: {column_mappings}")
        logger.info(f"Unmapped columns: {unmapped_columns}")

        # Verify required columns
        if 'tpa_id' not in data.columns:
            logger.error("Required column 'tpa_id' not found in Excel file")
            return {"error": "Excel file must contain 'tpa_id' column"}

        # Connect to database
        connection = connect_to_mysql()
        if not connection:
            return {"error": "Failed to connect to database"}

        cursor = connection.cursor()

        # Create columns list for table creation
        columns = []
        processed_columns = set()
        
        # Add mapping_id if not present
        has_mapping_id = 'mapping_id' in [col.lower() for col in data.columns]
        if not has_mapping_id:
            columns.append("mapping_id INT AUTO_INCREMENT PRIMARY KEY")
        
        # Process all columns including mapped ones
        for col in data.columns:
            if ',' in col:
                source_col, _ = parse_column_mappings(col)
                sanitized_col = sanitize_column_name(source_col)
            else:
                sanitized_col = sanitize_column_name(col)
            
            if sanitized_col not in processed_columns:
                if sanitized_col.lower() == 'mapping_id' and has_mapping_id:
                    columns.append(f"{sanitized_col} INT AUTO_INCREMENT PRIMARY KEY")
                elif sanitized_col.lower() == 'tpa_id':
                    columns.append(f"{sanitized_col} INT NOT NULL")
                else:
                    columns.append(f"{sanitized_col} VARCHAR(255)")
                processed_columns.add(sanitized_col)
                
                # Add columns for mapped fields if they don't exist
                if sanitized_col in column_mappings:
                    for target_col in column_mappings[sanitized_col]:
                        if target_col not in processed_columns:
                            columns.append(f"{target_col} VARCHAR(255)")
                            processed_columns.add(target_col)

        # Add timestamp columns
        columns.extend([
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ])

        # Create table
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            );
        """
        
        try:
            cursor.execute(create_table_sql)
            logger.info(f"Table {table_name} created successfully")

            # Add foreign key constraint with unique name
            add_constraint_sql = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT `fk_{table_name}_tpa_id` 
                FOREIGN KEY (tpa_id) 
                REFERENCES ftpa_master(ftp_id);
            """
            try:
                cursor.execute(add_constraint_sql)
                logger.info(f"Foreign key constraint added to {table_name}")
            except Exception as e:
                if e.errno == 1826:  # Duplicate foreign key constraint
                    logger.warning(f"Foreign key constraint already exists on {table_name}")
                else:
                    raise
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise

        # Prepare columns for insert
        insert_columns = []
        for col in data.columns:
            if ',' in col:
                source_col, target_cols = parse_column_mappings(col)
                sanitized_source = sanitize_column_name(source_col)
                insert_columns.append(sanitized_source)
                insert_columns.extend([sanitize_column_name(tc) for tc in target_cols])
            else:
                sanitized_col = sanitize_column_name(col)
                if sanitized_col.lower() != 'mapping_id' or has_mapping_id:
                    insert_columns.append(sanitized_col)

        placeholders = ', '.join(['%s'] * len(insert_columns))

        # Insert data
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(insert_columns)})
            VALUES ({placeholders});
        """

        rows_processed = 0
        errors = []
        for _, row in data.iterrows():
            try:
                values = []
                for col in data.columns:
                    if ',' in col:
                        source_col, target_cols = parse_column_mappings(col)
                        # Add source column value
                        values.append(row[source_col])
                        # Add same value for all target columns
                        values.extend([row[source_col]] * len(target_cols))
                    else:
                        sanitized_col = sanitize_column_name(col)
                        if sanitized_col.lower() != 'mapping_id' or has_mapping_id:
                            values.append(row[col])
                
                cursor.execute(insert_sql, values)
                rows_processed += 1
                if rows_processed % 100 == 0:
                    logger.info(f"Processed {rows_processed} rows")
            except Exception as e:
                error_msg = f"Error in row {rows_processed + 1}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Row data: {row.to_dict()}")
                errors.append(error_msg)
                continue

        connection.commit()
        
        result = {
            "message": f"Successfully processed {rows_processed} rows",
            "table_name": table_name,
            "mappings": column_mappings,
            "unmapped_columns": unmapped_columns
        }
        
        if errors:
            result["warnings"] = errors
            
        return result

    except Exception as e:
        logger.error(f"Error in create_mapping_table_from_excel: {str(e)}")
        logger.error(traceback.format_exc())
        if connection:
            connection.rollback()
        return {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
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
