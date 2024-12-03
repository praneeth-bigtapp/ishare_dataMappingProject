from db_connection import connect_to_mysql
import pandas as pd
import os
from datetime import datetime

def upload_data_with_mapping(file_path, mapping_table):
    """
    Creates a table using Excel filename and uploads data using mapping configuration.
    Only processes source columns from the mapping table that exist in the Excel file.
    
    Args:
        file_path: Path to the uploaded Excel file
        mapping_table: Name of the mapping table (e.g. vidal_mapping)
    """
    try:
        # Get table name from Excel filename (remove extension)
        table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
        # Replace spaces and special characters with underscore
        table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
        
        # Read the Excel file
        source_data = pd.read_excel(file_path)
        print(f"Excel columns: {list(source_data.columns)}")
        
        # Connect to database
        connection = connect_to_mysql()
        if not connection:
            return {"error": "Database connection failed"}
            
        cursor = connection.cursor()
        
        # Get mapping configuration
        mapping_query = f"""
            SELECT source_column, target_column, target_database, target_table 
            FROM {mapping_table}
        """
        cursor.execute(mapping_query)
        mappings = cursor.fetchall()
        print(f"Mappings from DB: {mappings}")
        
        if not mappings:
            return {"error": f"No mappings found in {mapping_table}"}
            
        # Get available source columns from Excel
        excel_columns = set(source_data.columns)
        
        # Filter mappings to only include source columns that exist in Excel
        valid_mappings = []
        for mapping in mappings:
            source_col = mapping[0]  # source_column
            if source_col in excel_columns:
                valid_mappings.append((source_col, mapping[1]))  # mapping[1] is target_column
        
        print(f"Valid mappings: {valid_mappings}")
        
        if not valid_mappings:
            return {
                "error": "No valid source columns found in Excel file that match the mapping table",
                "excel_columns": list(excel_columns),
                "mapping_source_columns": [m[0] for m in mappings]
            }
            
        # Create source to target mapping from valid columns
        source_to_target = {src: tgt for src, tgt in valid_mappings}
        target_columns = list(source_to_target.values())
            
        # Check if table exists and drop it if it does
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.commit()
            
        # Create table with target columns
        create_table_query = f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(f'{col} VARCHAR(255)' for col in target_columns)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        # Prepare data for insertion
        insert_data = pd.DataFrame()
        for source_col, target_col in source_to_target.items():
            insert_data[target_col] = source_data[source_col]
        
        # Insert data into table
        inserted_rows = 0
        for _, row in insert_data.iterrows():
            # Filter out None/NaN values
            valid_data = {k: v for k, v in row.items() if pd.notna(v)}
            if not valid_data:
                continue
                
            columns = ', '.join(valid_data.keys())
            placeholders = ', '.join(['%s'] * len(valid_data))
            
            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
            """
            cursor.execute(insert_query, list(valid_data.values()))
            inserted_rows += 1
        
        connection.commit()
        
        # Find source columns from mapping table that weren't mapped
        mapped_source_columns = set(source_to_target.keys())
        all_mapping_source_columns = set(m[0] for m in mappings)
        unmapped_source_columns = all_mapping_source_columns - mapped_source_columns
        
        return {
            "message": f"Successfully created table '{table_name}' and mapped {inserted_rows} rows",
            "table_name": table_name,
            "mapped_columns": {
                "found": list(mapped_source_columns),
                "total_in_mapping": len(mappings),
                "total_mapped": len(valid_mappings)
            },
            "unmapped_source_columns": list(unmapped_source_columns)  # Source columns in mapping table that weren't found in Excel
        }
        
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
