from db_connection import connect_to_mysql
import pandas as pd


def upload_data_with_mapping(file_path, mapping_table, target_table):
    """
    Uploads data from an Excel file to the target database table based on a mapping table.
    Only new rows (not already present in the target table) are inserted, and duplicates are handled gracefully.
    Arguments:
    - file_path: Path to the uploaded Excel file.
    - mapping_table: Name of the mapping table in the database.
    - target_table: Name of the target table in the database.
    """
    try:
        # Read the Excel file
        data = pd.read_excel(file_path)

        # Connect to the database
        connection = connect_to_mysql()
        if not connection:
            return {"error": "Database connection failed."}

        cursor = connection.cursor()

        # Validate the mapping table exists and fetch mappings
        cursor.execute(f"SHOW TABLES LIKE '{mapping_table}'")
        if not cursor.fetchone():
            return {"error": f"Mapping table '{mapping_table}' does not exist in the database."}

        cursor.execute(f"SELECT source_column_name, target_column_name FROM {mapping_table}")
        mappings = cursor.fetchall()
        if not mappings:
            return {"error": f"No mappings found in the table '{mapping_table}'."}

        # Extract source-to-target column mappings
        column_mapping = {row[0]: row[1] for row in mappings}

        # Validate the target table exists
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        if not cursor.fetchone():
            return {"error": f"Target table '{target_table}' does not exist in the database."}

        # Validate that all source columns exist in the Excel file
        missing_columns = [col for col in column_mapping.keys() if col not in data.columns]
        if missing_columns:
            return {"error": f"The following columns are missing in the Excel file: {', '.join(missing_columns)}"}

        # Prepare transformed data for insertion
        transformed_data = data[list(column_mapping.keys())].rename(columns=column_mapping)

        # Insert transformed data into the target table using `ON DUPLICATE KEY UPDATE`
        inserted_rows = 0
        for _, row in transformed_data.iterrows():
            columns = ", ".join(row.index)
            placeholders = ", ".join(["%s"] * len(row))
            update_clause = ", ".join([f"{col} = VALUES({col})" for col in row.index])
            insert_sql = f"""
                INSERT INTO {target_table} ({columns}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            cursor.execute(insert_sql, tuple(row))
            if cursor.rowcount == 1:  # Check if it was an actual insertion
                inserted_rows += 1

        # Commit the transaction
        connection.commit()
        return {"message": f"{inserted_rows} new rows successfully uploaded to table '{target_table}'."}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if "cursor" in locals():
            cursor.close()
        if "connection" in locals():
            connection.close()
