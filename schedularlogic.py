from datetime import datetime
import logging
from db_connection import connect_to_mysql
import mysql.connector.cursor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def format_date(value, column_name):
    """
    Convert date value to 'YYYY-MM-DD' format.
    Args:
        value (str): The date value to format.
        column_name (str): The column name being processed (for debugging).
    Returns:
        str: Formatted date in 'YYYY-MM-DD' or None if invalid.
    """
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            datetime.strptime(value, "%Y-%m-%d")  # Validate already correct format
            return value
        except ValueError:
            logging.error(f"Invalid date format for column '{column_name}': {value}")
            return None

def process_target_table(target_table, start_date=None, end_date=None):
    """
    Process data for a target table, with optional date filtering.
    Args:
        target_table (str): Target table to process.
        start_date (str): Start date for filtering data.
        end_date (str): End date for filtering data.
    Returns:
        dict: Summary of the operation.
    """
    try:
        connection = connect_to_mysql()
        if not connection:
            raise Exception("Failed to connect to database")

        cursor = connection.cursor(dictionary=True)

        # Retrieve column details dynamically from the target table
        cursor.execute(f"DESCRIBE {target_table}")
        table_columns = {row['Field']: row['Type'] for row in cursor.fetchall()}

        mapping_query = """
            SELECT source_column, target_column, transformation_logic, source_table 
            FROM mapping_table 
            WHERE target_table = %s
        """
        cursor.execute(mapping_query, (target_table,))
        mappings = cursor.fetchall()

        if not mappings:
            raise Exception(f"No mappings found for target table '{target_table}'")

        source_table = mappings[0]['source_table']
        cursor.execute(f"DESCRIBE {source_table}")
        source_table_columns = [row['Field'] for row in cursor.fetchall()]

        valid_mappings = [
            m for m in mappings if m['source_column'] in source_table_columns and m['target_column']
        ]

        source_columns = ", ".join([m['source_column'] for m in valid_mappings])
        source_query = f"SELECT {source_columns} FROM {source_table}"

        conditions = []
        if start_date:
            conditions.append(f"DATE(column_name) >= '{start_date}'")  # Replace `column_name` for filtering
        if end_date:
            conditions.append(f"DATE(column_name) <= '{end_date}'")

        if conditions:
            source_query += " WHERE " + " AND ".join(conditions)

        cursor.execute(source_query)
        source_data = cursor.fetchall()

        insert_count = 0
        failed_rows = []

        for row in source_data:
            try:
                transformed_data = {}
                for mapping in valid_mappings:
                    source_column = mapping['source_column']
                    target_column = mapping['target_column']
                    transformation_logic = mapping['transformation_logic']

                    value = row.get(source_column)

                    if transformation_logic:
                        source = str(value) if value is not None else ''
                        try:
                            value = eval(transformation_logic, {"source": source})
                        except Exception as e:
                            logging.error(f"Error in transformation logic for column '{source_column}': {e}")
                            value = None

                    # Handle specific column types
                    if target_column in table_columns:
                        column_type = table_columns[target_column].lower()
                        if "date" in column_type:
                            value = format_date(value, source_column)
                        elif "int" in column_type:
                            value = int(value) if value not in [None, 'nan', ''] else None
                        elif "decimal" in column_type:
                            value = float(value) if value not in [None, 'nan', ''] else None

                    transformed_data[target_column] = value

                # Match transformed_data keys with target table columns
                matched_columns = {
                    col: transformed_data[col]
                    for col in transformed_data.keys() if col in table_columns
                }

                if not matched_columns:
                    raise Exception("No matching columns between source and target table")

                target_columns = ", ".join(matched_columns.keys())
                placeholders = ", ".join(["%s"] * len(matched_columns))
                insert_query = f"INSERT INTO {target_table} ({target_columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, list(matched_columns.values()))
                insert_count += 1

            except Exception as e:
                failed_rows.append({"row": row, "error": str(e)})

        connection.commit()

        return {
            "message": f"{insert_count} rows inserted into {target_table}",
            "failed_rows": failed_rows,
        }

    except Exception as e:
        logging.error(f"Error processing {target_table}: {str(e)}")
        return {"error": str(e)}

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def schedule_processing(scheduler_name, cron_expression, target_table, start_date=None, end_date=None, process_logic=None):
    """
    Schedule a processing task and update the scheduler_details table.
    Args:
        scheduler_name (str): Scheduler name.
        cron_expression (str): Cron expression for scheduling.
        target_table (str): Target table to process.
        start_date (str): Start date for processing.
        end_date (str): End date for processing.
        process_logic (str): Process logic for scheduling (optional).
    Returns:
        dict: Result of the scheduling operation.
    """
    try:
        connection = connect_to_mysql()
        if not connection:
            raise Exception("Failed to connect to database")

        cursor = connection.cursor(dictionary=True)
        status = "Scheduled"

        insert_query = """
            INSERT INTO scheduler_details (scheduler_name, start_date_time, end_date_time, cron_expression, status, target_table, process_logic)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query,
            (
                scheduler_name,
                start_date if start_date else None,
                end_date if end_date else None,
                cron_expression,
                status,
                target_table,
                process_logic if process_logic else None
            )
        )
        connection.commit()

        process_result = process_target_table(target_table, start_date, end_date)

        update_query = """
            UPDATE scheduler_details
            SET status = 'Completed', end_date_time = %s
            WHERE scheduler_name = %s
        """
        cursor.execute(update_query, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), scheduler_name))
        connection.commit()

        return {
            "message": f"Scheduler '{scheduler_name}' executed successfully",
            "details": process_result
        }

    except Exception as e:
        logging.error(f"Error in schedule_processing: {str(e)}")
        return {"error": str(e)}

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
