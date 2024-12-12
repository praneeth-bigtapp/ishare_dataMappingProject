from datetime import datetime
import logging
from db_connection import connect_to_mysql
import mysql.connector.cursor


def format_date(value, column_name):
    """
    Convert date value to 'YYYY-MM-DD' format.
    Args:
        value (str): The date value to format.
        column_name (str): The column name being processed (for debugging).
    Returns:
        str: Formatted date in 'YYYY-MM-DD' or the original value if invalid.
    """
    if not value:
        return None
    try:
        # First, try DD/MM/YYYY format
        return datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            # Then, try YYYY-MM-DD format (already correct format)
            datetime.strptime(value, "%Y-%m-%d")  # Validate if it's valid
            return value
        except ValueError:
            raise Exception(f"Invalid date format for column '{column_name}': {value}")


def process_target_table(target_table, start_date=None, end_date=None):
    """
    Generic function to process data for a target table, with optional date filtering.
    Args:
        target_table (str): The name of the target table to process.
        start_date (str): Start date for filtering data (optional, format: YYYY-MM-DD).
        end_date (str): End date for filtering data (optional, format: YYYY-MM-DD).
    Returns:
        dict: Summary of the operation (e.g., rows processed, errors).
    """
    try:
        # Database connection and setup
        connection = connect_to_mysql()
        if not connection:
            raise Exception("Failed to connect to database")

        cursor = connection.cursor(dictionary=True)
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
            m for m in mappings
            if m['source_column'] in source_table_columns and m['target_column']
        ]

        # Build the SELECT query with optional date filters
        source_columns = ", ".join([m['source_column'] for m in valid_mappings])
        source_query = f"SELECT {source_columns} FROM claims_automation.{source_table}"

        conditions = []
        if start_date:
            conditions.append(
                f"DATE(column_name) >= '{start_date}'")  # Replace `column_name` with the actual column for date filtering
        if end_date:
            conditions.append(
                f"DATE(column_name) <= '{end_date}'")  # Replace `column_name` with the actual column for date filtering

        if conditions:
            source_query += " WHERE " + " AND ".join(conditions)

        cursor.execute(source_query)
        source_data = cursor.fetchall()

        insert_count = 0
        failed_rows = []

        # Insert logic with enhanced date handling
        for row in source_data:
            try:
                transformed_data = {}
                for mapping in valid_mappings:
                    source_column = mapping['source_column']
                    target_column = mapping['target_column']
                    transformation_logic = mapping['transformation_logic']

                    value = row.get(source_column)

                    if transformation_logic:
                        # Create a local variable for eval context
                        source = str(value) if value is not None else ''
                        try:
                            value = eval(transformation_logic, {"source": source})
                        except Exception as e:
                            logging.error(f"Error evaluating transformation logic for column {source_column}: {e}")
                            value = None

                    # Special handling for dates
                    if value and "date" in str(target_column).lower():
                        value = format_date(value, source_column)

                    transformed_data[target_column] = value

                # Insert transformed data into the target table
                target_columns = ", ".join(transformed_data.keys())
                placeholders = ", ".join(["%s"] * len(transformed_data))
                insert_query = f"INSERT INTO {target_table} ({target_columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, list(transformed_data.values()))
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
    Function to schedule a processing task and update the scheduler_details table.
    Args:
        scheduler_name (str): Name of the scheduler.
        cron_expression (str): Cron expression for scheduling.
        target_table (str): Target table name for processing.
        start_date (str): Start date for processing (optional).
        end_date (str): End date for processing (optional).
        process_logic (str): Optional process logic for scheduling (optional).
    Returns:
        dict: Result of scheduling operation.
    """
    try:
        connection = connect_to_mysql()
        if not connection:
            raise Exception("Failed to connect to database")

        cursor = connection.cursor(dictionary=True)
        status = "Scheduled"

        # Insert scheduler details into the scheduler_details table
        insert_query = """
            INSERT INTO scheduler_details (scheduler_name, start_date_time, end_date_time, cron_expression, status, target_table, process_logic)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query,
            (
                scheduler_name,
                start_date if start_date else None,  # Allow None for optional parameters
                end_date if end_date else None,      # Allow None for optional parameters
                cron_expression,
                status,
                target_table,
                process_logic if process_logic else None  # Set to None if not provided
            )
        )
        connection.commit()

        # Process the logic based on the scheduler
        process_result = process_target_table(target_table, start_date, end_date)

        # Update status in the scheduler_details table
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
