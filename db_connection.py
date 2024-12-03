import mysql.connector
from mysql.connector import Error
import yaml
import logging
import os

logger = logging.getLogger(__name__)

def load_db_config():
    """Loads the database configuration from the YAML file."""
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_config.yml")
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        db_config = config["database"]
        # Ensure port is an integer
        if "port" in db_config:
            db_config["port"] = int(db_config["port"])
        return db_config
    except Exception as e:
        logger.error(f"Error loading database configuration: {str(e)}")
        logger.error(f"Tried to load config from: {config_path}")
        raise

def connect_to_mysql():
    """
    Establishes a connection to the MySQL database using the YAML configuration.
    Returns the connection object for further operations.
    """
    try:
        db_config = load_db_config()
        logger.debug(f"Attempting to connect to database at {db_config['host']}:{db_config['port']}")
        
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"],
            user=db_config["user"],
            password=db_config["password"]
        )

        if connection.is_connected():
            logger.info("Connection to MySQL database was successful!")
            db_info = connection.get_server_info()
            logger.debug(f"Connected to MySQL Server version {db_info}")
            return connection
        else:
            logger.error("Failed to establish database connection")
            return None

    except Error as e:
        logger.error(f"Error while connecting to MySQL: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while connecting to MySQL: {str(e)}")
        return None
