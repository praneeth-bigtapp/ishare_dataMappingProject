import mysql.connector
from mysql.connector import Error
import yaml
import logging
import os
import traceback
import socket

logger = logging.getLogger(__name__)

def load_db_config():
    """
    Load database configuration from YAML file.
    Returns a dictionary with database connection parameters.
    """
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db_config.yml')
        logger.info(f"Loading database config from: {config_path}")
        
        if not os.path.exists(config_path):
            logger.error(f"Config file does not exist at path: {config_path}")
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            
        logger.info(f"Loaded raw config: {config}")
            
        if not config or 'database' not in config:
            logger.error(f"Invalid config structure. Config contents: {config}")
            raise ValueError("Invalid database configuration format - missing 'database' key")
            
        db_config = config['database']
        required_keys = ['host', 'port', 'name', 'user', 'password']
        
        # Verify all required keys exist and have values
        for key in required_keys:
            if key not in db_config:
                logger.error(f"Missing required key '{key}' in database config")
                raise ValueError(f"Missing required database configuration key: {key}")
            if not db_config[key]:
                logger.error(f"Empty value for key '{key}' in database config")
                raise ValueError(f"Empty value for required key: {key}")
                
        # Log the actual values being used
        logger.info(f"Using database configuration:")
        logger.info(f"  Host: {db_config['host']}")
        logger.info(f"  Port: {db_config['port']}")
        logger.info(f"  Database: {db_config['name']}")
        logger.info(f"  User: {db_config['user']}")
        
        return db_config
        
    except FileNotFoundError:
        logger.error(f"Database configuration file not found at: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing database configuration YAML: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading database configuration: {str(e)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        raise

def test_connection(host, port):
    """Test if we can reach the database server"""
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        return True
    except Exception as e:
        logger.error(f"Network connectivity test failed: {str(e)}")
        return False

def connect_to_mysql():
    """
    Establishes a connection to the MySQL database using the YAML configuration.
    Returns the connection object for further operations.
    """
    try:
        # Load configuration
        db_config = load_db_config()
        
        # Create connection
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['name'],
            port=int(db_config.get('port', 3306)),
            connect_timeout=30
        )
        
        if connection.is_connected():
            # Test the connection by running a simple query
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            logger.info("Successfully connected to MySQL database")
            return connection
            
    except mysql.connector.Error as err:
        logger.error(f"MySQL Error: {err}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None
