import sys
import os
import psycopg2
import configparser
import json
import logging
from datetime import datetime

# Constants for keys and formats
DB_PROPERTIES_FILE = 'db.properties'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

class Config:
    @staticmethod
    def get_db_properties_file():
        return DB_PROPERTIES_FILE

def check_keys(keys, section, prefix=""):
    """
    Check if the required keys are present in the given configuration section.

    Args:
        keys (list): List of required keys.
        section (configparser.SectionProxy): Configuration section to check.
        prefix (str, optional): Prefix to use for environment variables. Defaults to "".
    """
    for key in keys:
        env_key = f"{prefix}_{key}" if prefix else key
        if key not in section and env_key not in section:
            logging.error(f"Error: {env_key} not found in {section} section.")
            sys.exit(1)

def config():
    # Define required keys for archive and database connection
    required_archive_keys = ['ARCHIVE_DB_HOST', 'ARCHIVE_DB_PORT', 'ARCHIVE_DB_NAME', 'ARCHIVE_SCHEMA_NAME', 'ARCHIVE_DB_UNAME', 'ARCHIVE_DB_PASS']
    required_db_names_keys = ['DB_NAMES']

    # Initialize dictionaries to store parameters
    archive_param = {}
    source_param = {}
    db_names = []

    # Check if db.properties file exists
    if os.path.exists(Config.get_db_properties_file()):
        logging.info("Using database connection parameters from db.properties.")
        config_parser = configparser.ConfigParser()
        config_parser.read(Config.get_db_properties_file())

        # Check if all required keys are present in ARCHIVE section
        check_keys(required_archive_keys, config_parser['ARCHIVE'])

        # Check if required keys are present in Databases section
        check_keys(required_db_names_keys, config_parser['Databases'])

        # Extract archive parameters and database names from the config file
        archive_param = {key.upper(): config_parser['ARCHIVE'][key] for key in config_parser['ARCHIVE']}
        db_names = [name.strip() for name in config_parser.get('Databases', 'DB_NAMES').split(',')]

        # Extract source parameters for each database
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, config_parser[db_name], prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=config_parser, env_vars=os.environ, db_name=db_name)
    else:
        logging.error("Error: db.properties file not found. Using environment variables.")
        check_keys(required_archive_keys, os.environ)

        # Extract database names from environment variables
        db_names_env = os.environ.get('DB_NAMES')
        if db_names_env is not None:
            db_names = [name.strip() for name in db_names_env.split(',')]
        else:
            logging.error("Error: DB_NAMES not found in environment variables.")
            sys.exit(1)

        # Extract source parameters for each database from environment variables
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, os.environ, prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=None, env_vars=os.environ, db_name=db_name)

    # Return extracted parameters
    return db_names, archive_param, source_param

def create_source_param(config_parser, env_vars, db_name):
    """
    Create source parameters for a specific database.

    Args:
        config_parser (configparser.ConfigParser, optional): Configuration parser. Defaults to None.
        env_vars (dict): Environment variables.
        db_name (str): Database name.

    Returns:
        dict: Source parameters.
    """
    param_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
    source_param = {}

    for key in param_keys:
        env_key = f'{db_name}_{key}'
        source_param[env_key] = env_vars.get(env_key) or config_parser.get(db_name, env_key)

    return source_param

def get_tablevalues(row):
    """
    Get formatted values for a row in a table.

    Args:
        row (list): Row data.

    Returns:
        str: Formatted values.
    """
    final_values = ""
    for value in row:
        if value is None:
            final_values += "NULL,"
        else:
            final_values += f"'{value}',"
    final_values = final_values[:-1]
    return final_values

def read_tables_info(db_name):
    """
    Read table information from a JSON file or environment variable.

    Args:
        db_name (str): Database name.

    Returns:
        list: List of table information dictionaries.
    """
    try:
        with open(f'{db_name.lower()}_archive_table_info.json') as f:
            tables_info = json.load(f)
            logging.info(f"{db_name.lower()}_archive_table_info.json file found and loaded.")
            return tables_info['tables_info']
    except FileNotFoundError:
        logging.error(f"{db_name.lower()}_archive_table_info.json file not found. Using environment variables.")
        tables_info = os.environ.get(f"{db_name.lower()}_archive_table_info")
        if tables_info is None:
            logging.error(f"Environment variable {db_name.lower()}_archive_table_info not found.")
            sys.exit(1)
        return json.loads(tables_info)['tables_info']

def data_archive(db_name, db_param, tables_info):
    """
    Archive data from a source database to an archive database.

    Args:
        db_name (str): Database name.
        db_param (dict): Database connection parameters.
        tables_info (list): List of table information dictionaries.
    """
    try:
        # Connect to source and archive databases using context managers
        print("db_param:", db_param)
        logging.info("db_param: %s", db_param)
        with psycopg2.connect(
                user=db_param[f"{db_name}_SOURCE_DB_UNAME"],
                password=db_param[f"{db_name}_SOURCE_DB_PASS"],
                host=db_param[f"{db_name}_SOURCE_DB_HOST"],
                port=db_param[f"{db_name}_SOURCE_DB_PORT"],
                database=db_param[f"{db_name}_SOURCE_DB_NAME"]
        ) as source_conn, source_conn.cursor() as source_cur,\
             psycopg2.connect(
                user=db_param["ARCHIVE_DB_UNAME"],
                password=db_param["ARCHIVE_DB_PASS"],
                host=db_param["ARCHIVE_DB_HOST"],
                port=db_param["ARCHIVE_DB_PORT"],
                database=db_param["ARCHIVE_DB_NAME"]
        ) as archive_conn, archive_conn.cursor() as archive_cur:

            sschema_name = db_param[f"{db_name}_SOURCE_SCHEMA_NAME"]
            aschema_name = db_param["ARCHIVE_SCHEMA_NAME"]

            for table_info in tables_info:
                source_table_name = table_info['source_table']
                archive_table_name = table_info['archive_table']
                id_column = table_info['id_column']
                if 'date_column' in table_info and 'older_than_days' in table_info:
                    date_column = table_info['date_column']
                    older_than_days = table_info['older_than_days']
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} WHERE {date_column} < NOW() - INTERVAL '{older_than_days} days'"
                else:
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name}"
                source_cur.execute(select_query)
                rows = source_cur.fetchall()
                select_count = source_cur.rowcount
                logging.info(f"{select_count} Record(s) selected for archive from {source_table_name} from source database {db_name}")

                if select_count > 0:
                    for row in rows:
                        row_values = get_tablevalues(row)
                        insert_query = f"INSERT INTO {aschema_name}.{archive_table_name} VALUES ({row_values}) ON CONFLICT DO NOTHING"
                        archive_cur.execute(insert_query)
                        archive_conn.commit()
                        insert_count = archive_cur.rowcount
                        if insert_count == 0:
                            logging.warning(f"Skipping duplicate record with ID: {row[0]} in table {archive_table_name} from source database {db_name}")
                        else:
                            logging.info(f"{insert_count} Record(s) inserted successfully for table {archive_table_name} from source database {db_name}")
                        delete_query = f'DELETE FROM "{sschema_name}"."{source_table_name}" WHERE "{id_column}" = %s'
                        source_cur.execute(delete_query, (row[0],))
                        source_conn.commit()
                        delete_count = source_cur.rowcount
                        logging.info(f"{delete_count} Record(s) deleted successfully for table {source_table_name} from source database {db_name}")

    except (Exception, psycopg2.DatabaseError) as error:
        # Handle exceptions during the data archiving process
        logging.error("Error during data archiving:", error)

if __name__ == '__main__':
    # Configure logging settings
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Get database names, archive parameters, and source parameters
    db_names, archive_param, source_param = config()
    
    # Process each source database
    for db_name in db_names:
        # Combine source and archive parameters
        db_param = source_param[db_name]
        db_param.update(archive_param)
        
        # Read table information
        tables_info = read_tables_info(db_name)
        
        # Archive data for the current source database
        data_archive(db_name, db_param, tables_info)
