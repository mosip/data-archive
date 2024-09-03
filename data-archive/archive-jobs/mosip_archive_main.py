import sys
import os
import psycopg2
import configparser
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("archive_script.log"),
    logging.StreamHandler(sys.stdout)
])

# Function to check if required keys are present in a section
def check_keys(keys, section, prefix=""):
    missing_keys = []
    for key in keys:
        env_key = f"{prefix}_{key}" if prefix else key
        if key not in section and env_key not in section:
            logging.error(f"{env_key} not found in {section} section.")
            missing_keys.append(key)
    if missing_keys:
        logging.error(f"Missing keys: {', '.join(missing_keys)}")
        sys.exit(1)

# Function to read configuration from file or environment variables
def config():
    # Define required keys for archive and database connection
    required_archive_keys = ['ARCHIVE_DB_HOST', 'ARCHIVE_DB_PORT', 'ARCHIVE_DB_NAME', 'ARCHIVE_SCHEMA_NAME', 'ARCHIVE_DB_UNAME', 'ARCHIVE_DB_PASS']
    required_db_names_keys = ['DB_NAMES']

    archive_param = {}
    source_param = {}
    db_names = []

    # Check if db.properties file exists
    if os.path.exists('db.properties'):
        logging.info("Using database connection parameters from db.properties.")
        config_parser = configparser.ConfigParser()
        config_parser.read('db.properties')

        # Check if all required keys are present in ARCHIVE section
        check_keys(required_archive_keys, config_parser['ARCHIVE'])

        # Check if required keys are present in Databases section
        check_keys(required_db_names_keys, config_parser['Databases'])

        # Extract archive parameters and database names from the config file
        archive_param = {key.upper(): config_parser['ARCHIVE'][key] for key in config_parser['ARCHIVE']}
        db_names = config_parser.get('Databases', 'DB_NAMES').split(',')
        db_names = [name.strip() for name in db_names]

        # Extract source parameters for each database
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, config_parser[db_name], prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=config_parser, env_vars=os.environ, db_name=db_name)
    else:
        # Handle case when db.properties file is not found
        logging.error("db.properties file not found. Using environment variables.")
        # Use environment variables
        archive_param = {
            'ARCHIVE_DB_HOST': os.environ.get('ARCHIVE_DB_HOST'),
            'ARCHIVE_DB_PORT': os.environ.get('ARCHIVE_DB_PORT'),
            'ARCHIVE_DB_NAME': os.environ.get('ARCHIVE_DB_NAME'),
            'ARCHIVE_SCHEMA_NAME': os.environ.get('ARCHIVE_SCHEMA_NAME'),
            'ARCHIVE_DB_UNAME': os.environ.get('ARCHIVE_DB_UNAME'),
            'ARCHIVE_DB_PASS': os.environ.get('ARCHIVE_DB_PASS')
        }
        check_keys(required_archive_keys, archive_param)

        # Extract database names from environment variables
        db_names_env = os.environ.get('DB_NAMES')
        if db_names_env is not None:
            db_names = [name.strip() for name in db_names_env.split(',')]
        else:
            logging.error("DB_NAMES not found in environment variables.")
            sys.exit(1)

        # Extract source parameters for each database from environment variables
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, os.environ, prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=None, env_vars=os.environ, db_name=db_name)

    # Return extracted parameters
    return db_names, archive_param, source_param

def create_source_param(config_parser, env_vars, db_name):
    param_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
    source_param = {}

    for key in param_keys:
        env_key = f'{db_name}_{key}'
        if config_parser is not None:
            try:
                source_param[env_key] = config_parser.get(db_name, env_key)
            except (configparser.NoOptionError, configparser.NoSectionError):
                source_param[env_key] = env_vars.get(env_key)
        else:
            source_param[env_key] = env_vars.get(env_key)

    return source_param

# Function to get formatted values for a row in a table
def get_tablevalues(row):
    final_values = ""
    for value in row:
        if value is None:
            final_values += "NULL,"
        else:
            final_values += "'" + str(value) + "',"
    final_values = final_values[:-1]
    return final_values

def read_tables_info(db_name):
    file_path = f'{db_name.lower()}_archive_table_info.json'
    file_in_container_path = f'{db_name.lower()}_archive_table_info'
    
    try:
        with open(file_path) as f:
            tables_info = json.load(f)
            logging.info(f"{file_path} file found and loaded.")
            return tables_info['tables_info']
    except FileNotFoundError:
        logging.error(f"{file_path} file not found. Trying to retrieve from container volume.")

        container_volume_path = os.environ.get('CONTAINER_VOLUME_PATH')

        if container_volume_path:
            file_path_in_volume = os.path.join(container_volume_path, file_in_container_path)
            try:
                with open(file_path_in_volume) as f:
                    tables_info = json.load(f)
                    logging.info(f"Data retrieved from container volume: {file_path_in_volume}")
                    return tables_info['tables_info']
            except FileNotFoundError:
                logging.error(f"{file_path_in_volume} not found in container volume.")
        else:
            logging.error("Container volume path not provided. Exiting.")
            sys.exit(1)

# Operation: Delete records from source table
def delete_records(source_cur, source_conn, sschema_name, source_table_name, id_column, rows):
    delete_query = f'DELETE FROM "{sschema_name}"."{source_table_name}" WHERE "{id_column}" = %s'
    for row in rows:
        source_cur.execute(delete_query, (row[0],))
        source_conn.commit()
        delete_count = source_cur.rowcount
        logging.info(f"{delete_count} Record(s) deleted successfully for table {source_table_name}")

# Operation: Archive records without deleting from source
def archive_records_no_delete(archive_cur, archive_conn, aschema_name, archive_table_name, rows):
    for row in rows:
        row_values = get_tablevalues(row)
        insert_query = f"INSERT INTO {aschema_name}.{archive_table_name} VALUES ({', '.join(['%s']*len(row))}) ON CONFLICT DO NOTHING"
        archive_cur.execute(insert_query, row)
        archive_conn.commit()
        insert_count = archive_cur.rowcount
        if insert_count == 0:
            logging.info(f"Skipping duplicate record with ID: {row[0]} in table {archive_table_name}")
        else:
            logging.info(f"{insert_count} Record(s) inserted successfully for table {archive_table_name}")

# Operation: Archive records and delete from source
def archive_and_delete_records(source_cur, source_conn, archive_cur, archive_conn, sschema_name, aschema_name, source_table_name, archive_table_name, id_column, rows):
    archive_records_no_delete(archive_cur, archive_conn, aschema_name, archive_table_name, rows)
    delete_records(source_cur, source_conn, sschema_name, source_table_name, id_column, rows)

# Operation: Skip archival
def skip_archival(source_table_name, db_name):
    logging.info(f"Skipping archival for table {source_table_name} from source database {db_name}")

# Function to perform the data archive operation
def data_archive(db_name, db_param, tables_info):
    source_conn = None
    archive_conn = None
    source_cur = None
    archive_cur = None
    try:
        logging.info(f'Connecting to the PostgreSQL database for {db_name}...')
        # Establish connections to source and archive databases
        source_conn = psycopg2.connect(
            user=db_param[f"{db_name}_SOURCE_DB_UNAME"],
            password=db_param[f"{db_name}_SOURCE_DB_PASS"],
            host=db_param[f"{db_name}_SOURCE_DB_HOST"],
            port=db_param[f"{db_name}_SOURCE_DB_PORT"],
            database=db_param[f"{db_name}_SOURCE_DB_NAME"]
        )
        archive_conn = psycopg2.connect(
            user=db_param["ARCHIVE_DB_UNAME"],
            password=db_param["ARCHIVE_DB_PASS"],
            host=db_param["ARCHIVE_DB_HOST"],
            port=db_param["ARCHIVE_DB_PORT"],
            database=db_param["ARCHIVE_DB_NAME"]
        )
        source_cur = source_conn.cursor()
        archive_cur = archive_conn.cursor()
        sschema_name = db_param[f"{db_name}_SOURCE_SCHEMA_NAME"]
        aschema_name = db_param["ARCHIVE_SCHEMA_NAME"]

        for table_info in tables_info:
            source_table_name = table_info['source_table']
            archive_table_name = table_info['archive_table']
            id_column = table_info['id_column']
            date_column = table_info.get('date_column')
            retention_days = table_info.get('retension_days', 0)
            operation_type = table_info.get('operation_type', 'none').lower()

            if date_column:
                select_query = f"SELECT * FROM {sschema_name}.{source_table_name} WHERE {date_column} < NOW() - INTERVAL '{retention_days} days'"
            else:
                select_query = f"SELECT * FROM {sschema_name}.{source_table_name}"
            
            source_cur.execute(select_query)
            rows = source_cur.fetchall()
            select_count = source_cur.rowcount
            logging.info(f"{select_count} Record(s) selected for operation '{operation_type}' from {source_table_name} in source database {db_name}")

            if operation_type == 'delete':
                delete_records(source_cur, source_conn, sschema_name, source_table_name, id_column, rows)
            elif operation_type == 'archive_nodelete':
                archive_records_no_delete(archive_cur, archive_conn, aschema_name, archive_table_name, rows)
            elif operation_type == 'archive_delete':
                archive_and_delete_records(source_cur, source_conn, archive_cur, archive_conn, sschema_name, aschema_name, source_table_name, archive_table_name, id_column, rows)
            elif operation_type == 'none':
                skip_archival(source_table_name, db_name)
            else:
                logging.error(f"Invalid value for 'operation_type' in table {source_table_name}. Use 'delete', 'archive_delete', 'archive_nodelete', or 'none'.")

    except Exception as error:
        logging.error(f"Error: {error}")
        sys.exit(1)
    finally:
        # Close the database connections
        if source_cur is not None:
            source_cur.close()
        if archive_cur is not None:
            archive_cur.close()
        if source_conn is not None:
            source_conn.close()
        if archive_conn is not None:
            archive_conn.close()
if __name__ == "__main__":
    db_names, archive_param, source_param = config()

    for db_name in db_names:
        tables_info = read_tables_info(db_name)
        data_archive(db_name, {**archive_param, **source_param[db_name]}, tables_info)
