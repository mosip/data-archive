
#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import psycopg2
import configparser
import json
from datetime import datetime
from psycopg2 import extras
from psycopg2 import pool

# Define batch size
#BATCH_SIZE = 10
source_conn_pool = None
archive_conn_pool = None
# Function to check if required keys are present in a section
def check_keys(keys, section, prefix=""):
    missing_keys = []
    for key in keys:
        env_key = f"{prefix}_{key}" if prefix else key
        if key not in section and env_key not in section:
            print(f"Error: {env_key} not found in {section} section.")
            missing_keys.append(key)
    if missing_keys:
        print(f"Missing keys: {', '.join(missing_keys)}")
        sys.exit(1)

# Function to read configuration from file or environment variables
def config():
    # Define required keys for archive and database connection
    required_archive_keys = ['ARCHIVE_DB_HOST', 'ARCHIVE_DB_PORT', 'ARCHIVE_DB_NAME', 'ARCHIVE_SCHEMA_NAME', 'ARCHIVE_DB_UNAME', 'ARCHIVE_DB_PASS']
    required_db_names_keys = ['DB_NAMES']

    archive_param = {}
    source_param = {}
    db_names = []
    batch_size = None # Batch size will be read from db.properties or environment variables

    # Check if db.properties file exists
    if os.path.exists('db.properties'):
        print("Using database connection parameters from db.properties.")
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

        # Extract batch size from the config file if available
        if config_parser.has_option('ARCHIVE', 'BATCH_SIZE'):
            batch_size = int(config_parser['ARCHIVE']['BATCH_SIZE'])
            print(f"Using BATCH_SIZE from db.properties: {batch_size}")
        else:
            print("Error: BATCH_SIZE not found in db.properties.")
            # Check environment variable for batch size if not found in config file
            batch_size_env = os.environ.get('BATCH_SIZE')
            if batch_size_env:
                batch_size = int(batch_size_env)
                print(f"Using BATCH_SIZE from environment variables: {batch_size}")
            else:
                print("Error: BATCH_SIZE not found in environment variables.")
                sys.exit(1)            

        # Extract source parameters for each database
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, config_parser[db_name], prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=config_parser, env_vars=os.environ, db_name=db_name)
    else:
        # Handle case when db.properties file is not found
        print("Error: db.properties file not found. Using environment variables.")
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

        # Extract batch size from environment variables if available
        # batch_size_env = os.environ.get('BATCH_SIZE')
        # if batch_size_env:
        #     batch_size = int(batch_size_env)
        #     print(f"Using BATCH_SIZE from environment variables: {batch_size}")

        # Check environment variable for batch size if not found in config file
        batch_size_env = os.environ.get('BATCH_SIZE')
        if batch_size_env:
            batch_size = int(batch_size_env)
            print(f"Using BATCH_SIZE from environment variables: {batch_size}")
        else:
            print("Error: BATCH_SIZE not found in environment variables.")
            sys.exit(1)

        # Extract database names from environment variables
        db_names_env = os.environ.get('DB_NAMES')
        if db_names_env is not None:
            db_names = [name.strip() for name in db_names_env.split(',')]
        else:
            print("Error: DB_NAMES not found in environment variables.")
            sys.exit(1)

        # Extract source parameters for each database from environment variables
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, os.environ, prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=None, env_vars=os.environ, db_name=db_name)

    # Return extracted parameters and dynamic batch size
    return db_names, archive_param, source_param, batch_size

# Function to create source parameters for a specific database
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
            final_values += "'" + str(value).replace("'", "''") + "',"
    final_values = final_values[:-1]
    return final_values

# Function to read table information from a JSON file or container volume
def read_tables_info(db_name):
    file_path = f'{db_name.lower()}_archive_table_info.json'
    file_in_container_path = f'{db_name.lower()}_archive_table_info'

    try:
        with open(file_path) as f:
            tables_info = json.load(f)
            print(f"{file_path} file found and loaded.")
            return tables_info['tables_info']
    except FileNotFoundError:
        print(f"{file_path} file not found. Trying to retrieve from container volume.")

        # Assuming CONTAINER_VOLUME_PATH is the environment variable containing the path to the container volume
        container_volume_path = os.environ.get('CONTAINER_VOLUME_PATH')

        if container_volume_path:
            file_path_in_volume = os.path.join(container_volume_path, file_in_container_path)
            try:
                with open(file_path_in_volume) as f:
                    tables_info = json.load(f)
                    print(f"Data retrieved from container volume: {file_path_in_volume}")
                    return tables_info['tables_info']
            except FileNotFoundError:
                print(f"{file_path_in_volume} not found in container volume.")
        else:
            print("Container volume path not provided. Exiting.")
            sys.exit(1)

# Function to initialize connection pools
def init_connection_pools(archive_param, source_param):
    global source_conn_pool, archive_conn_pool

    # Print the archive and source parameters for debugging
    print("Fetching connection pools...")
    print("Archive Parameters:", archive_param)
    print("Source Parameters:", source_param)
    # Initialize source database connection pool
    try:
        source_conn_pool = pool.SimpleConnectionPool(
            1, 10,  # minconn, maxconn
            user=db_param[f"{db_name}_SOURCE_DB_UNAME"],
            password=db_param[f"{db_name}_SOURCE_DB_PASS"],
            host=db_param[f"{db_name}_SOURCE_DB_HOST"],
            port=db_param[f"{db_name}_SOURCE_DB_PORT"],
            database=db_param[f"{db_name}_SOURCE_DB_NAME"]
        )
        print("Source DB connection pool created successfully.")
    except Exception as e:
        print(f"Error creating source DB connection pool: {e}")
        sys.exit(1)

    # Initialize archive database connection pool
    try:
        archive_conn_pool = pool.SimpleConnectionPool(
            1, 10,  # minconn, maxconn
            user=db_param["ARCHIVE_DB_UNAME"],
            password=db_param["ARCHIVE_DB_PASS"],
            host=db_param["ARCHIVE_DB_HOST"],
            port=db_param["ARCHIVE_DB_PORT"],
            database=db_param["ARCHIVE_DB_NAME"]
        )
        print("Archive DB connection pool created successfully.")
    except Exception as e:
        print(f"Error creating archive DB connection pool: {e}")
        sys.exit(1)

# Function to archive data from source database to archive database
def data_archive(db_name, db_param, tables_info, batch_size):
    source_conn = None
    archive_conn = None
    source_cur = None
    archive_cur = None

    total_archived = 0
    total_deleted = 0
    total_skipped = 0

    try:
        print(f"Fetching connections from connection pools for {db_name}...")

        # Fetch connection from source pool
        source_conn = source_conn_pool.getconn()
        print(f"Source DB connection obtained from pool for {db_name}.")

        # Fetch connection from archive pool
        archive_conn = archive_conn_pool.getconn()
        print(f"Archive DB connection obtained from pool.")

        source_cur = source_conn.cursor()
        archive_cur = archive_conn.cursor()
        sschema_name = db_param[f"{db_name}_SOURCE_SCHEMA_NAME"]
        aschema_name = db_param["ARCHIVE_SCHEMA_NAME"]

        for table_info in tables_info:
            source_table_name = table_info['source_table']
            archive_table_name = table_info['archive_table']
            id_column = table_info['id_column']
            date_column = table_info.get('date_column', None)
            retention_days = table_info.get('retention_days', None)
            operation_type = table_info.get('operation_type', 'none').lower()

            last_processed_id = None  # Initialize last processed ID

            # Prepare the select query based on operation type and retention settings
            if retention_days and date_column:
                where_clause = f"WHERE {date_column} < NOW() - INTERVAL '{retention_days} days' AND {id_column} > %s"
            else:
                where_clause = f"WHERE {id_column} > %s"

            while True:
                # SELECT query for operation_type delete
                if operation_type == 'delete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"

                # SELECT query for operation_type archive_delete
                elif operation_type == 'archive_delete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"

                # SELECT query for operation_type archive_nodelete
                elif operation_type == 'archive_nodelete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"

                # Skip the processing if operation_type is 'none'
                elif operation_type == 'none':
                    print(f"No operation specified for {source_table_name}, skipping.")
                    break

                # Execute the select query
                source_cur.execute(select_query, (last_processed_id if last_processed_id else '0', batch_size))
                rows = source_cur.fetchall()

                if not rows:
                    print(f"No more records to process for {source_table_name}.")
                    break

                for row in rows:
                    row_id = row[0]
                    values = get_tablevalues(row)

                    # Check for existence before inserting for archive_delete and archive_nodelete
                    if operation_type in ['archive_delete', 'archive_nodelete']:
                        check_query = f"SELECT 1 FROM {aschema_name}.{archive_table_name} WHERE {id_column} = %s"
                        archive_cur.execute(check_query, (row_id,))
                        exists = archive_cur.fetchone()

                        if exists:
                            total_skipped += 1
                            print(f"Record {row_id} already exists in {archive_table_name}, skipping.")
                        else:
                            try:
                                # Insert the record into the archive database
                                insert_query = f"INSERT INTO {aschema_name}.{archive_table_name} VALUES ({values})"
                                archive_cur.execute(insert_query)
                                total_archived += 1
                                print(f"Record {row_id} inserted into {archive_table_name}.")
                            except psycopg2.Error as e:
                                print(f"Error inserting record {row_id} into {archive_table_name}: {e}")
                                archive_conn.rollback()
                                continue

                    # Delete from source if needed (for archive_delete or delete)
                    if operation_type == 'archive_delete' or operation_type == 'delete':
                        try:
                            delete_query = f"DELETE FROM {sschema_name}.{source_table_name} WHERE {id_column} = %s"
                            source_cur.execute(delete_query, (row_id,))
                            total_deleted += 1
                            print(f"Record {row_id} deleted from {source_table_name}.")
                        except psycopg2.Error as e:
                            print(f"Error deleting record {row_id} from {source_table_name}: {e}")
                            source_conn.rollback()
                            continue

                # Update last processed ID
                last_processed_id = row_id

            source_conn.commit()
            archive_conn.commit()

    except Exception as e:
        print(f"Error archiving data for {db_name}: {e}")
    finally:
        # Release connections back to the pool
        if source_conn:
            source_conn_pool.putconn(source_conn)
            print(f"Source DB connection returned to pool for {db_name}.")
        if archive_conn:
            archive_conn_pool.putconn(archive_conn)
            print(f"Archive DB connection returned to pool.")
        # Print the summary for this database
        print(f"Data archival completed for {db_name}.")
        print(f"Total records archived: {total_archived}")
        print(f"Total records deleted: {total_deleted}")
        print(f"Total records skipped: {total_skipped}")


def main():
    try:
        # Get configuration parameters
        db_names, archive_param, source_param, batch_size = config()
        print(f"Starting data archive process with BATCH_SIZE: {batch_size} for databases: {db_names}")

        # Process each database
        for db_name in db_names:
            tables_info = read_tables_info(db_name)
            data_archive(db_name, {**archive_param, **source_param[db_name]}, tables_info, batch_size)
            print(f"Data archive process completed for {db_name}.")

    except Exception as e:
        print(f"Error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
