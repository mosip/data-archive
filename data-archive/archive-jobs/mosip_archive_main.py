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

# Create connection pools for source and archive databases
source_pool = {}
archive_pool = None

def init_pools(archive_param, source_param):
    global archive_pool, source_pool

    # Create a connection pool for the archive database
    archive_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,  # Adjust the min and max connections according to your needs
        user=archive_param["ARCHIVE_DB_UNAME"],
        password=archive_param["ARCHIVE_DB_PASS"],
        host=archive_param["ARCHIVE_DB_HOST"],
        port=archive_param["ARCHIVE_DB_PORT"],
        database=archive_param["ARCHIVE_DB_NAME"]
    )

    # Create connection pools for each source database
    for db_name, param in source_param.items():
        source_pool[db_name] = psycopg2.pool.SimpleConnectionPool(
            1, 10,  # Adjust the min and max connections according to your needs
            user=param[f"{db_name}_SOURCE_DB_UNAME"],
            password=param[f"{db_name}_SOURCE_DB_PASS"],
            host=param[f"{db_name}_SOURCE_DB_HOST"],
            port=param[f"{db_name}_SOURCE_DB_PORT"],
            database=param[f"{db_name}_SOURCE_DB_NAME"]
        )

def get_connection(pool, db_name=None):
    # Get a connection from the specified pool
    if db_name:
        return source_pool[db_name].getconn()
    else:
        return archive_pool.getconn()

def release_connection(pool, conn, db_name=None):
    # Return the connection back to the pool
    if db_name:
        source_pool[db_name].putconn(conn)
    else:
        archive_pool.putconn(conn)

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
    required_archive_keys = ['ARCHIVE_DB_HOST', 'ARCHIVE_DB_PORT', 'ARCHIVE_DB_NAME', 'ARCHIVE_SCHEMA_NAME', 'ARCHIVE_DB_UNAME', 'ARCHIVE_DB_PASS']
    required_db_names_keys = ['DB_NAMES']

    archive_param = {}
    source_param = {}
    db_names = []
    batch_size = None 

    if os.path.exists('db.properties'):
        print("Using database connection parameters from db.properties.")
        config_parser = configparser.ConfigParser()
        config_parser.read('db.properties')

        check_keys(required_archive_keys, config_parser['ARCHIVE'])
        check_keys(required_db_names_keys, config_parser['Databases'])

        archive_param = {key.upper(): config_parser['ARCHIVE'][key] for key in config_parser['ARCHIVE']}
        db_names = config_parser.get('Databases', 'DB_NAMES').split(',')
        db_names = [name.strip() for name in db_names]

        if config_parser.has_option('ARCHIVE', 'BATCH_SIZE'):
            batch_size = int(config_parser['ARCHIVE']['BATCH_SIZE'])
            print(f"Using BATCH_SIZE from db.properties: {batch_size}")
        else:
            batch_size_env = os.environ.get('BATCH_SIZE')
            if batch_size_env:
                batch_size = int(batch_size_env)
                print(f"Using BATCH_SIZE from environment variables: {batch_size}")
            else:
                print("Error: BATCH_SIZE not found.")
                sys.exit(1)

        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, config_parser[db_name], prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=config_parser, env_vars=os.environ, db_name=db_name)
    else:
        print("Error: db.properties file not found.")
        sys.exit(1)

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
        print(f'Connecting to the PostgreSQL source and archive databases for {db_name}...')

        # Get connections from the connection pools
        source_conn = get_connection(source_pool, db_name)
        archive_conn = get_connection(archive_pool)

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

            last_processed_id = None  

            if retention_days and date_column:
                where_clause = f"WHERE {date_column} < NOW() - INTERVAL '{retention_days} days' AND {id_column} > %s"
            else:
                where_clause = f"WHERE {id_column} > %s"

            while True:
                if operation_type == 'delete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"
                elif operation_type == 'archive_delete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"
                elif operation_type == 'archive_nodelete':
                    select_query = f"SELECT * FROM {sschema_name}.{source_table_name} {where_clause} ORDER BY {id_column} LIMIT %s"
                elif operation_type == 'none':
                    print(f"No operation specified for {source_table_name}, skipping.")
                    break

                source_cur.execute(select_query, (last_processed_id if last_processed_id else '0', batch_size))
                rows = source_cur.fetchall()

                if not rows:
                    print(f"No more records to process for {source_table_name}.")
                    break

                for row in rows:
                    row_id = row[0] 
                    values = get_tablevalues(row)

                    if operation_type in ['archive_delete', 'archive_nodelete']:
                        insert_query = f"INSERT INTO {aschema_name}.{archive_table_name} VALUES ({values}) ON CONFLICT DO NOTHING"
                        archive_cur.execute(insert_query)

                        if operation_type == 'archive_delete':
                            delete_query = f"DELETE FROM {sschema_name}.{source_table_name} WHERE {id_column} = %s"
                            source_cur.execute(delete_query, (row_id,))
                            total_deleted += 1

                    elif operation_type == 'delete':
                        delete_query = f"DELETE FROM {sschema_name}.{source_table_name} WHERE {id_column} = %s"
                        source_cur.execute(delete_query, (row_id,))
                        total_deleted += 1

                    last_processed_id = row_id
                    total_archived += 1 if operation_type in ['archive_delete', 'archive_nodelete'] else 0

                source_conn.commit()
                archive_conn.commit()

        print(f"Archiving complete for {db_name}. Total archived: {total_archived}, Total deleted: {total_deleted}, Total skipped: {total_skipped}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while archiving data for {db_name}: {error}")
        source_conn.rollback()
        archive_conn.rollback()

    finally:
        if source_cur:
            source_cur.close()
        if archive_cur:
            archive_cur.close()
        if source_conn:
            release_connection(source_pool, source_conn, db_name)
        if archive_conn:
            release_connection(archive_pool, archive_conn)

# Main function
def main():
    db_names, archive_param, source_param, batch_size = config()

    # Initialize connection pools
    init_pools(archive_param, source_param)

    # Process each database
    for db_name in db_names:
        tables_info = read_tables_info(db_name)
        print(f"Processing {db_name} with tables: {tables_info}")
        data_archive(db_name, source_param, tables_info, batch_size)

if __name__ == '__main__':
    main()
