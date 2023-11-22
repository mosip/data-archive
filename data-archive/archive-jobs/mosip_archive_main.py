#!/usr/bin/python
# -*- coding: utf-8 -*-
# Import necessary libraries and modules
import sys
import os
import psycopg2
import configparser
import json
from datetime import datetime

# Function to check if required keys are present in a section
def check_keys(keys, section, prefix=""):
    for key in keys:
        env_key = f"{prefix}_{key}" if prefix else key
        if key not in section and env_key not in section:
            print(f"Error: {env_key} not found in {section} section.")
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

        # Extract source parameters for each database
        for db_name in db_names:
            required_source_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
            check_keys(required_source_keys, config_parser[db_name], prefix=db_name)
            source_param[db_name] = create_source_param(config_parser=config_parser, env_vars=os.environ, db_name=db_name)
    else:
        # Handle case when db.properties file is not found
        print("Error: db.properties file not found. Using environment variables.")
        # Use environment variables
        check_keys(required_archive_keys, os.environ)

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
    print(f"archive_param: {archive_param}")

    # Return extracted parameters
    return db_names, archive_param, source_param

# Function to create source parameters for a specific database
def create_source_param(config_parser, env_vars, db_name):
    param_keys = ['SOURCE_DB_HOST', 'SOURCE_DB_PORT', 'SOURCE_DB_NAME', 'SOURCE_SCHEMA_NAME', 'SOURCE_DB_UNAME', 'SOURCE_DB_PASS']
    source_param = {}

    # Extract source parameters from environment variables or config file
    for key in param_keys:
        env_key = f'{db_name}_{key}'
        source_param[env_key] = env_vars.get(env_key) or config_parser.get(db_name, env_key)

    return source_param

# Function to get formatted values for a row in a table
def get_tablevalues(row):
    finalValues = ""
    for value in row:
        if value is None:
            finalValues += "NULL,"
        else:
            finalValues += "'" + str(value) + "',"
    finalValues = finalValues[:-1]
    return finalValues

# Function to read table information from a JSON file or environment variable
def read_tables_info(db_name):
    try:
        # Attempt to read table information from a JSON file
        with open(f'{db_name.lower()}_archive_table_info.json') as f:
            tables_info = json.load(f)
            print(f"{db_name.lower()}_archive_table_info.json file found and loaded.")
            return tables_info['tables_info']
    except FileNotFoundError:
        # Handle case when JSON file is not found
        print(f"{db_name.lower()}_archive_table_info.json file not found. Using environment variables.")
        tables_info = os.environ.get(f"{db_name.lower()}_archive_table_info")
        if tables_info is None:
            print(f"Environment variable {db_name.lower()}_archive_table_info not found.")
            sys.exit(1)
        return json.loads(tables_info)['tables_info']

# Function to archive data from source to archive database
def dataArchive(db_name, dbparam, tables_info):
    sourceConn = None
    archiveConn = None
    sourceCur = None
    archiveCur = None
    try:
        print(f'Connecting to the PostgreSQL database for {db_name}...')
        # Establish connections to source and archive databases
        sourceConn = psycopg2.connect(
            user=dbparam[f"{db_name}_SOURCE_DB_UNAME"],
            password=dbparam[f"{db_name}_SOURCE_DB_PASS"],
            host=dbparam[f"{db_name}_SOURCE_DB_HOST"],
            port=dbparam[f"{db_name}_SOURCE_DB_PORT"],
            database=dbparam[f"{db_name}_SOURCE_DB_NAME"]
        )
        archiveConn = psycopg2.connect(
            user=dbparam["ARCHIVE_DB_UNAME"],
            password=dbparam["ARCHIVE_DB_PASS"],
            host=dbparam["ARCHIVE_DB_HOST"],
            port=dbparam["ARCHIVE_DB_PORT"],
            database=dbparam["ARCHIVE_DB_NAME"]
        )
        sourceCur = sourceConn.cursor()
        archiveCur = archiveConn.cursor()
        sschemaName = dbparam[f"{db_name}_SOURCE_SCHEMA_NAME"]
        aschemaName = dbparam["ARCHIVE_SCHEMA_NAME"]

        # Loop through the list of table_info dictionaries
        for table_info in tables_info:
            source_table_name = table_info['source_table']
            archive_table_name = table_info['archive_table']
            id_column = table_info['id_column']
            if 'date_column' in table_info and 'older_than_days' in table_info:
                date_column = table_info['date_column']
                older_than_days = table_info['older_than_days']
                # Construct a SELECT query with date-based filtering
                select_query = f"SELECT * FROM {sschemaName}.{source_table_name} WHERE {date_column} < NOW() - INTERVAL '{older_than_days} days'"
            else:
                # Construct a basic SELECT query
                select_query = f"SELECT * FROM {sschemaName}.{source_table_name}"
            sourceCur.execute(select_query)
            rows = sourceCur.fetchall()
            select_count = sourceCur.rowcount
            print(f"{select_count} Record(s) selected for archive from {source_table_name} from source database {db_name}")

            if select_count > 0:
                for row in rows:
                    rowValues = get_tablevalues(row)
                    # Construct an INSERT query to archive the selected row
                    insert_query = f"INSERT INTO {aschemaName}.{archive_table_name} VALUES ({rowValues}) ON CONFLICT DO NOTHING"
                    archiveCur.execute(insert_query)
                    archiveConn.commit()
                    insert_count = archiveCur.rowcount
                    if insert_count == 0:
                        print(f"Skipping duplicate record with ID: {row[0]} in table {archive_table_name} from source database {db_name}")
                    else:
                        print(f"{insert_count} Record(s) inserted successfully for table {archive_table_name} from source database {db_name}")
                    # Construct a DELETE query to remove the archived row from the source table
                    delete_query = f'DELETE FROM "{sschemaName}"."{source_table_name}" WHERE "{id_column}" = %s'
                    sourceCur.execute(delete_query, (row[0],))
                    sourceConn.commit()
                    delete_count = sourceCur.rowcount
                    print(f"{delete_count} Record(s) deleted successfully for table {source_table_name} from source database {db_name}")
    except (Exception, psycopg2.DatabaseError) as error:
        # Handle exceptions during the data archiving process
        print("Error during data archiving:", error)
    finally:
        # Close database connections
        if sourceCur is not None:
            sourceCur.close()
        if sourceConn is not None:
            sourceConn.close()
            print(f'Source database connection for {db_name} closed.')
        if archiveCur is not None:
            archiveCur.close()
        if archiveConn is not None:
            archiveConn.close()
            print('Archive database connection closed.')

# Main execution when the script is run
if __name__ == '__main__':
    # Get database names, archive parameters, and source parameters
    db_names, archive_param, source_param = config()
    
    # Process each source database
    for db_name in db_names:
        # Combine source and archive parameters
        dbparam = source_param[db_name]
        dbparam.update(archive_param)
        
        # Read table information
        tables_info = read_tables_info(db_name)
        
        # Archive data for the current source database
        dataArchive(db_name, dbparam, tables_info)
