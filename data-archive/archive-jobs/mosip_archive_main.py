#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import psycopg2
import configparser
import json
from datetime import datetime

def config():
    config = configparser.ConfigParser()
    archive_param = {}
    source_param = {}
    db_names = []

    # Check environment variables first
    if os.environ.get('ARCHIVE_DB_HOST') is not None:
        print("Using database connection parameters from environment variables.")
        archive_param = {
            'ARCHIVE_DB_HOST': os.environ.get('ARCHIVE_DB_HOST'),
            'ARCHIVE_DB_PORT': os.environ.get('ARCHIVE_DB_PORT'),
            'ARCHIVE_DB_NAME': os.environ.get('ARCHIVE_DB_NAME'),
            'ARCHIVE_SCHEMA_NAME': os.environ.get('ARCHIVE_SCHEMA_NAME'),
            'ARCHIVE_DB_UNAME': os.environ.get('ARCHIVE_DB_UNAME'),
            'ARCHIVE_DB_PASS': os.environ.get('ARCHIVE_DB_PASS')
        }

        db_names_env = os.environ.get('DB_NAMES')
        if db_names_env is not None:
            db_names = [name.strip() for name in db_names_env.split(',')]
        else:
            print("Error: DB_NAMES not found in environment variables.")
            sys.exit(1)

        for db_name in db_names:
            source_param[db_name] = {
                f'{db_name}_SOURCE_DB_HOST': os.environ.get(f'{db_name}_SOURCE_DB_HOST'),
                f'{db_name}_SOURCE_DB_PORT': os.environ.get(f'{db_name}_SOURCE_DB_PORT'),
                f'{db_name}_SOURCE_DB_NAME': os.environ.get(f'{db_name}_SOURCE_DB_NAME'),
                f'{db_name}_SOURCE_SCHEMA_NAME': os.environ.get(f'{db_name}_SOURCE_SCHEMA_NAME'),
                f'{db_name}_SOURCE_DB_UNAME': os.environ.get(f'{db_name}_SOURCE_DB_UNAME'),
                f'{db_name}_SOURCE_DB_PASS': os.environ.get(f'{db_name}_SOURCE_DB_PASS')
            }
    else:
        # If environment variables are not set, try reading from db.properties
        if os.path.exists('db.properties'):
            print("Using database connection parameters from db.properties.")
            config.read('db.properties')
            archive_param = {key.upper(): config['ARCHIVE'][key] for key in config['ARCHIVE']}
            db_names = config.get('Databases', 'DB_NAMES').split(',')
            db_names = [name.strip() for name in db_names]

            for db_name in db_names:
                source_param[db_name] = {
                    f'{db_name}_SOURCE_DB_HOST': config.get(db_name, f'{db_name}_SOURCE_DB_HOST'),
                    f'{db_name}_SOURCE_DB_PORT': config.get(db_name, f'{db_name}_SOURCE_DB_PORT'),
                    f'{db_name}__SOURCE_DB_NAME': config.get(db_name, f'{db_name}_SOURCE_DB_NAME'),
                    f'{db_name}_SOURCE_SCHEMA_NAME': config.get(db_name, f'{db_name}_SOURCE_SCHEMA_NAME'),
                    f'{db_name}_SOURCE_DB_UNAME': config.get(db_name, f'{db_name}_SOURCE_DB_UNAME'),
                    f'{db_name}_SOURCE_DB_PASS': config.get(db_name, f'{db_name}_SOURCE_DB_PASS')
                }
        else:
            print("Error: db.properties file not found.")
            sys.exit(1)

    return db_names, archive_param, source_param

def getValues(row):
    finalValues = ""
    for value in row:
        if value is None:
            finalValues += "NULL,"
        else:
            finalValues += "'" + str(value) + "',"
    finalValues = finalValues[:-1]
    return finalValues

def read_tables_info(db_name):
    try:
        with open(f'{db_name.lower()}_archive_table_info.json') as f:
            tables_info = json.load(f)
            print(f"{db_name.lower()}_archive_table_info.json file found and loaded.")
            return tables_info['tables_info']
    except FileNotFoundError:
        print(f"{db_name.lower()}_archive_table_info.json file not found. Using environment variables.")
        tables_info = os.environ.get(f"{db_name.lower()}_archive_table_info")
        if tables_info is None:
            print(f"Environment variable {db_name.lower()}_archive_table_info not found.")
            sys.exit(1)
        return json.loads(tables_info)['tables_info']

def dataArchive(db_name, dbparam, tables_info):
    sourceConn = None
    archiveConn = None
    sourceCur = None
    archiveCur = None
    try:
        print('Connecting to the PostgreSQL database...')
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
                select_query = f"SELECT * FROM {sschemaName}.{source_table_name} WHERE {date_column} < NOW() - INTERVAL '{older_than_days} days'"
            else:
                select_query = f"SELECT * FROM {sschemaName}.{source_table_name}"
            sourceCur.execute(select_query)
            rows = sourceCur.fetchall()
            select_count = sourceCur.rowcount
            print(f"{select_count} Record(s) selected for archive from {source_table_name}")
            if select_count > 0:
                for row in rows:
                    rowValues = getValues(row)
                    insert_query = f"INSERT INTO {aschemaName}.{archive_table_name} VALUES ({rowValues}) ON CONFLICT DO NOTHING"
                    archiveCur.execute(insert_query)
                    archiveConn.commit()
                    insert_count = archiveCur.rowcount
                    if insert_count == 0:
                        print(f"Skipping duplicate record with ID: {row[0]}")
                    else:
                        print(f"{insert_count} Record inserted successfully")
                    delete_query = f'DELETE FROM "{sschemaName}"."{source_table_name}" WHERE "{id_column}" = %s'
                    sourceCur.execute(delete_query, (row[0],))
                    sourceConn.commit()
                    delete_count = sourceCur.rowcount
                    print(f"{delete_count} Record(s) deleted successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error during data archiving:", error)
    finally:
        if sourceCur is not None:
            sourceCur.close()
        if sourceConn is not None:
            sourceConn.close()
            print('Source database connection closed.')
        if archiveCur is not None:
            archiveCur.close()
        if archiveConn is not None:
            archiveConn.close()
            print('Archive database connection closed.')

if __name__ == '__main__':
    db_names, archive_param, source_param = config()
    for db_name in db_names:
        dbparam = source_param[db_name]
        dbparam.update(archive_param)
        tables_info = read_tables_info(db_name)
        dataArchive(db_name, dbparam, tables_info)
