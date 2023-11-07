#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import psycopg2
import configparser
import datetime
import os
import json
from datetime import datetime

def config():
    config = configparser.ConfigParser()
    if os.path.exists('all_db_properties.properties'):
        config.read('all_db_properties.properties')
        archive_param = {key.upper(): config['ARCHIVE'][key] for key in config['ARCHIVE']}
        source_param = {db_name: {key.upper(): config[db_name][key] for key in config[db_name]} for db_name in config.sections() if db_name != 'ARCHIVE'}
        
        # Read database names from properties file
        db_names = config.get('Databases', 'DB_NAMES').split(', ')
        print("all_db_properties.properties file found and loaded.")
    else:
        print("all_db_properties.properties file not found. Using environment variables.")
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
            db_names = db_names_env.split(', ')
        else:
            print("Error: DB_NAMES not found in properties file or environment variables.")
            sys.exit(1)

        source_param = {}
        for db_name in db_names:
            source_param[db_name] = {
                f'{db_name}_SOURCE_DB_HOST': os.environ.get(f'{db_name}_SOURCE_DB_HOST'),
                f'{db_name}_SOURCE_DB_PORT': os.environ.get(f'{db_name}_SOURCE_DB_PORT'),
                f'{db_name}_SOURCE_DB_NAME': os.environ.get(f'{db_name}_SOURCE_DB_NAME'),
                f'{db_name}_SOURCE_SCHEMA_NAME': os.environ.get(f'{db_name}_SOURCE_SCHEMA_NAME'),
                f'{db_name}_SOURCE_DB_UNAME': os.environ.get(f'{db_name}_SOURCE_DB_UNAME'),
                f'{db_name}_SOURCE_DB_PASS': os.environ.get(f'{db_name}_SOURCE_DB_PASS')
            }
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
        with open('{}_archive_table_info.json'.format(db_name.lower())) as f:
            tables_info = json.load(f)
            print("{}_archive_table_info.json file found and loaded.".format(db_name.lower()))
            print("Contents of the JSON file:")
            print(json.dumps(tables_info, indent=4))
            return tables_info['tables_info']
    except FileNotFoundError:
        print("{}_archive_table_info.json file not found. Using environment variables.".format(db_name.lower()))
        tables_info = os.environ.get("{}_archive_table_info".format(db_name.lower()))
        if tables_info is None:
            print("Environment variable {}_archive_table_info not found.".format(db_name.lower()))
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
            user=dbparam["{}_SOURCE_DB_UNAME".format(db_name)],
            password=dbparam["{}_SOURCE_DB_PASS".format(db_name)],
            host=dbparam["{}_SOURCE_DB_HOST".format(db_name)],
            port=dbparam["{}_SOURCE_DB_PORT".format(db_name)],
            database=dbparam["{}_SOURCE_DB_NAME".format(db_name)]
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
        sschemaName = dbparam["{}_SOURCE_SCHEMA_NAME".format(db_name)]
        aschemaName = dbparam["ARCHIVE_SCHEMA_NAME"]

        # Loop through the list of table_info dictionaries
        for table_info in tables_info:
            source_table_name = table_info['source_table']
            archive_table_name = table_info['archive_table']
            id_column = table_info['id_column']
            if 'date_column' in table_info and 'older_than_days' in table_info:
                date_column = table_info['date_column']
                older_than_days = table_info['older_than_days']
                select_query = "SELECT * FROM {0}.{1} WHERE {2} < NOW() - INTERVAL '{3} days'".format(sschemaName, source_table_name, date_column, older_than_days)
            else:
                select_query = "SELECT * FROM {0}.{1}".format(sschemaName, source_table_name)
            sourceCur.execute(select_query)
            rows = sourceCur.fetchall()
            select_count = sourceCur.rowcount
            print(select_count, ": Record(s) selected for archive from", source_table_name)
            if select_count > 0:
                for row in rows:
                    rowValues = getValues(row)
                    insert_query = "INSERT INTO {0}.{1} VALUES ({2}) ON CONFLICT DO NOTHING".format(aschemaName, archive_table_name, rowValues)
                    archiveCur.execute(insert_query)
                    archiveConn.commit()
                    insert_count = archiveCur.rowcount
                    if insert_count == 0:
                        print("Skipping duplicate record with ID:", row[0])
                    else:
                        print(insert_count, ": Record inserted successfully")
                    delete_query = 'DELETE FROM "{0}"."{1}" WHERE "{2}" = %s'.format(sschemaName, source_table_name, id_column)
                    sourceCur.execute(delete_query, (row[0],))
                    sourceConn.commit()
                    delete_count = sourceCur.rowcount
                    print(delete_count, ": Record(s) deleted successfully")
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
