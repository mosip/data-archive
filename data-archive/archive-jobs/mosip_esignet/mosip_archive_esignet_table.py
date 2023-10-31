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
    dbparam = {}
    try:
        with open('db.properties') as f:
            lines = f.readlines()
            for line in lines:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    dbparam[key] = value
    except FileNotFoundError:
        print("db.properties file not found. Using environment variables.")
        pass
    # Environment variables will overwrite file values if present
    dbparam["SOURCE_ESIGNET_DB_SERVERIP"] = os.environ.get("SOURCE_ESIGNET_DB_SERVERIP") or dbparam.get("SOURCE_ESIGNET_DB_SERVERIP")
    dbparam["SOURCE_ESIGNET_DB_PORT"] = os.environ.get("SOURCE_ESIGNET_DB_PORT") or dbparam.get("SOURCE_ESIGNET_DB_PORT")
    dbparam["SOURCE_ESIGNET_DB_NAME"] = os.environ.get("SOURCE_ESIGNET_DB_NAME") or dbparam.get("SOURCE_ESIGNET_DB_NAME")
    dbparam["SOURCE_ESIGNET_SCHEMA_NAME"] = os.environ.get("SOURCE_ESIGNET_SCHEMA_NAME") or dbparam.get("SOURCE_ESIGNET_SCHEMA_NAME")
    dbparam["SOURCE_ESIGNET_DB_UNAME"] = os.environ.get("SOURCE_ESIGNET_DB_UNAME") or dbparam.get("SOURCE_ESIGNET_DB_UNAME")
    dbparam["SOURCE_ESIGNET_DB_PASS"] = os.environ.get("SOURCE_ESIGNET_DB_PASS") or dbparam.get("SOURCE_ESIGNET_DB_PASS")
    dbparam["ARCHIVE_DB_SERVERIP"] = os.environ.get("ARCHIVE_DB_SERVERIP") or dbparam.get("ARCHIVE_DB_SERVERIP")
    dbparam["ARCHIVE_DB_PORT"] = os.environ.get("ARCHIVE_DB_PORT") or dbparam.get("ARCHIVE_DB_PORT")
    dbparam["ARCHIVE_DB_NAME"] = os.environ.get("ARCHIVE_DB_NAME") or dbparam.get("ARCHIVE_DB_NAME")
    dbparam["ARCHIVE_SCHEMA_NAME"] = os.environ.get("ARCHIVE_SCHEMA_NAME") or dbparam.get("ARCHIVE_SCHEMA_NAME")
    dbparam["ARCHIVE_DB_UNAME"] = os.environ.get("ARCHIVE_DB_UNAME") or dbparam.get("ARCHIVE_DB_UNAME")
    dbparam["ARCHIVE_DB_PASS"] = os.environ.get("ARCHIVE_DB_PASS") or dbparam.get("ARCHIVE_DB_PASS")
    try:
        with open('archive_table_info.json') as f:
            archive_table_info = json.load(f)
            dbparam['tables'] = archive_table_info['tables_info']
    except FileNotFoundError:
        print("archive_table_info.json file not found.")
        sys.exit(1)
    return dbparam
def getValues(row):
    finalValues = ""
    for value in row:
        if value is None:
            finalValues += "NULL,"
        else:
            finalValues += "'" + str(value) + "',"
    finalValues = finalValues[:-1]
    return finalValues
def dataArchive():
    sourceConn = None
    archiveConn = None
    sourceCur = None
    archiveCur = None
    try:
        dbparam = config()
        print('Connecting to the PostgreSQL database...')
        print("Source Database Connection Parameters:")
        sourceConn = psycopg2.connect(user=dbparam["SOURCE_ESIGNET_DB_UNAME"],
                                      password=dbparam["SOURCE_ESIGNET_DB_PASS"],
                                      host=dbparam["SOURCE_ESIGNET_DB_SERVERIP"],
                                      port=dbparam["SOURCE_ESIGNET_DB_PORT"],
                                      database=dbparam["SOURCE_ESIGNET_DB_NAME"])
        archiveConn = psycopg2.connect(user=dbparam["ARCHIVE_DB_UNAME"],
                                       password=dbparam["ARCHIVE_DB_PASS"],
                                       host=dbparam["ARCHIVE_DB_SERVERIP"],
                                       port=dbparam["ARCHIVE_DB_PORT"],
                                       database=dbparam["ARCHIVE_DB_NAME"])
        sourceCur = sourceConn.cursor()
        archiveCur = archiveConn.cursor()
        sschemaName = dbparam["SOURCE_ESIGNET_SCHEMA_NAME"]
        aschemaName = dbparam["ARCHIVE_SCHEMA_NAME"]
        # Loop through the list of table_info dictionaries
        for table_info in dbparam['tables']:
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
    dataArchive()