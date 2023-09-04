#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import psycopg2
import configparser
import datetime
import os
from configparser import ConfigParser
from datetime import datetime

def config(filename='mosip_archive_idrepo.ini'):
    parser = ConfigParser()
    parser.read(filename)
    dbparam = {}
    if parser.has_section('MOSIP-DB-SECTION'):
        params = parser.items('MOSIP-DB-SECTION')
        for param in params:
            dbparam[param[0]] = param[1]
    else:
        raise Exception('Section [MOSIP-DB-SECTION] not found in the {0} file'.format(filename))
    dbparam["source_db_serverip"] = os.environ.get("SOURCE_DB_SERVERIP")
    dbparam["source_db_port"] = os.environ.get("SOURCE_DB_PORT")
    dbparam["source_db_uname"] = os.environ.get("SOURCE_DB_UNAME")
    dbparam["source_db_pass"] = os.environ.get("SOURCE_DB_PASS")
    dbparam["archive_db_serverip"] = os.environ.get("ARCHIVE_DB_SERVERIP")
    dbparam["archive_db_port"] = os.environ.get("ARCHIVE_DB_PORT")
    dbparam["archive_db_uname"] = os.environ.get("ARCHIVE_DB_UNAME")
    dbparam["archive_db_pass"] = os.environ.get("ARCHIVE_DB_PASS")
    if parser.has_section('ARCHIVE'):
        dbparam['tables'] = {}
        for table_key in parser.options('ARCHIVE'):
            table_info = parser.get('ARCHIVE', table_key).split(',')
            dbparam['tables'][table_info[1]] = {
                'source_table': table_info[0],
                'archive_table': table_info[1],
                'id_column': table_info[2],
                'date_column': table_info[3] if len(table_info) > 3 else None,
                'older_than_days': int(table_info[4]) if len(table_info) > 4 else None
            }
    else:
        raise Exception('Section [ARCHIVE] not found in the {0} file'.format(filename))
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
        sourceConn = psycopg2.connect(user=dbparam["source_db_uname"],
                                      password=dbparam["source_db_pass"],
                                      host=dbparam["source_db_serverip"],
                                      port=dbparam["source_db_port"],
                                      database=dbparam["source_db_name"])
        archiveConn = psycopg2.connect(user=dbparam["archive_db_uname"],
                                       password=dbparam["archive_db_pass"],
                                       host=dbparam["archive_db_serverip"],
                                       port=dbparam["archive_db_port"],
                                       database=dbparam["archive_db_name"])
        sourceCur = sourceConn.cursor()
        archiveCur = archiveConn.cursor()
        sschemaName = dbparam["source_schema_name"]
        aschemaName = dbparam["archive_schema_name"]
        for archive_table_name, table_info in dbparam['tables'].items():
            source_table_name = table_info['source_table']
            id_column = table_info['id_column']
            if table_info['date_column'] and table_info['older_than_days']:
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
        print(error)
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
