#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import psycopg2
import configparser
import datetime
from configparser import ConfigParser
from datetime import datetime

def config(filename='mosip_archive_kernel.ini'):
    parser = ConfigParser()
    parser.read(filename)
    dbparam = {}

    if parser.has_section('MOSIP-DB-SECTION'):
        params = parser.items('MOSIP-DB-SECTION')
        for param in params:
            dbparam[param[0]] = param[1]
    else:
        raise Exception('Section [MOSIP-DB-SECTION] not found in the {0} file'.format(filename))

    if parser.has_section('ARCHIVE'):
        tables = parser.items('ARCHIVE')
        dbparam['tables'] = {}
        for table, table_info in tables:
            if table_info:
                columns = table_info.split(',')
                if len(columns) != 3:
                    raise Exception('Invalid number of parameters for table {0} in the [ARCHIVE] section'.format(table))
                date_column, id_column, retention_period = columns
                retention_period = int(''.join(filter(str.isdigit, retention_period.strip())))  # Remove non-digit characters
                table_info_dict = {
                    'id_column': id_column.strip(),
                    'retention_period': retention_period,
                    'date_column': date_column.strip(),
                }
                dbparam['tables'][table] = table_info_dict
            else:
                raise Exception('Invalid parameters for table {0} in the [ARCHIVE] section'.format(table))
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
    finalValues = finalValues[:-1]  # Remove the trailing comma
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

        for table_name, table_info in dbparam['tables'].items():
            id_column = table_info['id_column']
            retention_period = table_info['retention_period']
            date_column = table_info['date_column']

            print(table_name)
            select_query = "SELECT * FROM {0}.{1} WHERE {2} < NOW() - INTERVAL '{3} days'".format(sschemaName, table_name, date_column, retention_period)
            sourceCur.execute(select_query)
            rows = sourceCur.fetchall()
            select_count = sourceCur.rowcount
            print(select_count, ": Record(s) selected for archive from", table_name)

            if select_count > 0:
                for row in rows:
                    rowValues = getValues(row)
                    insert_query = "INSERT INTO {0}.{1} VALUES ({2}) ON CONFLICT DO NOTHING".format(aschemaName, table_name, rowValues)
                    archiveCur.execute(insert_query)
                    archiveConn.commit()
                    insert_count = archiveCur.rowcount
                    if insert_count == 0:
                        print("Skipping duplicate record with ID:", row[0])
                    else:
                        print(insert_count, ": Record inserted successfully")

                    delete_query = 'DELETE FROM "{0}"."{1}" WHERE "{2}" = %s'.format(sschemaName, table_name, id_column)
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
