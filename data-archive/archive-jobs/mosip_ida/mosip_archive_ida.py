#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import configparser
import psycopg2
import datetime
from configparser import ConfigParser
from datetime import datetime

def config(filename='mosip_archive_ida.ini', section='MOSIP-DB-SECTION'):
    parser = ConfigParser()
    parser.read(filename)
    dbparam = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            dbparam[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

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

        tables = [
            {
                'name': dbparam["archive_table1"],
                'id_column': 'id',
                'older_than_days': int(dbparam["archive_table1_older_than_days"])
            },
            {
                'name': dbparam["archive_table2"],
                'id_column': 'id',
                'older_than_days': int(dbparam["archive_table2_older_than_days"])
            },
            {
                'name': dbparam["archive_table3"],
                'id_column': 'job_execution_id',
                'older_than_days': int(dbparam["archive_table3_older_than_days"])
            },
            {
                'name': dbparam["archive_table4"],
                'id_column': 'job_execution_id',
                'older_than_days': int(dbparam["archive_table4_older_than_days"])
            },
            {
                'name': dbparam["archive_table5"],
                'id_column': 'job_execution_id',
                'older_than_days': int(dbparam["archive_table5_older_than_days"])
            },
            {
                'name': dbparam["archive_table6"],
                'id_column': 'job_execution_id',
                'older_than_days': int(dbparam["archive_table6_older_than_days"])
            },
            {
                'name': dbparam["archive_table7"],
                'id_column': 'step_execution_id',
                'older_than_days': int(dbparam["archive_table7_older_than_days"])
            },
            {
                'name': dbparam["archive_table8"],
                'id_column': 'step_execution_id',
                'older_than_days': int(dbparam["archive_table8_older_than_days"])
            },
            {
                'name': dbparam["archive_table9"],
                'id_column': 'event_id',
                'older_than_days': int(dbparam["archive_table9_older_than_days"])
            }
        ]

        sschemaName = dbparam["source_schema_name"]
        aschemaName = dbparam["archive_schema_name"]

        for table in tables:
            tableName = table['name']
            idColumn = table['id_column']
            olderThanDays = table['older_than_days']

            print(tableName)
            select_query = "SELECT * FROM {0}.{1} WHERE cr_dtimes < NOW() - INTERVAL '{2} days'".format(sschemaName, tableName, olderThanDays)
            sourceCur.execute(select_query)
            rows = sourceCur.fetchall()
            select_count = sourceCur.rowcount
            print(select_count, ": Record(s) selected for archive from", tableName)

            if select_count > 0:
                for row in rows:
                    rowValues = getValues(row)
                    insert_query = "INSERT INTO {0}.{1} VALUES ({2})".format(aschemaName, tableName, rowValues)
                    archiveCur.execute(insert_query)
                    archiveConn.commit()
                    insert_count = archiveCur.rowcount
                    print(insert_count, ": Record inserted successfully")

                    if insert_count > 0:
                        delete_query = "DELETE FROM {0}.{1} WHERE {2} = '{3}'".format(sschemaName, tableName, idColumn, row[0])
                        sourceCur.execute(delete_query)
                        sourceConn.commit()
                        delete_count = sourceCur.rowcount
                        print(delete_count, ": Record(s) deleted successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if sourceConn is not None:
            sourceCur.close()
            sourceConn.close()
            print('Source database connection closed.')
        if archiveConn is not None:
            archiveCur.close()
            archiveConn.close()
            print('Archive database connection closed.')

if __name__ == '__main__':
    dataArchive()
