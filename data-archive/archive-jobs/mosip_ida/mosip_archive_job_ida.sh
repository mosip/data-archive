#!/bin/bash

# Export variables as global environment variables
export SOURCE_IDA_DB_SERVERIP
export SOURCE_IDA_DB_PORT
export SOURCE_IDA_DB_UNAME
export SOURCE_IDA_DB_PASS
export ARCHIVE_DB_SERVERIP
export ARCHIVE_DB_PORT
export ARCHIVE_DB_UNAME
export ARCHIVE_DB_PASS

python3 mosip_archive_ida_table.py
