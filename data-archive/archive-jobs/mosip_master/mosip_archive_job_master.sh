#!/bin/bash

# Export variables as global environment variables
export SOURCE_MASTER_DB_SERVERIP
export SOURCE_MASTER_DB_PORT
export SOURCE_MASTER_DB_UNAME
export SOURCE_MASTER_DB_PASS
export ARCHIVE_DB_SERVERIP
export ARCHIVE_DB_PORT
export ARCHIVE_DB_UNAME
export ARCHIVE_DB_PASS
python3 mosip_archive_master_table.py
