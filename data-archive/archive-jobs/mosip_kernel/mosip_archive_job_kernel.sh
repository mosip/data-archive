#!/bin/bash

# Export variables as global environment variables
export SOURCE_KERNEL_DB_SERVERIP
export SOURCE_KERNEL_DB_PORT
export SOURCE_KERNEL_DB_UNAME
export SOURCE_KERNEL_DB_PASS
export ARCHIVE_DB_SERVERIP
export ARCHIVE_DB_PORT
export ARCHIVE_DB_UNAME
export ARCHIVE_DB_PASS

python3 mosip_archive_kernel_table.py
