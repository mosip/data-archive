#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to handle errors
handle_error() {
  local exit_code="$?"
  echo "Error occurred in script at line $BASH_LINENO with exit code $exit_code"
  # Add additional error handling or cleanup here if needed
  exit $exit_code
}

# Trap errors and call the handle_error function
trap 'handle_error' ERR

echo "Executing archive-jobs sequentilay which was mentioned in DB_NAMES"

cd archive-jobs

python3 mosip_archive_main.py

echo "Executed archive-jobs successfully"
