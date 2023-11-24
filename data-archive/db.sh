#!/bin/bash
# entrypoint.sh

# Function to handle errors
handle_error() {
  echo "Error occurred in script at line $1. Exiting."
  exit 1
}

# Trap errors and execute the handle_error function
trap 'handle_error $LINENO' ERR

# Exit immediately if any command exits with a non-zero status
set -e

echo "Executing dbscript"

cd db_scripts/mosip_archive

# Execute the deployment script without the properties file, as it was passed by arguments
bash deploy.sh

echo "Executed successfully"
