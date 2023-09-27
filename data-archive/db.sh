#!/bin/bash
# entrypoint.sh
set -e

echo "Executing dbscript"

cd db_scripts/mosip_archive

chmod +x deploy.sh
# Execute the deployment script with the properties file
bash deploy.sh

echo "Executed successfully"

