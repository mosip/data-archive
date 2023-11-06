#!/bin/sh
# entrypoint.sh
set -e

echo "Executing db_scripts"

echo "Executing dbscript"

cd db_scripts/mosip_archive

# Execute the deployment script without the properties file..,as it was passed by arguments
bash deploy.sh

echo "executed db_scripts succesfully"

echo "Executing archive-jobs"

python3 mosip_archive_main.py

echo "executed archive-jobs successfully"
