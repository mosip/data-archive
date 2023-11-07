#!/bin/bash
# entrypoint.sh

echo "Executing dbscript"

cd db_scripts/mosip_archive

# Execute the deployment script without the properties file..,as it was passed by arguments
bash deploy.sh

echo "Executed successfully
