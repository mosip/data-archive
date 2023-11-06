#!/bin/bash
# entrypoint.sh

echo "executing archive jobs"

cd archive-jobs

# Get a list of all directories
directories=$(find . -type d)

# Iterate over the directories and remove the db.properties file from each directory
for dir in $directories; do
  rm -f "$dir/all_db_properties.properties"
done

# Execute the deployment script without the properties file..,as it was passed by arguments
python3 mosip_archive_main.py

echo "Executed successfully"