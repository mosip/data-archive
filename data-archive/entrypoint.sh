#!/bin/sh
# entrypoint.sh
set -e

#echo "Executing db.sh"

#bash db.sh

#echo "executed db.sh succesfully"


echo "Executing archive.sh"

bash archive.sh

echo "executed archive.sh successfully"
