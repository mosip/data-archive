#!/bin/sh
# entrypoint.sh

echo "Executing db.sh"

bash db.sh

echo "executed db.sh succesfully"

sleep 1m

echo "Executing archive.sh"

bash archive.sh

echo "executed archive.sh successfully"
