#!/bin/sh
# entrypoint.sh

echo "Executing db.sh"
chmod +x db.sh
./db.sh

echo "executed db.sh succesfully"

sleep 1m

echo "Executing archive.sh"
chmod +x archive.sh
./archive.sh

echo "executed archive.sh successfully"
