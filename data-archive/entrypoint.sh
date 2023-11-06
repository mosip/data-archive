#!/bin/sh
# entrypoint.sh
set -e

<<<<<<< HEAD
#echo "Executing db.sh"

#bash db.sh

#echo "executed db.sh succesfully"


echo "Executing archive.sh"

bash archive.sh
=======
#echo "Executing db_scripts"

#cd db_scripts/mosip_archive

# Execute the deployment script without the properties file..,as it was passed by arguments
#bash deploy.sh

#echo "executed db_scripts succesfully"

echo "Executing archive-jobs sequentilay which was mentioned in DB_NAMES"
>>>>>>> c1616056b12c35a2fda5e72fa0baef5fa3dc69ad

echo "executed archive.sh successfully"
