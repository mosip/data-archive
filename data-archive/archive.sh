set -e

echo "Executing archive-jobs sequentilay which was mentioned in DB_NAMES"

cd archive-jobs

python3 mosip_archive_main.py

echo "executed archive-jobs successfully"
