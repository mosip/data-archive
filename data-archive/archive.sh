#!/bin/bash

echo "Executing archive-jobs sequentilay"

cd archive-jobs

python3 mosip_archive_main.py

echo "Executed archive-jobs successfully"
