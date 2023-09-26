#!/bin/bash
# entrypoint.sh

echo "executing archive jobs"

cd archive-jobs

# Define the list of job types
job_types=("audit" "credential" "esignet" "ida" "idrepo" "kernel" "master" "pms" "regprc" "resident")

# Loop through each job type
for i in "${job_types[@]}"; do
    # Construct directory and file paths
    script_dir="mosip_${i}"

    # Change directory to the specific job directory
    cd "$script_dir"
    script="mosip_archive_job_${i}.sh"
    ini_file="mosip_archive_${i}.ini"

    # Check if the script and INI file exist before executing
    if [ -f "$script" ] && [ -f "$ini_file" ]; then
        # Execute the script with the corresponding INI file using bash
        bash "$script" "$ini_file"
    else
        echo "Script or INI file not found for job type: $i"
    fi
    # Change back to the main directory after executing the script
    cd ..
done

echo "archive script executed successfully"
