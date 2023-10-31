#!/bin/bash
# entrypoint.sh

echo "executing archive jobs"

# Change to the directory where the archive jobs are stored
archive_jobs_dir="archive-jobs"
if [ ! -d "$archive_jobs_dir" ]; then
    echo "Error: Directory '$archive_jobs_dir' does not exist."
    exit 1
fi
cd "$archive_jobs_dir"

# Get a list of all directories
#directories=$(find . -type d)

# Iterate over the directories and remove the db.properties file from each directory
#for dir in $directories; do
  #rm -f "$dir/db.properties"
#done

# Define the list of job types
job_types=("audit" "credential" "esignet" "ida" "idrepo" "kernel" "master" "pms" "regprc" "resident")

# Loop through each job type
for job_type in "${job_types[@]}"; do
    # Construct directory and file paths
    script_dir="mosip_${job_type}"

    # Check if the script directory exists before proceeding
    if [ ! -d "$script_dir" ]; then
        echo "Directory not found for job type: $job_type"
        continue
    fi

    # Change directory to the specific job directory
    cd "$script_dir"

    script="mosip_archive_job_${job_type}.sh"

    # Check if the script exists before executing
    if [ -f "$script" ]; then
        # Execute the script using bash and pass the environment variables dynamically
        if ! bash "$script"; then
            echo "Error: Failed to execute script for job type: $job_type"
        fi
    else
        echo "Script not found for job type: $job_type"
    fi
    # Change back to the main directory after executing the script
    cd ../
done
