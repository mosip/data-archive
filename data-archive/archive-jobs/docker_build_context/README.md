TO BUILD DOCKER IMAGE:

docker build -t mosip-archive-job .

TO RUN DOCKER IMAGE BY PASSING INI FILE DURING RUNTIME:

docker run -v /path/to/mosip_archive.ini:/app/mosip_archive.ini mosip-archive-job /app/mosip_archive.ini

*-v /path/to/mosip_archive.ini:/app/mosip_archive.ini: This option mounts the mosip_archive.ini file from the host machine into the container at the path /app/mosip_archive.ini.
*mosip-archive-job: This is the name of the Docker image.
