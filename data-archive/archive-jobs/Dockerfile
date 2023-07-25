# Use an official Python runtime as the base image
FROM python:3.9
# Set the working directory in the container
WORKDIR /archive
#copy all python script db to working dir
COPY mosip_ida/* mosip_idrepo/* mosip_resident/* mosip_regprc/* ./
# Copy the requirements file to the working directory
COPY requirements.txt .
# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip
# Create a virtual environment
RUN python -m venv /venv
# Activate the virtual environment
ENV PATH="/venv/bin:$PATH"
# Install the Python dependencies within the virtual environment
RUN pip install --no-cache-dir -r requirements.txt
#setting env variable
ENV source_db_serverip=
ENV source_db_port=
ENV source_db_name=
ENV source_schema_name=
ENV source_db_uname=
ENV source_db_pass=
ENV archive_db_serverip=
ENV archive_db_port=
ENV archive_db_name=
ENV archive_schema_name=
ENV archive_db_uname=
ENV archive_db_pass=

WORKDIR /archive

CMD cd $source_db_name;bash mosip_archive_job.sh ;
