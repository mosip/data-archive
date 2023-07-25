# Use an official Python runtime as the base image
FROM python:3.9
# Set the working directory in the container
WORKDIR /archive
COPY mosip_archive_ida_table.py .
COPY mosip_archive_ida.ini .
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
# Set environment variables with default values (you can change these at runtime)
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

# Run the Python script using the provided environment variables
CMD ["python3", "mosip_archive_ida_table.py"]
